"""
Module that contains all operations related to add metadata feature in a cluster

"""

import logging
import json
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import get_provisioner_label
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility import version

log = logging.getLogger(__name__)

CEPH_CSI_OPERATOR_CONFIG = "ceph-csi-operator-config"
_CLUSTERNAME_ARG = "--clustername"


def _ctrlplugin_labels():
    """Return cephfs and rbd CSI ctrlplugin/provisioner labels for this ODF version."""
    return [
        get_provisioner_label(constants.CEPHFILESYSTEM),
        get_provisioner_label(constants.CEPHBLOCKPOOL),
    ]


def _ctrlplugin_selector_values():
    """Return label values (without ``app=``) for get_all_pods selector."""
    return [
        label.replace("app=", "")
        for label in _ctrlplugin_labels()
        if isinstance(label, str)
    ]


def _arg_cluster_name(arg):
    """Return cluster name from ``--clustername=...`` or None."""
    if not isinstance(arg, str):
        return None
    if arg.startswith(f"{_CLUSTERNAME_ARG}="):
        return arg.split("=", 1)[-1]
    return None


def csi_enable_metadata_is_deprecated():
    """
    Return True if Driver CRD ``spec.enableMetadata`` is marked deprecated.

    The ceph-csi-operator CRD keeps the field for compatibility and documents
    that it is no longer used and will be ignored.
    """
    crd = OCP(kind=constants.CRD_KIND, resource_name="drivers.csi.ceph.io")
    data = crd.get(dont_raise=True, silent=True)
    if not data:
        return False
    for ver in (data.get("spec") or {}).get("versions") or []:
        enable_md = ((ver.get("schema") or {}).get("openAPIV3Schema") or {}).get(
            "properties", {}
        ).get("spec", {}).get("properties", {}).get("enableMetadata") or {}
        desc = (enable_md.get("description") or "").lower()
        if (
            "deprecated" in desc
            or "no longer used" in desc
            or "will be ignored" in desc
        ):
            return True
    return False


def csi_metadata_is_unconditional():
    """
    Return True when CSI PVC/PV metadata is always-on (no ``--setmetadata`` gate).

    Capability probe first: if the Driver CRD marks ``enableMetadata`` deprecated
    (ignored by ceph-csi), the container flag is gone and metadata is
    unconditional. Version ``>= VERSION_5_0`` is the fallback when the CRD
    cannot be read. Flag absence on a running ctrlplugin is not used as the
    probe — a pre-5.0 cluster with metadata disabled also has no flag.
    """
    if csi_enable_metadata_is_deprecated():
        log.info(
            "Driver CRD marks enableMetadata deprecated; CSI metadata is unconditional"
        )
        return True
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version >= version.VERSION_5_0:
        log.info(
            "ODF %s >= 5.0; treating CSI metadata as unconditional",
            ocs_version,
        )
        return True
    return False


def iter_ctrlplugin_containers(pod_obj):
    """
    Yield ``(pod_name, container_name, args)`` for every container on cephfs
    and rbd CSI ctrlplugin (or provisioner) pods.

    Args:
        pod_obj (OCP): Pod OCP object in the storage namespace.

    Yields:
        tuple: pod name, container name, args list (may be empty).
    """

    @retry((CommandFailed, ResourceWrongStatusException), tries=3, delay=15)
    def _plugin_pods():
        plugin_pods = pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=_ctrlplugin_selector_values(),
        )
        log.info("CSI ctrlplugin/provisioner pods: %s", plugin_pods)
        pod.validate_pods_are_respinned_and_running_state(plugin_pods)
        return plugin_pods

    for p in _plugin_pods():
        containers = pod_obj.exec_oc_cmd(
            f"get pod {p.name} --output jsonpath='{{.spec.containers}}'"
        )
        for container in containers or []:
            yield p.name, container.get("name") or "", container.get("args") or []


def check_setmetadata_availability(pod_obj):
    """
    Return True if CSI metadata is enabled for this cluster.

    On ODF versions that still gate the feature with ``--setmetadata=true``,
    every cephfs and rbd ctrlplugin/provisioner pod must have that arg on at
    least one container (including ``csi-addons``, where the flag also lived).

    When metadata is unconditional (ODF 5.0+: flag removed, ``enableMetadata``
    ignored), this returns True. Absence of the flag is expected there; it is
    not treated as "metadata disabled".

    Args:
        pod_obj (OCP): Pod OCP object in the storage namespace.

    Returns:
        bool: True if metadata is enabled (flag present, or always-on).
    """
    if csi_metadata_is_unconditional():
        log.info(
            "CSI metadata is unconditional; --setmetadata is not expected on "
            "ctrlplugin containers"
        )
        return True

    pods_with_flag = set()
    pods_seen = set()
    for pod_name, container_name, args in iter_ctrlplugin_containers(pod_obj):
        pods_seen.add(pod_name)
        if "--setmetadata=true" in args:
            log.info(
                "Found --setmetadata=true on pod %s container %s",
                pod_name,
                container_name,
            )
            pods_with_flag.add(pod_name)

    missing = pods_seen - pods_with_flag
    if missing:
        log.warning(
            "Pods without --setmetadata=true in any container: %s",
            sorted(missing),
        )
        return False
    return bool(pods_seen)


@retry(AssertionError, tries=3, delay=15, backoff=1)
def _assert_setmetadata_enabled(pod_obj):
    assert check_setmetadata_availability(pod_obj), "Metadata not enabled"


def get_csi_cluster_name(pod_obj):
    """
    Return the CSI cluster name from ctrlplugin ``--clustername`` or OperatorConfig.

    Args:
        pod_obj (OCP): Pod OCP object used to run ``oc get``.

    Returns:
        str | None: Cluster name, or None if it cannot be determined.
    """
    for _pod_name, _cname, args in iter_ctrlplugin_containers(pod_obj):
        for arg in args:
            name = _arg_cluster_name(arg)
            if name:
                log.info("Cluster name from --clustername: %s", name)
                return name
    try:
        name = pod_obj.exec_oc_cmd(
            f"get operatorconfig {CEPH_CSI_OPERATOR_CONFIG} "
            "--output jsonpath='{.spec.driverSpecDefaults.clusterName}'"
        )
    except CommandFailed:
        log.warning(
            "Could not read OperatorConfig %s driverSpecDefaults.clusterName",
            CEPH_CSI_OPERATOR_CONFIG,
        )
        return None
    if name:
        name = str(name).strip()
        log.info(
            "Cluster name from OperatorConfig %s: %s",
            CEPH_CSI_OPERATOR_CONFIG,
            name,
        )
        return name or None
    return None


def patch_metadata(enable=True):
    """
    Patch CSI Driver CRs ``spec.enableMetadata``.

    On ODF >= 5.0 this field is deprecated and ignored by ceph-csi (the
    ``--setmetadata`` container flag was removed). Callers must skip this
    patch when :func:`csi_metadata_is_unconditional` is True rather than
    treating a successful patch as enabling the feature.

    Args:
        enable (bool): Value to write to spec.enableMetadata.
    """
    patch_data = [{"op": "add", "path": "/spec/enableMetadata", "value": enable}]
    patch_json = json.dumps(patch_data)

    rbd_cmd = (
        f"oc patch {constants.CEPH_DRIVER_CSI} {constants.RBD_PROVISIONER} --type json "
        f"-p '{patch_json}' -n {config.ENV_DATA['cluster_namespace']}"
    )
    cephfs_cmd = (
        f"oc patch {constants.CEPH_DRIVER_CSI} {constants.CEPHFS_PROVISIONER} --type json "
        f"-p '{patch_json}' -n {config.ENV_DATA['cluster_namespace']}"
    )

    for cmd, name in [(rbd_cmd, "RBD"), (cephfs_cmd, "CephFS")]:
        try:
            run_cmd(cmd)
        except CommandFailed as ex:
            log.error(f"Failed to patch {name} provisioner: {ex}")
            raise


def enable_metadata(config_map_obj, pod_obj):
    """
    Enable CSI metadata for PVC/PV objects, then return the CSI cluster name.

    - ODF < 4.19: patch rook-ceph-operator-config ``CSI_ENABLE_METADATA``.
    - ODF 4.19–4.x: patch Driver ``spec.enableMetadata`` and wait for
      ``--setmetadata=true`` on ctrlplugin containers.
    - ODF >= 5.0 (unconditional metadata): do not patch the deprecated
      field and do not assert on ``--setmetadata``. Callers verify metadata
      on subvolumes/images via :func:`fetch_metadata`.

    Returns:
        str: Cluster name from ``--clustername`` or OperatorConfig, else None.
    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    always_on = csi_metadata_is_unconditional()

    if ocs_version < version.VERSION_4_19:
        assert config_map_obj.patch(
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            params='{"data":{"CSI_ENABLE_METADATA": "true"}}',
        ), "Failed to patch rook-ceph-operator-config"

        for selector in _ctrlplugin_labels():
            assert pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=selector,
                dont_allow_other_resources=True,
                timeout=60,
            ), f"Pods with selector {selector} are not running"
    elif not always_on:
        patch_metadata(enable=True)
    else:
        log.info(
            "Skipping Driver spec.enableMetadata patch; CSI metadata is "
            "unconditional on this ODF version"
        )

    if not always_on:
        _assert_setmetadata_enabled(pod_obj)

    return get_csi_cluster_name(pod_obj)


def available_subvolumes(sc_name, toolbox_pod, fs):
    """
    To fetch available subvolumes for cephfs or rbd

    Args:
        sc_name (str): storage class
        toolbox_pod (str): ceph tool box pod
        fs (str): file system

    Returns:
        list: subvolumes available for rbd or cephfs

    """
    if (
        sc_name == constants.DEFAULT_STORAGECLASS_CEPHFS
        or sc_name == constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
    ):
        cephfs_subvolumes = toolbox_pod.exec_cmd_on_pod(
            f"ceph fs subvolume ls {fs} --group_name csi"
        )
        log.info(f"available cephfs subvolumes-----{cephfs_subvolumes}")
        return cephfs_subvolumes
    elif sc_name == constants.DEFAULT_STORAGECLASS_RBD:
        rbd_cephblockpool = toolbox_pod.exec_cmd_on_pod(f"rbd ls {fs} --format json")
        log.info(f"available rbd cephblockpool-----{rbd_cephblockpool}")
        return rbd_cephblockpool
    elif sc_name == constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD:
        rbd_cephblockpool = toolbox_pod.exec_cmd_on_pod("rbd ls --format json")
        log.info(f"available rbd cephblockpool-----{rbd_cephblockpool}")
        return rbd_cephblockpool
    else:
        log.exception("Metadata feature is not supported for this storage class")


def created_subvolume(available_subvolumes, updated_subvolumes, sc_name):
    """
    To fetch created subvolume for cephfs or rbd

    Args:
        available_subvolumes (list): List of available subvolumes
        updated_subvolumes (list): Updated list of subvolumes
        sc_name (str): storage class

    Returns:
        str: name of subvolume created

    """
    for sub_vol in updated_subvolumes:
        if sub_vol not in available_subvolumes:
            created_subvolume = sub_vol
            if (
                sc_name == constants.DEFAULT_STORAGECLASS_CEPHFS
                or sc_name == constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
            ):
                log.info(f"created sub volume---- {created_subvolume['name']}")
                return created_subvolume["name"]
            elif (
                sc_name == constants.DEFAULT_STORAGECLASS_RBD
                or sc_name == constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            ):
                log.info(f"created sub volume---- {created_subvolume}")
                return created_subvolume
            else:
                log.exception(
                    "Metadata feature is not supported for this storage class"
                )


def fetch_metadata(
    sc_name,
    fs,
    toolbox_pod,
    created_subvol,
    snapshot=False,
    available_subvolumes=None,
    updated_subvolumes=None,
):
    """
    To fetch metadata details created for cephfs or rbd

    Args:
        sc_name (str): storage class
        toolbox_pod (str): ceph tool box pod
        fs (str): file system
        created_subvol (str): Created sub volume
        snapshot (bool): snapshot or not
        available_subvolumes (list): List of available subvolumes
        updated_subvolumes (list): Updated list of subvolumes

    Returns:
        json: metadata details

    """
    if (
        sc_name == constants.DEFAULT_STORAGECLASS_CEPHFS
        or sc_name == constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
    ):
        if snapshot:
            snap_subvolume = toolbox_pod.exec_cmd_on_pod(
                f"ceph fs subvolume snapshot ls {fs} {created_subvol} --group_name=csi --format=json"
            )
            log.info(f"snap subvolume----{snap_subvolume}")
            metadata = toolbox_pod.exec_cmd_on_pod(
                f"ceph fs subvolume snapshot metadata ls {fs} {created_subvol} {snap_subvolume[0]['name']}"
                + " --group_name=csi --format=json"
            )
        else:
            metadata = toolbox_pod.exec_cmd_on_pod(
                f"ceph fs subvolume metadata ls {fs} {created_subvol} --group_name=csi --format=json"
            )
    elif sc_name == constants.DEFAULT_STORAGECLASS_RBD:
        if snapshot:
            created_subvol = created_subvolume(
                available_subvolumes, updated_subvolumes, sc_name
            )
        metadata = toolbox_pod.exec_cmd_on_pod(
            f"rbd image-meta ls {fs}/{created_subvol} --format=json"
        )
    elif sc_name == constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD:
        if snapshot:
            created_subvol = created_subvolume(
                available_subvolumes, updated_subvolumes, sc_name
            )
        metadata = toolbox_pod.exec_cmd_on_pod(
            f"rbd image-meta ls {created_subvol} --format=json"
        )
    else:
        log.exception("Metadata feature is not supported for this storage class")
    log.info(f"metadata is ------ {metadata}")
    return metadata


def validate_metadata(
    metadata,
    clustername,
    pv_name=None,
    pvc_name=None,
    namespace=None,
    volumesnapshot_name=None,
    volumesnapshot_content=None,
):
    """
    To validate the metadata details

    Args:
        metadata (json): metadata details
        clustername (str): cluster name
        pv_name (str): name of the pv
        pvc_name (str): name of the pvc
        namespace (str): namespace
        volumesnapshot_name (str): name of the volumesnapshot
        volumesnapshot_content (str): volumesnapshot content

    """
    assert (
        clustername == metadata["csi.ceph.com/cluster/name"]
    ), "Error: cluster name is not as expected"
    if pv_name:
        assert (
            pv_name == metadata["csi.storage.k8s.io/pv/name"]
        ), "Error: pv name is not as expected"
    if pvc_name:
        assert (
            pvc_name == metadata["csi.storage.k8s.io/pvc/name"]
        ), "Error: pvc name is not as expected"
        assert (
            namespace == metadata["csi.storage.k8s.io/pvc/namespace"]
        ), "Error: namespace is not as expected"
    if volumesnapshot_name:
        assert (
            volumesnapshot_name == metadata["csi.storage.k8s.io/volumesnapshot/name"]
        ), "Error: volumesnapshot name is not as expected"
    if volumesnapshot_content:
        assert (
            volumesnapshot_content
            == metadata["csi.storage.k8s.io/volumesnapshotcontent/name"]
        ), "Error: snapshot content name is not as expected"
        assert (
            namespace == metadata["csi.storage.k8s.io/volumesnapshot/namespace"]
        ), "Error: namespace is not as expected"


def update_testdata_for_external_modes(
    sc_name,
    fs,
    external_mode=False,
):
    """
    Update the file sytem and storage class names for external mode clusters

    Args:
        sc_name (str): storage class
        fs (str): file system
        external_mode(bool): External mode or not

    Returns:
        sc_name (str): storage class
        fs (str): file system

    """
    if external_mode:
        if sc_name == constants.DEFAULT_STORAGECLASS_CEPHFS:
            fs = "fsvol001"
            sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
        elif sc_name == constants.DEFAULT_STORAGECLASS_RBD:
            fs = ""
            sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
        else:
            log.exception("Metadata feature is not supported for this storage class")
    return fs, sc_name
