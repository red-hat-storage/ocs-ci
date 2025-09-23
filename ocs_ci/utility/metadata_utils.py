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
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility import version

log = logging.getLogger(__name__)


def check_setmetadata_availability(pod_obj):
    """
    Check if --setmetadata=true is present in CSI plugin pods' args.

    Args:
        pod_obj (obj): pod object

    Returns:
        bool: True if --setmetadata=true is set on all CSI plugin pods, else False.
    """
    selectors = [get_provisioner_label(constants.CEPHFILESYSTEM)] + [
        get_provisioner_label(constants.CEPHBLOCKPOOL)
    ]
    selectors = [
        label.replace("app=", "") for label in selectors if isinstance(label, str)
    ]

    @retry((CommandFailed, ResourceWrongStatusException), tries=3, delay=15)
    def get_and_validate_plugin_pods():
        plugin_pods = pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=selectors,
        )
        log.info(f"Provisioner pods: {plugin_pods}")
        pod.validate_pods_are_respinned_and_running_state(plugin_pods)
        return plugin_pods

    # Get validated plugin pods
    plugin_pods = get_and_validate_plugin_pods()

    all_containers_have_flag = True

    for p in plugin_pods:
        containers = pod_obj.exec_oc_cmd(
            f"get pod {p.name} --output jsonpath='{{.spec.containers}}'"
        )
        found_flag = False
        for container in containers:
            args = container.get("args", [])
            if "--setmetadata=true" in args:
                found_flag = True
                break
        if not found_flag:
            log.warning(
                f"Pod {p.name} does not have '--setmetadata=true' in any container args."
            )
            all_containers_have_flag = False

    return all_containers_have_flag


def patch_metadata(enable=True):
    """
    Patch CSI drivers to enable or disable metadata collection.

    Args:
        enable (bool): Whether to enable or disable metadata.
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
    Enable CSI_ENABLE_METADATA through configmap or patch depending on OCS version.
    Enhanced version with improved retry logic and better error handling.

    Returns:
        str: Cluster name if found, else None.
    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    log.info(f"Enabling metadata for OCS version: {ocs_version}")

    if ocs_version < version.VERSION_4_19:
        # Legacy configmap approach for OCS < 4.19
        assert config_map_obj.patch(
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            params='{"data":{"CSI_ENABLE_METADATA": "true"}}',
        ), "Failed to patch rook-ceph-operator-config"

        for selector in [
            get_provisioner_label(constants.CEPHFILESYSTEM),
            get_provisioner_label(constants.CEPHBLOCKPOOL),
        ]:
            assert pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=selector,
                dont_allow_other_resources=True,
                timeout=60,
            ), f"Pods with selector {selector} are not running"

    else:
        # Newer patch-based approach for OCS >= 4.19
        log.info("Using patch-based metadata enable for OCS >= 4.19")
        patch_metadata(enable=True)

    # Enhanced verification with longer timeout for OCS >= 4.19
    retry_attempts = 5 if ocs_version >= version.VERSION_4_19 else 3
    retry_delay = 30 if ocs_version >= version.VERSION_4_19 else 15

    @retry(AssertionError, tries=retry_attempts, delay=retry_delay, backoff=1)
    def _retry_check_metadata_enabled(pod_obj):
        if not check_setmetadata_availability(pod_obj):
            if ocs_version >= version.VERSION_4_19:
                log.warning("Metadata not yet enabled after patch, retrying...")
            raise AssertionError("Metadata not enabled")
        return True

    _retry_check_metadata_enabled(pod_obj)
    log.info("Metadata successfully enabled and verified")

    # Get cluster name from CephFS provisioner pod
    try:
        cephfs_pods = pod.get_cephfsplugin_provisioner_pods(
            cephfsplugin_provisioner_label=(
                get_provisioner_label(constants.CEPHFILESYSTEM)
            )
        )
        if not cephfs_pods:
            log.warning(
                "No CephFS provisioner pods found, cannot retrieve cluster name"
            )
            return None

        args = pod_obj.exec_oc_cmd(
            f"get pod {cephfs_pods[0].name} --output jsonpath='{{.spec.containers[].args}}'"
        )
        for arg in args:
            if "--clustername" in arg:
                cluster_name = arg.split("=", 1)[-1]
                log.info(f"Retrieved cluster name: {cluster_name}")
                return cluster_name

        log.warning("No clustername parameter found in provisioner pod args")
        return None

    except Exception as e:
        log.error(f"Failed to retrieve cluster name: {e}")
        return None


def disable_metadata(config_map_obj, pod_obj):
    """
    Disable CSI_ENABLE_METADATA via configmap or patch.
    Enhanced version that ensures metadata is properly disabled by restarting
    provisioner pods if necessary.
    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    log.info(f"Disabling metadata for OCS version: {ocs_version}")

    if ocs_version < version.VERSION_4_19:
        # Legacy configmap approach for OCS < 4.19
        assert config_map_obj.patch(
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            params='{"data":{"CSI_ENABLE_METADATA": "false"}}',
        ), "Failed to patch rook-ceph-operator-config"

        for selector in [
            get_provisioner_label(constants.CEPHFILESYSTEM),
            get_provisioner_label(constants.CEPHBLOCKPOOL),
        ]:
            assert pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=selector,
                dont_allow_other_resources=True,
                timeout=60,
            ), f"Pods with selector {selector} are not running"
    else:
        # Newer patch-based approach for OCS >= 4.19
        log.info("Using patch-based metadata disable for OCS >= 4.19")
        patch_metadata(enable=False)

    # Enhanced verification with pod restart logic for OCS >= 4.19
    @retry(AssertionError, tries=5, delay=30, backoff=1)
    def _retry_check_metadata_disabled_with_restart(pod_obj):
        """
        Enhanced metadata disable verification that restarts pods if needed.
        """
        if check_setmetadata_availability(pod_obj):
            if ocs_version >= version.VERSION_4_19:
                log.warning(
                    "Metadata still enabled after patch, forcing provisioner pod restart"
                )
                _force_provisioner_pod_restart(pod_obj)
                # Wait for pods to restart and stabilize
                import time

                time.sleep(60)
            else:
                raise AssertionError("Metadata still enabled after configmap update")

        # Final verification
        assert not check_setmetadata_availability(
            pod_obj
        ), "Metadata still enabled after restart"
        return True

    def _force_provisioner_pod_restart(pod_obj):
        """
        Force restart of provisioner pods to ensure metadata disable takes effect.
        """
        log.info("Forcing restart of provisioner pods to apply metadata disable")

        provisioner_selectors = [
            get_provisioner_label(constants.CEPHFILESYSTEM),
            get_provisioner_label(constants.CEPHBLOCKPOOL),
        ]

        for selector in provisioner_selectors:
            log.info(f"Restarting pods with selector: {selector}")

            # Get current provisioner pods
            try:
                provisioner_pods = pod_obj.get(selector=selector)["items"]
                log.info(f"Found {len(provisioner_pods)} provisioner pods to restart")

                # Delete existing pods to force restart
                for pod_info in provisioner_pods:
                    pod_name = pod_info["metadata"]["name"]
                    log.info(f"Deleting provisioner pod: {pod_name}")
                    try:
                        pod_obj.delete(resource_name=pod_name)
                    except Exception as e:
                        log.warning(f"Failed to delete pod {pod_name}: {e}")

                # Wait for new pods to be running
                log.info(f"Waiting for new pods with selector {selector} to be running")
                assert pod_obj.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=selector,
                    timeout=180,  # Increased timeout for pod restart
                ), f"New pods with selector {selector} are not running after restart"

                log.info(f"Successfully restarted pods with selector: {selector}")

            except Exception as e:
                log.error(f"Failed to restart pods with selector {selector}: {e}")
                raise

    _retry_check_metadata_disabled_with_restart(pod_obj)
    log.info("Metadata successfully disabled and verified")


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
