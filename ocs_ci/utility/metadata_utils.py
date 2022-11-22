"""
Module that contains all operations related to add metadata feature in a cluster

"""

import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod

log = logging.getLogger(__name__)


def check_setmetadata_availability(pod_obj):
    """
    Check setmetadata parameter is available or not for cephfs and rbd plugin pods

    """
    plugin_provisioner_pod_objs = pod.get_all_pods(
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        selector=["csi-cephfsplugin-provisioner", "csi-rbdplugin-provisioner"],
    )
    log.info(f"list of provisioner pods---- {plugin_provisioner_pod_objs}")
    response = pod.validate_pods_are_respinned_and_running_state(
        plugin_provisioner_pod_objs
    )
    log.info(response)
    for plugin_provisioner_pod in plugin_provisioner_pod_objs:
        args = pod_obj.exec_oc_cmd(
            "get pod "
            + plugin_provisioner_pod.name
            + " --output jsonpath='{.spec.containers[4].args}'"
        )
        return "--setmetadata=true" in args


def enable_metadata(
    config_map_obj,
    pod_obj,
):
    """
    Enable CSI_ENABLE_METADATA
    Steps:
    1:- Enable CSI_ENABLE_METADATA flag via patch request
    2:- Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner
    pods are up and running
    3:- Check 'setmatadata' is set for csi-cephfsplugin-provisioner
    and csi-rbdplugin-provisioner pods

    Returns:
        str: cluster name

    """
    enable_metadata = '{"data":{"CSI_ENABLE_METADATA": "true"}}'

    # Enable metadata feature for rook-ceph-operator-config using patch command
    assert config_map_obj.patch(
        resource_name="rook-ceph-operator-config",
        params=enable_metadata,
    ), "configmap/rook-ceph-operator-config not patched"

    # Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner pods are up and running
    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=csi-cephfsplugin-provisioner",
        dont_allow_other_resources=True,
        timeout=60,
    )

    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=csi-rbdplugin-provisioner",
        dont_allow_other_resources=True,
        timeout=60,
    )

    # Check 'setmatadata' is set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
    res = check_setmetadata_availability(pod_obj)
    if not res:
        raise AssertionError

    cephfsplugin_provisioner_pods = pod.get_cephfsplugin_provisioner_pods()

    args = pod_obj.exec_oc_cmd(
        "get pod "
        + cephfsplugin_provisioner_pods[0].name
        + " --output jsonpath='{.spec.containers[4].args}'"
    )
    for arg in args:
        if "--clustername" in arg:
            return arg[14:]


def disable_metadata(
    config_map_obj,
    pod_obj,
):
    """
    Disable CSI_ENABLE_METADATA
    Steps:
    1:- Disable CSI_ENABLE_METADATA flag via patch request
    2:- Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner
    pods are up and running
    3:- Check 'setmatadata' is not set for csi-cephfsplugin-provisioner
    and csi-rbdplugin-provisioner pods

    """
    disable_metadata = '{"data":{"CSI_ENABLE_METADATA": "false"}}'

    # Disable metadata feature for rook-ceph-operator-config using patch command
    assert config_map_obj.patch(
        resource_name="rook-ceph-operator-config",
        params=disable_metadata,
    ), "configmap/rook-ceph-operator-config not patched"

    # Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner pods are up and running
    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=csi-cephfsplugin",
        dont_allow_other_resources=True,
        timeout=60,
    )

    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=csi-rbdplugin",
        dont_allow_other_resources=True,
        timeout=60,
    )

    # Check 'setmatadata' is not set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
    res = check_setmetadata_availability(pod_obj)
    if res:
        raise AssertionError


def available_subvolumes(sc_name, toolbox_pod, fs):
    """
    To fetch available subvolumes for cephfs or rbd

    Returns:
    list: list of subvolumes available for rbd or cephfs

    """
    if sc_name == constants.DEFAULT_STORAGECLASS_CEPHFS:
        # fs="ocs-storagecluster-cephfilesystem"
        cephfs_subvolumes = toolbox_pod.exec_cmd_on_pod(
            f"ceph fs subvolume ls {fs} --group_name csi"
        )
        log.info(f"available cephfs subvolumes-----{cephfs_subvolumes}")
        return cephfs_subvolumes

    else:
        # fs="ocs-storagecluster-cephblockpool"
        rbd_cephblockpool = toolbox_pod.exec_cmd_on_pod(f"rbd ls {fs} --format json")
        log.info(f"available rbd cephblockpool-----{rbd_cephblockpool}")
        return rbd_cephblockpool


def created_subvolume(available_subvolumes, updated_subvolumes, sc_name):
    """
    To fetch created subvolume for cephfs or rbd

    Returns:
    str: name of subvolume created

    """
    for sub_vol in updated_subvolumes:
        if sub_vol not in available_subvolumes:
            created_subvolume = sub_vol
            if sc_name == constants.DEFAULT_STORAGECLASS_CEPHFS:
                log.info(f"created sub volume---- {created_subvolume['name']}")
                return created_subvolume["name"]
            else:
                log.info(f"created sub volume---- {created_subvolume}")
                return created_subvolume


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

    Returns:
    json: metadata details

    """
    if sc_name == constants.DEFAULT_STORAGECLASS_CEPHFS:
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
    else:
        if snapshot:
            created_subvol = created_subvolume(
                available_subvolumes, updated_subvolumes, sc_name
            )
        metadata = toolbox_pod.exec_cmd_on_pod(
            f"rbd image-meta ls {fs}/{created_subvol} --format=json"
        )
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
