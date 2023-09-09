"""
Module that contains all operations related to add metadata feature in a cluster

"""

import logging
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
)
from ocs_ci.helpers.storageclass_helpers import storageclass_name

log = logging.getLogger(__name__)


def check_setmetadata_availability(pod_obj):
    """
    Check setmetadata parameter is available or not for cephfs and rbd plugin pods

    Args:
        pod_obj (obj): pod object

    """
    plugin_provisioner_pod_objs = pod.get_all_pods(
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=["csi-cephfsplugin-provisioner", "csi-rbdplugin-provisioner"],
    )
    log.info(f"list of provisioner pods---- {plugin_provisioner_pod_objs}")
    response = retry((CommandFailed, ResourceWrongStatusException), tries=3, delay=15)(
        pod.validate_pods_are_respinned_and_running_state
    )(plugin_provisioner_pod_objs)
    log.info(response)
    get_args_provisioner_plugin_pods = []
    for plugin_provisioner_pod in plugin_provisioner_pod_objs:
        containers = pod_obj.exec_oc_cmd(
            "get pod "
            + plugin_provisioner_pod.name
            + " --output jsonpath='{.spec.containers}'"
        )
        for entry in containers:
            if "--setmetadata=true" in entry["args"]:
                get_args_provisioner_plugin_pods.append(entry["args"])
    if get_args_provisioner_plugin_pods:
        return all(
            ["--setmetadata=true" in args for args in get_args_provisioner_plugin_pods]
        )
    else:
        return False


def enable_metadata(
    config_map_obj,
    pod_obj,
):
    """
    Enable CSI_ENABLE_METADATA

    Args:
        config_map_obj (obj): configmap object
        pod_obj (obj): pod object

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
        resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
        params=enable_metadata,
    ), "configmap/rook-ceph-operator-config not patched"

    # Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner pods are up and running
    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        dont_allow_other_resources=True,
        timeout=60,
    ), "Pods are not in running status"

    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        dont_allow_other_resources=True,
        timeout=60,
    ), "Pods are not in running status"

    # Check 'setmatadata' is set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
    res = check_setmetadata_availability(pod_obj)
    assert res, "Error: The metadata not set for cephfs and rbd plugin provisioner pods"

    cephfsplugin_provisioner_pods = pod.get_cephfsplugin_provisioner_pods()

    args = pod_obj.exec_oc_cmd(
        "get pod "
        + cephfsplugin_provisioner_pods[0].name
        + " --output jsonpath='{.spec.containers[4].args}'"
    )
    for arg in args:
        if "--clustername" in arg:
            log.info(f"Fetch the cluster name parameter {arg}")
            # To fetch value of clustername parameter
            return arg[14:]


def disable_metadata(
    config_map_obj,
    pod_obj,
):
    """
    Disable CSI_ENABLE_METADATA

    Args:
        config_map_obj (obj): configmap object
        pod_obj (obj): pod object

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
        resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
        params=disable_metadata,
    ), "configmap/rook-ceph-operator-config not patched"

    # Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner pods are up and running
    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        dont_allow_other_resources=True,
        timeout=60,
    ), "Pods are not in running status"

    assert pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        dont_allow_other_resources=True,
        timeout=60,
    ), "Pods are not in running status"

    # Check 'setmatadata' is not set for csi-cephfsplugin-provisioner and csi-rbdplugin-provisioner pods
    res = check_setmetadata_availability(pod_obj)
    assert (
        not res
    ), "Error: The metadata is set, while it is expected to be unavailable "


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
    if sc_name == storageclass_name(constants.OCS_COMPONENTS_MAP["cephfs"]):
        cephfs_subvolumes = toolbox_pod.exec_cmd_on_pod(
            f"ceph fs subvolume ls {fs} --group_name csi"
        )
        log.info(f"available cephfs subvolumes-----{cephfs_subvolumes}")
        return cephfs_subvolumes

    elif sc_name == storageclass_name(constants.OCS_COMPONENTS_MAP["blockpools"]):
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
            if sc_name == storageclass_name(constants.OCS_COMPONENTS_MAP["cephfs"]):
                log.info(f"created sub volume---- {created_subvolume['name']}")
                return created_subvolume["name"]
            elif sc_name == storageclass_name(
                constants.OCS_COMPONENTS_MAP["blockpools"]
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
    if sc_name == storageclass_name(constants.OCS_COMPONENTS_MAP["cephfs"]):
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
    elif sc_name == storageclass_name(constants.OCS_COMPONENTS_MAP["blockpools"]):
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
