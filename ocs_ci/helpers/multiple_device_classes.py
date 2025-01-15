import logging
import random

from ocs_ci.helpers.helpers import create_lvs_resource
from ocs_ci.ocs.cluster import check_ceph_osd_tree, check_ceph_osd_df_tree
from ocs_ci.ocs.exceptions import CephHealthException
from ocs_ci.ocs.node import add_disk_to_node, get_node_objs
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.resources.pvc import wait_for_pvcs_in_lvs_to_reach_status
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_size,
    get_device_class,
    verify_storage_device_class,
    verify_device_class_in_osd_tree,
    get_deviceclass_name_per_count,
)
from ocs_ci.utility.utils import sum_of_two_storage_sizes

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config


log = logging.getLogger(__name__)


def create_new_lvs_for_new_deviceclass(
    worker_nodes, create_disks_for_lvs=True, ssd=True
):
    """
    Create new LocalVolumeSet resource for a new device class

    Args:
        worker_nodes (list): The worker node names to be used in the LocalVolumeSet resource.
        create_disks_for_lvs (bool): If True, it will create a new disks for the new LocalVolumeSet resource.
        ssd (bool): if True, mark disk as SSD

    Returns:
        OCS: The OCS instance for the LocalVolumeSet resource

    """
    osd_size = get_storage_size()
    log.info(f"the osd size is {osd_size}")
    old_lvs_max_size = sum_of_two_storage_sizes(osd_size, "30Gi")
    ocp_lvs_obj = OCP(
        kind=constants.LOCAL_VOLUME_SET,
        namespace=defaults.LOCAL_STORAGE_NAMESPACE,
        resource_name=constants.LOCAL_BLOCK_RESOURCE,
    )
    log.info(
        f"Update the old LocalVolumeSet {ocp_lvs_obj.resource_name} with the maxSize "
        f"{old_lvs_max_size} so it will not consume the new PVs"
    )
    params = (
        f'{{"spec": {{"deviceInclusionSpec": {{"maxSize": "{old_lvs_max_size}"}}}}}}'
    )
    lvs_result = ocp_lvs_obj.patch(params=params, format_type="merge")
    assert (
        lvs_result
    ), f"Failed to update the LocalVolumeSet {ocp_lvs_obj.resource_name}"

    log.info(
        "Create a new minSize that will be be higher than the maxSize of the old LVS, so that the new LVS "
        "will consume the disks with the new size"
    )
    min_size = sum_of_two_storage_sizes(old_lvs_max_size, "10Gi")
    log.info(
        "Limit the max size of the new LVS, so it will consume only the new added disks"
    )
    max_size = sum_of_two_storage_sizes(min_size, "60Gi")
    suffix = "".join(random.choices("0123456789", k=5))
    sc_name = f"localvolume{suffix}"
    lvs_obj = create_lvs_resource(sc_name, sc_name, worker_nodes, min_size, max_size)

    if create_disks_for_lvs:
        disk_size_in_gb = sum_of_two_storage_sizes(min_size, "10Gi")
        disk_size = int(disk_size_in_gb[:-2])
        worker_node_objs = get_node_objs(worker_nodes)
        for n in worker_node_objs:
            add_disk_to_node(n, disk_size=disk_size, ssd=ssd)

    return lvs_obj


def check_ceph_state_post_add_deviceclass():
    """
    Check the Ceph state post add a new deviceclass.
    The function checks the Ceph device classes and osd tree.

    Raises:
        CephHealthException: In case the Ceph device classes and osd tree checks
            didn't finish successfully

    """
    log.info("Check the Ceph device classes and osd tree")
    device_class = get_device_class()
    ct_pod = get_ceph_tools_pod()
    try:
        verify_storage_device_class(device_class, check_multiple_deviceclasses=True)
        verify_device_class_in_osd_tree(
            ct_pod, device_class, check_multiple_deviceclasses=True
        )
    except AssertionError as ex:
        raise CephHealthException(ex)
    if not check_ceph_osd_tree():
        raise CephHealthException("The ceph osd tree checks didn't finish successfully")
    if not check_ceph_osd_df_tree():
        raise CephHealthException(
            "The ceph osd df tree output is not formatted correctly"
        )


def verification_steps_after_adding_new_deviceclass():
    deviceclass_name_per_count = get_deviceclass_name_per_count()
    log.info(f"deviceclass name per count = {deviceclass_name_per_count}")
    lvs_obj = OCP(
        kind=constants.LOCAL_VOLUME_SET, namespace=defaults.LOCAL_STORAGE_NAMESPACE
    )
    lvs_items = lvs_obj.data["items"]
    for lvs_data in lvs_items:
        lvs_name = lvs_data["metadata"]["name"]
        pvc_count = deviceclass_name_per_count[lvs_name]
        wait_for_pvcs_in_lvs_to_reach_status(
            lvs_name, pvc_count, constants.STATUS_BOUND
        )

    osd_pods_count = sum(deviceclass_name_per_count.values())
    pod_obj = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    log.info("Waiting for the OSD pods to reach the Running state")
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OSD_APP_LABEL,
        resource_count=osd_pods_count,
        timeout=180,
        sleep=10,
    )

    check_ceph_state_post_add_deviceclass()
