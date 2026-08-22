import logging
import random

from ocs_ci.helpers.helpers import create_lvs_resource
from ocs_ci.ocs.cluster import check_ceph_osd_tree
from ocs_ci.ocs.exceptions import CephHealthException, ResourceNotFoundError
from ocs_ci.ocs.node import (
    add_disk_to_node,
    get_node_objs,
    get_osd_running_nodes,
)
from ocs_ci.ocs.resources.pv import (
    get_pv_in_status,
    wait_for_pvs_in_lvs_to_reach_status,
)
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.resources.pvc import (
    get_pvcs_in_deviceset,
    wait_for_pvcs_in_deviceset_to_reach_status,
)
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_size,
    get_device_class,
    verify_storage_device_class,
    verify_device_class_in_osd_tree,
    get_deviceset_name_per_count,
    get_first_sc_name_from_storagecluster,
    get_default_storagecluster,
    get_all_device_sets,
    get_deviceset_sc_name,
    set_deviceset_count,
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import sum_of_two_storage_sizes, TimeoutSampler

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config


log = logging.getLogger(__name__)


def get_first_deviceset_by_sc_name():
    """
    Get the first StorageCluster deviceset matching the first SC name.

    Returns:
        dict: The first deviceset spec entry, or None if not found.

    """
    first_sc_name = get_first_sc_name_from_storagecluster()
    for device_set in get_all_device_sets():
        if get_deviceset_sc_name(device_set) == first_sc_name:
            return device_set
    log.warning(
        "No deviceset found for storageclass %s",
        first_sc_name,
    )
    return None


def _is_deviceset_count_healed(deviceset_name, target_count, replica):
    """
    Check whether deviceset count and leftover PVCs are healed.

    Args:
        deviceset_name (str): Deviceset name to check.
        target_count (int): Expected deviceset count after heal.
        replica (int): Deviceset replica value.

    Returns:
        bool: True if count matches, no non-Bound deviceset PVCs remain,
            and Bound PVC count equals target_count * replica.

    """
    current_count = get_deviceset_name_per_count().get(deviceset_name)
    if current_count is None:
        log.warning("Deviceset %s not found while waiting for heal", deviceset_name)
        return False
    current_count = int(current_count)
    if current_count != target_count:
        log.info(
            "Waiting for deviceset %s count to reach %s (current=%s)",
            deviceset_name,
            target_count,
            current_count,
        )
        return False

    deviceset_pvcs = get_pvcs_in_deviceset(deviceset_name)
    bound_pvcs = [pvc for pvc in deviceset_pvcs if pvc.status == constants.STATUS_BOUND]
    non_bound_pvcs = [
        pvc for pvc in deviceset_pvcs if pvc.status != constants.STATUS_BOUND
    ]
    if non_bound_pvcs:
        log.info(
            "Waiting for non-Bound leftover deviceset PVCs to be deleted: %s",
            [pvc.name for pvc in non_bound_pvcs],
        )
        return False

    expected_bound = target_count * replica
    if len(bound_pvcs) != expected_bound:
        log.info(
            "Waiting for deviceset %s to have %s Bound PVCs "
            "(target_count=%s * replica=%s, current Bound=%s)",
            deviceset_name,
            expected_bound,
            target_count,
            replica,
            len(bound_pvcs),
        )
        return False
    return True


def heal_inflated_default_deviceset_count(timeout=600, sleep=20):
    """
    Lower inflated default deviceset count and clear non-Bound leftover PVCs.

    Failed add_capacity runs can leave storageDeviceSets[0].count higher than
    Bound deviceset PVC capacity. This patches count down and deletes non-Bound
    leftover PVCs, then waits via TimeoutSampler until the heal is complete.

    Args:
        timeout (int): Seconds to wait for the heal to settle.
        sleep (int): Seconds between heal polls.

    Returns:
        bool: True if a heal was applied, False if no heal was needed.

    """
    first_deviceset = get_first_deviceset_by_sc_name()
    if not first_deviceset:
        return False
    first_sc_name = get_deviceset_sc_name(first_deviceset)
    deviceset_name = first_deviceset["name"]
    current_count = int(first_deviceset["count"])
    replica = int(first_deviceset.get("replica", 1)) or 1

    deviceset_pvcs = get_pvcs_in_deviceset(deviceset_name)
    bound_count = len(
        [pvc for pvc in deviceset_pvcs if pvc.status == constants.STATUS_BOUND]
    )
    log.info(
        "Default deviceset %s (sc=%s): count=%s, replica=%s, "
        "Bound PVCs=%s, total PVCs=%s",
        deviceset_name,
        first_sc_name,
        current_count,
        replica,
        bound_count,
        len(deviceset_pvcs),
    )
    if bound_count == 0:
        return False

    if bound_count % replica != 0:
        log.warning(
            "Bound PVC count %s is not divisible by replica %s; "
            "skipping deviceset count heal to avoid a partial "
            "replica group",
            bound_count,
            replica,
        )
        return False

    # Each deviceset count unit provisions 'replica' OSDs/PVCs. Derive the
    # highest safe count from Bound PVC capacity so count * replica matches
    # provisioned PVCs without truncating a partial replica group.
    log.info(
        "Calculating target deviceset count from Bound PVCs (%s) / "
        "replica (%s) so count * replica matches provisioned capacity",
        bound_count,
        replica,
    )
    target_count = max(1, bound_count // replica)
    if current_count <= target_count:
        return False

    log.warning(
        "Healing inflated deviceset count on %s from %s to %s "
        "to match Bound PVCs before device-class test",
        deviceset_name,
        current_count,
        target_count,
    )
    set_deviceset_count(target_count)
    for pvc in deviceset_pvcs:
        if pvc.status != constants.STATUS_BOUND:
            log.info(
                "Deleting non-Bound leftover deviceset PVC %s (status=%s)",
                pvc.name,
                pvc.status,
            )
            pvc.delete(wait=False)

    for healed in TimeoutSampler(
        timeout,
        sleep,
        _is_deviceset_count_healed,
        deviceset_name,
        target_count,
        replica,
    ):
        if healed:
            log.info(
                "Deviceset %s heal completed: count=%s, Bound PVCs=%s",
                deviceset_name,
                target_count,
                target_count * replica,
            )
            return True
    return False


def ensure_storagecluster_ready_for_deviceclass_test(timeout=600, sleep=20):
    """
    Heal leftover StorageCluster deviceset inflation and wait for Ready.

    Failed add_capacity runs can leave the default deviceset count higher than
    the number of Bound deviceset PVCs. That keeps StorageCluster in
    Progressing and poisons later device-class tests. This helper heals the
    inflated count when needed, then waits for PHASE=Ready.

    Args:
        timeout (int): Seconds to wait for StorageCluster Ready.
        sleep (int): Seconds between Ready polls.

    """
    sc_obj = get_default_storagecluster()
    sc_name = sc_obj.resource_name
    sc_data = sc_obj.get()
    log.info(
        "StorageCluster %s phase before device-class test: %s",
        sc_name,
        sc_data.get("status", {}).get("phase"),
    )
    heal_inflated_default_deviceset_count(timeout=timeout, sleep=sleep)

    log.info(
        "Waiting for StorageCluster %s to reach Ready before device-class test",
        sc_name,
    )
    sc_obj.wait_for_resource(
        condition=constants.STATUS_READY,
        resource_name=sc_name,
        column="PHASE",
        timeout=timeout,
        sleep=sleep,
    )


def create_new_lvs_for_new_deviceclass(
    worker_nodes, create_disks_for_lvs=True, ssd=True
):
    """
    Create a new LocalVolumeSet resource for a new device class
    It performs the following steps:
    1. Update the old LocalVolumeSet with a maxSize, so it will not consume the new PVs.
    2. Create a new minSize that will be higher than the maxSize of the LocalVolumeSets
    so that the new LVS will consume the disks with the new size.
    3. Limit the max size of the new LVS, so it will consume only the newly added disks.
    4. Create a new LocalVolumeSet with the minSize and maxSize above.
    5. If the param 'create_disks_for_lvs' is True, add new disks for the worker nodes.
    The disk size will be between the minSize and maxSize above to match the new LVS.

    Args:
        worker_nodes (list): The worker node names to be used in the LocalVolumeSet resource.
        create_disks_for_lvs (bool): If True, it will create a new disks for the new LocalVolumeSet resource.
        ssd (bool): if True, mark disk as SSD

    Returns:
        OCS: The OCS instance for the LocalVolumeSet resource

    """
    osd_size = get_storage_size()
    log.info(f"the osd size is {osd_size}")
    # Limit the old LVS max size so it will not consume the new PVs
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

    lvs_items = OCP(
        kind=constants.LOCAL_VOLUME_SET,
        namespace=defaults.LOCAL_STORAGE_NAMESPACE,
    ).get()["items"]

    lvs_max_sizes = [
        lvs_data["spec"]["deviceInclusionSpec"].get("maxSize", 0)
        for lvs_data in lvs_items
    ]
    lvs_max_size = max(lvs_max_sizes, key=lambda size: int(size[0:-2]))

    log.info(
        f"Create a new minSize that will be be higher than the maxSize of the LocalVolumeSets "
        f"{lvs_max_size}, so that the new LVS will consume the disks with the new size"
    )
    min_size = sum_of_two_storage_sizes(lvs_max_size, "10Gi")
    log.info(
        "Limit the max size of the new LVS, so it will consume only the new added disks"
    )
    max_size = sum_of_two_storage_sizes(min_size, "40Gi")
    suffix = "".join(random.choices("0123456789", k=5))
    sc_name = f"localvolume{suffix}"
    lvs_obj = create_lvs_resource(sc_name, sc_name, worker_nodes, min_size, max_size)

    if create_disks_for_lvs:
        # The disk size will be between the minSize and maxSize above to match the new LVS
        disk_size_in_gb = sum_of_two_storage_sizes(min_size, "10Gi")
        disk_size = int(disk_size_in_gb[:-2])
        worker_node_objs = get_node_objs(worker_nodes)
        for n in worker_node_objs:
            add_disk_to_node(n, disk_size=disk_size, ssd=ssd)

    return lvs_obj


def get_default_lvs_obj():
    """
    Get the default LocalVolumeSet object

    Returns:
        OCS: The OCS instance for the LocalVolumeSet resource

    """
    resource_name = constants.LOCAL_BLOCK_RESOURCE
    lvs_obj = OCP(
        kind=constants.LOCAL_VOLUME_SET,
        namespace=defaults.LOCAL_STORAGE_NAMESPACE,
    )
    if not lvs_obj.is_exist(resource_name=resource_name):
        raise ResourceNotFoundError(
            f"The LocalVolumeSet resource {resource_name} not found"
        )

    lvs_data = lvs_obj.get(resource_name=resource_name)
    return OCS(**lvs_data)


def add_disks_matching_lvs_size(worker_nodes, ssd=True):
    """
    Add new disks for an existing LocalVolumeSet resource
    The disk size will be equal to the existing OSD size.

    Args:
        worker_nodes (list): The worker node names to be used in the LocalVolumeSet resource.
        ssd (bool): if True, mark disk as SSD

    """
    osd_size = get_storage_size()
    log.info(f"the osd size is {osd_size}")

    # The disk size will be equal to the existing OSD size
    disk_size_in_gb = osd_size
    disk_size = int(disk_size_in_gb[:-2])
    worker_node_objs = get_node_objs(worker_nodes)
    for n in worker_node_objs:
        add_disk_to_node(n, disk_size=disk_size, ssd=ssd)


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


def verify_deviceclasses_steps():
    """
    The function verify the following:
    1. Wait for the DeviceSet PVCs to reach the Bound state.
    2. Wait for the OSD pods to reach the Running state.
    3. Check the Ceph state post add a new deviceclass as defined in the function
    'check_ceph_state_post_add_deviceclass'.

    """
    deviceset_name_per_count = get_deviceset_name_per_count()
    log.info(f"deviceclass name per count = {deviceset_name_per_count}")

    for deviceset_name, pvc_count in deviceset_name_per_count.items():
        wait_for_pvcs_in_deviceset_to_reach_status(
            deviceset_name, pvc_count, constants.STATUS_BOUND
        )

    osd_pods_count = sum(deviceset_name_per_count.values())
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


def verify_available_pvs_for_deviceclass(sc_name=None, wait=True, timeout=180):
    """
    Verify that sufficient available PVs exist for a new device class, and add
    disks to OSD nodes if needed.

    Args:
        sc_name (str): The storage class name to be used for the new device class. If None, it will use
            the first storage class name from the storage cluster.
        wait (bool): If True, it will wait for the new PVs to be available after adding disks.
        timeout (int): The maximum time to wait for the new PVs to be available after adding disks,
            in seconds.

    Returns:
        int: The number of PVs that are available for the new device class after adding disks if needed.

    """
    sc_name = sc_name or get_first_sc_name_from_storagecluster()
    osd_node_names = get_osd_running_nodes()
    log.info(f"osd node names = {osd_node_names}")
    available_pvs = get_pv_in_status(
        storage_class=sc_name, status=constants.STATUS_AVAILABLE
    )

    available_pvs_count = len(available_pvs)
    available_nodes_count = len(osd_node_names)
    if available_pvs_count >= available_nodes_count:
        log.info(
            f"There are already enough available PVs ({available_pvs_count}) to create a new device class, "
            f"no need to add new disks. The existing available PVs will be used for the new device class."
        )
        return available_pvs_count
    log.info("Adding new disks to the osd nodes to be used for the new device class")
    provision_pvs_count = available_nodes_count - available_pvs_count
    log.info(f"Number of PVs needed to be provisioned: {provision_pvs_count}")
    add_disks_matching_lvs_size(osd_node_names[:provision_pvs_count])

    if wait:
        log.info("Waiting for the new PVs to be available after adding disks")
        wait_for_pvs_in_lvs_to_reach_status(
            sc_name,
            available_nodes_count,
            constants.STATUS_AVAILABLE,
            timeout=timeout,
        )

    return available_nodes_count
