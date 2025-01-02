import logging
import random

from ocs_ci.helpers.helpers import create_lvs_resource
from ocs_ci.ocs.node import add_disk_to_node
from ocs_ci.ocs.resources.storage_cluster import get_storage_size
from ocs_ci.utility.utils import sum_of_two_storage_sizes

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP


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
    lvs_result = ocp_lvs_obj.patch(params=params, format_type="json")
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
    max_size = sum_of_two_storage_sizes(old_lvs_max_size, "60Gi")
    suffix = "".join(random.choices("0123456789", k=5))
    sc_name = f"ssd{suffix}"
    lvs_obj = create_lvs_resource(sc_name, worker_nodes, min_size, max_size)

    if create_disks_for_lvs:
        disk_size_in_gb = sum_of_two_storage_sizes(old_lvs_max_size, "20Gi")
        disk_size = int(disk_size_in_gb[:-2])
        for n in worker_nodes:
            add_disk_to_node(n, disk_size=disk_size, ssd=ssd)

    return lvs_obj
