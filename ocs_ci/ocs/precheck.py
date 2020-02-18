import logging
from ocs_ci.ocs.resources.pod import get_osd_pods

log = logging.getLogger(__name__)


def check_max_osds_per_node(max_number_osds=3):
    """
    Function to check there is less than `max_number_osds` per node

    Args:
        max_number_osds (int): upper bound

    Returns:
        bool: True if condition is satisfied

    """
    osd_on_node_count = {}
    for pod in get_osd_pods():
        node_name = pod.pod_data['spec']['nodeName']
        osd_on_node_count[node_name] = osd_on_node_count.get(node_name, 0) + 1
    for count in osd_on_node_count.values():
        if count >= max_number_osds:
            return False
    return True
