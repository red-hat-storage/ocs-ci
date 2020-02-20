import logging

from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


def check_max_osds_per_node(max_number_osds=2):
    """
    Function to check there is less than or equal to `max_number_osds` per node

    Args:
        max_number_osds (int): upper bound

    Raises:
        UnexpectedBehaviour if number of osds is exceeded

    """
    osd_on_node_count = {}
    for pod in get_osd_pods():
        node_name = pod.pod_data['spec']['nodeName']
        osd_on_node_count[node_name] = osd_on_node_count.get(node_name, 0) + 1
    for node_name, count in osd_on_node_count.items():
        if count > max_number_osds:
            raise UnexpectedBehaviour(
                f"Node {node_name} runs {count} OSDs, maximum {max_number_osds} is allowed "
            )
