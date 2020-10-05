import logging
import time

from ocs_ci.ocs.node import get_typed_nodes, get_node_name
from ocs_ci.ocs.resources.pod import get_ocs_operator_pod, get_pod_node
from ocs_ci.ocs.platform_nodes import PlatformNodesFactory
from tests.helpers import wait_for_ct_pod_recovery

log = logging.getLogger(__name__)


def get_ocs_operator_node_name():
    """
    Getting node's name that running ocs-operator pod

    Returns:
        str: node's name that running ocs-operator pod

    """
    ocs_operator_pod = get_ocs_operator_pod()
    log.debug(f"ocs operator pod info: {ocs_operator_pod}")
    ocs_operator_node = get_pod_node(ocs_operator_pod)

    return get_node_name(ocs_operator_node)


def worker_node_shutdown(abrupt):
    """
    Shutdown worker node that running ocs-operator pod

    Args:
        abrupt: (bool): True if abrupt shutdown, False for permanent shutdown

    Raises:
        AssertionError: in case the ceph-tools pod was not recovered

    """

    nodes = PlatformNodesFactory().get_nodes_platform()
    log.info(f"Abrupt {abrupt}")
    # get ocs-operator node:
    ocs_operator_node_name = get_ocs_operator_node_name()

    # get workers node objects:
    node_to_shutdown = list()
    for node in get_typed_nodes():
        node_name = get_node_name(node)
        log.info(f"node: {node_name}, ocs operator node: {ocs_operator_node_name}")
        if node_name == ocs_operator_node_name:
            node_to_shutdown.append(node)
            log.info(f"node to shutdown: {get_node_name(node_to_shutdown[0])}")
            nodes.stop_nodes(node_to_shutdown)
            log.info("stop instance - done!")
            break

    log.info("Sleeping 5 minutes")
    time.sleep(320)
    assert wait_for_ct_pod_recovery(), "Ceph tools pod failed to come up on another node"
    if abrupt:
        log.info("Abrupt Shutdown")
        if node_to_shutdown:
            nodes.start_nodes(nodes=node_to_shutdown)
