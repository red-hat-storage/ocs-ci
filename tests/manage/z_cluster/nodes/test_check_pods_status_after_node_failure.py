import logging
import pytest
import random

from ocs_ci.framework.testlib import ManageTest, tier4a, ignore_leftovers
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_ocs_nodes,
    get_nodes_in_statuses,
    get_node_pods,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
    get_pod_name_by_pattern,
    get_pod_obj,
)

log = logging.getLogger(__name__)


def get_rook_ceph_pods_not_in_node(node_name):
    """
    Get all the rook ceph pods that are not running on the node

    Args:
        node_name (str): The node name

    Returns:
        list: List of the rook ceph pod objects that are not running on the node 'node_name'

    """
    rook_ceph_pod_names_set = set(get_pod_name_by_pattern("rook-ceph-"))
    node_pods = get_node_pods(node_name)
    node_pod_names_set = set([p.name for p in node_pods])
    rook_ceph_pod_names_not_in_node = list(rook_ceph_pod_names_set - node_pod_names_set)

    return [get_pod_obj(pod_name) for pod_name in rook_ceph_pod_names_not_in_node]


@ignore_leftovers
@tier4a
class TestCheckPodsAfterNodeFailure(ManageTest):
    """
    Test check pods status after a node failure event.

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """

        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again

        """

        def finalizer():
            not_ready_nodes = get_nodes_in_statuses([constants.NODE_NOT_READY])
            not_ready_node_names = [n.name for n in not_ready_nodes]
            log.warning(
                f"We have nodes in not ready statuses: {not_ready_node_names}. "
                f"Starting the nodes that are not ready..."
            )
            nodes.restart_nodes(not_ready_nodes)
            wait_for_nodes_status(node_names=not_ready_node_names)

        request.addfinalizer(finalizer)

    def test_check_pods_status_after_node_failure(self, nodes, node_restart_teardown):
        """
        Test check pods status after a node failure event.
        All the rook ceph pods should be in "Running" or "Completed"
        state after a node failure event.

        """
        ocs_nodes = get_ocs_nodes()
        if not ocs_nodes:
            pytest.skip("We don't have ocs nodes in the cluster")

        ocs_node = random.choice(ocs_nodes)
        node_name = ocs_node.name
        log.info(f"Selected node is '{node_name}'")
        # Save the rook ceph pods before shutting down the node
        rook_ceph_pods_not_in_node = get_rook_ceph_pods_not_in_node(node_name)

        log.info(f"Shutting down node '{node_name}'")
        nodes.stop_nodes([ocs_node])
        wait_for_nodes_status(node_names=[node_name], status=constants.NODE_NOT_READY)
        log.info(f"The node '{node_name}' reached '{constants.NODE_NOT_READY}' status")

        timeout = 1800
        log.info("Check the rook pods are in 'Running' or 'Completed' state")
        are_pods_running = wait_for_pods_to_be_running(
            pods_to_check=rook_ceph_pods_not_in_node, timeout=timeout, sleep=30
        )
        assert are_pods_running, f"The pods are not 'Running' after {timeout} seconds"

        log.info("All the pods are in 'Running' or 'Completed' state")
        log.info(f"Starting the node '{node_name}' again...")
        nodes.start_nodes(nodes=[ocs_node])
        wait_for_nodes_status(node_names=[node_name])

        log.info(
            "Waiting for all the pods to be running and cluster health to be OK..."
        )
        wait_for_pods_to_be_running(timeout=600)
        self.sanity_helpers.health_check(tries=40)
