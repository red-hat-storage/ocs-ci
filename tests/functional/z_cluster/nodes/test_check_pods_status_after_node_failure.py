import logging
import pytest
import random

from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier4a,
    ignore_leftovers,
    skipif_ibm_cloud,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_external_mode,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_ocs_nodes,
    get_nodes_in_statuses,
    get_node_pods,
    get_node_osd_ids,
    get_node_mon_ids,
    get_worker_nodes,
    wait_for_node_count_to_reach_status,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
    get_osd_pods,
    get_osd_pod_id,
    wait_for_change_in_pods_statuses,
    get_rook_ceph_pod_names,
    get_mon_pods,
    get_mon_pod_id,
    check_pods_after_node_replacement,
    get_pod_objs,
)
from ocs_ci.ocs.cluster import is_managed_service_cluster

log = logging.getLogger(__name__)


def get_rook_ceph_pod_names_not_in_node(node_name):
    """
    Get all the rook ceph pod names that are not running on the node

    Args:
        node_name (str): The node name

    Returns:
        list: List of the rook ceph pod names that are not running on the node 'node_name'

    """
    rook_ceph_pod_names_set = set(get_rook_ceph_pod_names())
    node_pods = get_node_pods(node_name)
    node_pod_names_set = set([p.name for p in node_pods])
    rook_ceph_pod_names_not_in_node = list(rook_ceph_pod_names_set - node_pod_names_set)

    return rook_ceph_pod_names_not_in_node


def get_rook_ceph_pod_names_in_node(node_name):
    """
    Get all the rook ceph pod names that are running on the node

    Args:
        node_name (str): The node name

    Returns:
        list: List of the rook ceph pod names that are running on the node 'node_name'

    """
    rook_ceph_pods = get_pod_objs(get_rook_ceph_pod_names())
    rook_ceph_pods_in_node = get_node_pods(node_name, rook_ceph_pods)
    rook_ceph_pod_names_in_node = [p.name for p in rook_ceph_pods_in_node]

    return rook_ceph_pod_names_in_node


def wait_for_change_in_rook_ceph_pods(node_name, timeout=300, sleep=20):
    """
    Wait for change in the rook ceph pod statuses running on the node

    Args:
        node_name (str): The node name
        timeout (int): Time to wait for the rook ceph pod statuses to change
        sleep (int): Time to wait between iterations

    Returns:
        bool: True, if the rook ceph pods statuses have changed. False, otherwise

    """
    rook_ceph_pod_names_in_node = get_rook_ceph_pod_names_in_node(node_name)
    is_rook_ceph_pods_status_changed = wait_for_change_in_pods_statuses(
        rook_ceph_pod_names_in_node, timeout=timeout, sleep=sleep
    )
    return is_rook_ceph_pods_status_changed


@brown_squad
@ignore_leftovers
@tier4a
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_external_mode
@pytest.mark.polarion_id("OCS-2552")
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
            if not_ready_nodes:
                not_ready_node_names = [n.name for n in not_ready_nodes]
                log.warning(
                    f"We have nodes in not ready statuses: {not_ready_node_names}. "
                    f"Starting the nodes that are not ready..."
                )
                nodes.restart_nodes(not_ready_nodes)
                wait_for_nodes_status(node_names=not_ready_node_names)
            else:
                log.info("All the nodes are in 'Ready' state")

        request.addfinalizer(finalizer)

    @skipif_ibm_cloud
    def test_check_pods_status_after_node_failure(self, nodes, node_restart_teardown):
        """
        Test check pods status after a node failure event.
        All the rook ceph pods should be in "Running" or "Completed"
        state after a node failure event.

        """
        ocs_nodes = get_ocs_nodes()
        if not ocs_nodes:
            pytest.skip("We don't have ocs nodes in the cluster")

        wnodes = get_worker_nodes()

        ocs_node = random.choice(ocs_nodes)
        node_name = ocs_node.name
        log.info(f"Selected node is '{node_name}'")
        # Save the rook ceph pods, the osd ids, and the mon ids before shutting down the node
        rook_ceph_pod_names_not_in_node = get_rook_ceph_pod_names_not_in_node(node_name)
        node_osd_ids = get_node_osd_ids(node_name)
        node_mon_ids = get_node_mon_ids(node_name)

        log.info(f"Shutting down node '{node_name}'")
        nodes.stop_nodes([ocs_node])
        wait_for_nodes_status(node_names=[node_name], status=constants.NODE_NOT_READY)
        log.info(f"The node '{node_name}' reached '{constants.NODE_NOT_READY}' status")

        log.info("Wait for a change in the node rook ceph pod statuses...")
        timeout = 420
        is_rook_ceph_pods_status_changed = wait_for_change_in_rook_ceph_pods(
            node_name, timeout=timeout
        )
        assert (
            is_rook_ceph_pods_status_changed
        ), f"The node rook ceph pods status didn't change after {timeout} seconds"

        log.info("Check the rook ceph pods are in 'Running' or 'Completed' state")
        previous_timeout = timeout
        timeout = 600
        are_pods_running = wait_for_pods_to_be_running(
            pod_names=rook_ceph_pod_names_not_in_node, timeout=timeout, sleep=30
        )
        assert are_pods_running, (
            f"Increased timeout from {previous_timeout} to {timeout} seconds, "
            f"The pods are not 'Running' even after {timeout} seconds"
        )

        # Get the rook ceph pods without the osd, and mon pods have the old node ids
        osd_pods = get_osd_pods()
        new_node_osd_id_names_set = {
            p.name for p in osd_pods if get_osd_pod_id(p) in node_osd_ids
        }
        mon_pods = get_mon_pods()
        new_node_mon_id_names_set = {
            p.name for p in mon_pods if get_mon_pod_id(p) in node_mon_ids
        }

        new_node_osd_mon_id_names_set = new_node_osd_id_names_set.union(
            new_node_mon_id_names_set
        )
        rook_ceph_pod_names_set = set(get_rook_ceph_pod_names())
        new_rook_ceph_pod_names = list(
            rook_ceph_pod_names_set - new_node_osd_mon_id_names_set
        )

        log.info(
            "Verify that the new rook ceph pods are in 'Running' or 'Completed' state"
        )
        timeout = 300
        are_new_pods_running = wait_for_pods_to_be_running(
            pod_names=new_rook_ceph_pod_names, timeout=timeout, sleep=20
        )
        assert (
            are_new_pods_running
        ), f"The new pods are not 'Running' after {timeout} seconds"

        log.info("All the pods are in 'Running' or 'Completed' state")

        if is_managed_service_cluster():
            log.info(
                "When we use the managed service, the worker node should recover automatically "
                "by starting the node or removing it, and creating a new one."
                "Waiting for all the worker nodes to be ready..."
            )
            wait_for_node_count_to_reach_status(node_count=len(wnodes), timeout=900)
            log.info("Waiting for all the pods to be running")
            assert check_pods_after_node_replacement(), "Not all the pods are running"
        else:
            log.info(f"Starting the node '{node_name}' again....")
            nodes.start_nodes(nodes=[ocs_node])
            wait_for_nodes_status(node_names=[node_name], timeout=360)
            log.info("Waiting for all the pods to be running")
            wait_for_pods_to_be_running(timeout=600)

        log.info("Checking that the cluster health is OK...")
        self.sanity_helpers.health_check(tries=40)
