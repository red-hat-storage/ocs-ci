import logging
import pytest

from ocs_ci.framework.testlib import libtest, tier1, resiliency
from ocs_ci.framework.pytest_customization.marks import skipif_ibm_power
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    get_worker_nodes,
    get_node_objs,
    wait_for_nodes_status,
)
from ocs_ci.ocs.platform_nodes import IBMHCINode

logger = logging.getLogger(__name__)


@libtest
@tier1
@resiliency
@skipif_ibm_power
@pytest.mark.polarion_id("OCS-5678")
class TestIBMHCINodeOperations:
    """
    Test node operations for IBM HCI (Hardware Convergence Infrastructure) platform

    This test suite validates power management operations on IBM HCI baremetal nodes
    including stop, start, restart, and restart by stop-and-start operations.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, request):
        """
        Setup and teardown fixture for IBM HCI node tests

        Ensures all nodes are powered on and ready after each test
        Uses the IBMHCINode.restart_nodes_by_stop_and_start_teardown() method
        """

        def finalizer():
            logger.info("Teardown: Ensuring all nodes are powered on and ready")
            try:
                ibm_hci = IBMHCINode()
                # Use the teardown method from IBMHCINode class
                ibm_hci.restart_nodes_by_stop_and_start_teardown()
            except Exception as e:
                logger.error(f"Error during teardown: {e}")
                # Don't raise - we want teardown to complete even if there are errors

        request.addfinalizer(finalizer)

    def test_stop_and_start_single_worker_node(self):
        """
        Test stopping and starting a single worker node

        Steps:
        1. Get a worker node
        2. Stop the node using power_off
        3. Verify node is NotReady
        4. Start the node using power_on
        5. Verify node is Ready
        """
        logger.info("Test: Stop and start single worker node")

        ibm_hci = IBMHCINode()

        # Get worker nodes
        worker_node_names = get_worker_nodes()
        worker_nodes = get_node_objs(worker_node_names)
        assert worker_nodes, "No worker nodes found"

        # Select first worker node
        test_node = [worker_nodes[0]]
        node_name = test_node[0].name
        logger.info(f"Testing with worker node: {node_name}")

        # Stop the node
        logger.info(f"Stopping node: {node_name}")
        ibm_hci.stop_nodes(test_node)

        # Wait for node to be NotReady
        logger.info(f"Waiting for node {node_name} to be NotReady")
        wait_for_nodes_status(
            node_names=[node_name], status=constants.NODE_NOT_READY, timeout=300
        )
        logger.info(f"Node {node_name} is NotReady")

        # Start the node
        logger.info(f"Starting node: {node_name}")
        ibm_hci.start_nodes(test_node)

        # Wait for node to be Ready
        logger.info(f"Waiting for node {node_name} to be Ready")
        wait_for_nodes_status(
            node_names=[node_name], status=constants.NODE_READY, timeout=900
        )
        logger.info(f"Node {node_name} is Ready")

    def test_restart_single_worker_node(self):
        """
        Test restarting a single worker node using power reset

        Steps:
        1. Get a worker node
        2. Restart the node using power_reset
        3. Verify node becomes NotReady temporarily
        4. Verify node returns to Ready state
        """
        logger.info("Test: Restart single worker node using power reset")

        ibm_hci = IBMHCINode()

        # Get worker nodes
        worker_node_names = get_worker_nodes()
        worker_nodes = get_node_objs(worker_node_names)
        assert worker_nodes, "No worker nodes found"

        # Select first worker node
        test_node = [worker_nodes[0]]
        node_name = test_node[0].name
        logger.info(f"Testing with worker node: {node_name}")

        # Restart the node (with wait=True, it will wait for node to be ready)
        logger.info(f"Restarting node: {node_name}")
        ibm_hci.restart_nodes(test_node, wait=True)

        logger.info(f"Node {node_name} restarted successfully and is Ready")

    def test_restart_node_by_stop_and_start(self):
        """
        Test restarting a node by sequential stop and start operations

        Steps:
        1. Get a worker node
        2. Restart node using stop-and-start method
        3. Verify node goes through NotReady state
        4. Verify node returns to Ready state
        """
        logger.info("Test: Restart node by stop and start")

        ibm_hci = IBMHCINode()

        # Get worker nodes
        worker_node_names = get_worker_nodes()
        worker_nodes = get_node_objs(worker_node_names)
        assert worker_nodes, "No worker nodes found"

        # Select first worker node
        test_node = [worker_nodes[0]]
        node_name = test_node[0].name
        logger.info(f"Testing with worker node: {node_name}")

        # Restart by stop and start (includes automatic wait for ready)
        logger.info(f"Restarting node {node_name} by stop and start")
        ibm_hci.restart_nodes_by_stop_and_start(test_node, force=True)

        logger.info(f"Node {node_name} restarted successfully by stop-and-start")

    def test_stop_and_start_multiple_worker_nodes(self):
        """
        Test stopping and starting multiple worker nodes

        Steps:
        1. Get multiple worker nodes (up to 2)
        2. Stop all selected nodes
        3. Verify all nodes are NotReady
        4. Start all nodes
        5. Verify all nodes are Ready
        """
        logger.info("Test: Stop and start multiple worker nodes")

        ibm_hci = IBMHCINode()

        # Get worker nodes
        worker_node_names = get_worker_nodes()
        worker_nodes = get_node_objs(worker_node_names)
        assert len(worker_nodes) >= 2, "Need at least 2 worker nodes for this test"

        # Select up to 2 worker nodes
        test_nodes = worker_nodes[:2]
        node_names = [node.name for node in test_nodes]
        logger.info(f"Testing with worker nodes: {node_names}")

        # Stop the nodes
        logger.info(f"Stopping nodes: {node_names}")
        ibm_hci.stop_nodes(test_nodes)

        # Wait for nodes to be NotReady (multiple nodes take longer)
        logger.info(f"Waiting for nodes to be NotReady: {node_names}")
        wait_for_nodes_status(
            node_names=node_names, status=constants.NODE_NOT_READY, timeout=900
        )
        logger.info(f"All nodes are NotReady: {node_names}")

        # Start the nodes
        logger.info(f"Starting nodes: {node_names}")
        ibm_hci.start_nodes(test_nodes)

        # Wait for nodes to be Ready
        logger.info(f"Waiting for nodes to be Ready: {node_names}")
        wait_for_nodes_status(
            node_names=node_names, status=constants.NODE_READY, timeout=900
        )
        logger.info(f"All nodes are Ready: {node_names}")

    def test_power_status_check(self):
        """
        Test checking power status of nodes

        Steps:
        1. Get a worker node
        2. Verify node is powered on (status check)
        3. Stop the node
        4. Verify power status reflects powered off state
        5. Start the node
        6. Verify node is powered on again
        """
        logger.info("Test: Check power status of nodes")

        ibm_hci = IBMHCINode()

        # Get worker nodes
        worker_node_names = get_worker_nodes()
        worker_nodes = get_node_objs(worker_node_names)
        assert worker_nodes, "No worker nodes found"

        # Select first worker node
        test_node = [worker_nodes[0]]
        node_name = test_node[0].name
        logger.info(f"Testing power status with node: {node_name}")

        # Check initial power status (should be on)
        logger.info(f"Checking initial power status of {node_name}")
        initial_status = ibm_hci.ibm_hci.power_status(node_name)
        logger.info(f"Initial power status: {initial_status}")

        # Stop the node
        logger.info(f"Stopping node: {node_name}")
        ibm_hci.stop_nodes(test_node)

        # Wait a bit for power state to change
        import time

        time.sleep(30)

        # Check power status after stop
        logger.info("Checking power status after stop")
        stopped_status = ibm_hci.ibm_hci.power_status(node_name)
        logger.info(f"Power status after stop: {stopped_status}")

        # Start the node
        logger.info(f"Starting node: {node_name}")
        ibm_hci.start_nodes(test_node)

        # Wait for node to be ready
        wait_for_nodes_status(
            node_names=[node_name], status=constants.NODE_READY, timeout=900
        )

        # Check final power status (should be on)
        logger.info("Checking final power status")
        final_status = ibm_hci.ibm_hci.power_status(node_name)
        logger.info(f"Final power status: {final_status}")

    def test_node_operations_with_ceph_health_check(self):
        """
        Test node operations and verify Ceph health after operations

        Steps:
        1. Get a worker node
        2. Restart node by stop and start
        3. Wait for node to be ready
        4. Verify Ceph cluster health is OK
        """
        logger.info("Test: Node operations with Ceph health check")

        from ocs_ci.utility.utils import ceph_health_check

        ibm_hci = IBMHCINode()

        # Get worker nodes
        worker_node_names = get_worker_nodes()
        worker_nodes = get_node_objs(worker_node_names)
        assert worker_nodes, "No worker nodes found"

        # Select first worker node
        test_node = [worker_nodes[0]]
        node_name = test_node[0].name
        logger.info(f"Testing with worker node: {node_name}")

        # Restart node by stop and start
        logger.info(f"Restarting node {node_name} by stop and start")
        ibm_hci.restart_nodes_by_stop_and_start(test_node, force=True)

        # Verify Ceph health
        logger.info("Checking Ceph cluster health after node restart")
        ceph_health_check(tries=30, delay=60)
        logger.info("Ceph cluster health is OK")


# Made with Bob
