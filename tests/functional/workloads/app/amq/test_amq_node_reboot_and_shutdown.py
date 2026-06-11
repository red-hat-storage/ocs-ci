import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import (
    skipif_vsphere_ipi,
    skipif_ibm_cloud,
    magenta_squad,
    skipif_rosa_hcp,
)
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs, get_nodes
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)
POD = ocp.OCP(kind=constants.POD, namespace=constants.AMQ_NAMESPACE)
TILLER_NAMESPACE = "tiller"


@magenta_squad
@ignore_leftovers
@workloads
@skipif_vsphere_ipi
class TestAMQNodeReboot(E2ETest):
    """
    Test case to reboot or shutdown and recovery
    node when amq workload is running

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
        Restart nodes that are in status NotReady
        for situations in which the test failed in between

        """

        def finalizer():

            # Validate all nodes are in READY state
            not_ready_nodes = [
                n
                for n in get_node_objs()
                if n.ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
            ]
            logger.warning(
                f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
            )
            if not_ready_nodes:
                nodes.restart_nodes_by_stop_and_start(not_ready_nodes)
                wait_for_nodes_status()

            logger.info("All nodes are in Ready status")

        request.addfinalizer(finalizer)

    @pytest.fixture()
    def amq_setup(self, amq_factory_fixture):
        """
        Creates amq cluster and run benchmarks
        """
        sc_name = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)
        self.amq, self.threads = retry(CommandFailed, tries=60, delay=3, backoff=1)(
            amq_factory_fixture
        )(sc_name=sc_name.name)

    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*["worker"], marks=pytest.mark.polarion_id("OCS-1282")),
            pytest.param(
                *["master"],
                marks=[pytest.mark.polarion_id("OCS-1281"), skipif_rosa_hcp],
            ),
        ],
    )
    def test_amq_after_rebooting_node(self, node_type, nodes, amq_setup):
        """
        Test case to validate rebooting master node shouldn't effect
        amq workloads running in background

        """
        logger.test_step(f"Get all AMQ pods and {node_type} node for reboot")
        pod_obj_list = get_all_pods(namespace=constants.AMQ_NAMESPACE)
        logger.info(f"Found {len(pod_obj_list)} AMQ pods")

        node = get_nodes(node_type, num_of_nodes=1)
        logger.info(f"Selected {node_type} node for reboot: {node[0].name}")

        logger.test_step(f"Reboot {node_type} node")
        nodes.restart_nodes(node, wait=False)
        logger.info(f"Initiated restart of node: {node[0].name}")

        waiting_time = 90
        logger.info(f"Waiting {waiting_time}s for node to reboot")
        time.sleep(waiting_time)

        logger.test_step("Wait for cluster connectivity and all nodes to be Ready")
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(ocp.wait_for_cluster_connectivity(tries=400))
        logger.info("Cluster connectivity restored")

        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))
        logger.info("All nodes are in Ready state")

        logger.test_step("Verify cluster health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster health check passed")

        logger.test_step("Verify all AMQ pods are running")
        pods_running = POD.wait_for_resource(
            condition="Running", resource_count=len(pod_obj_list), timeout=300
        )
        logger.assertion(
            f"AMQ pods status: expected_count={len(pod_obj_list)}, "
            f"running={pods_running}, condition='Running'"
        )
        assert (
            pods_running
        ), f"Not all AMQ pods are running after {node_type} node reboot"

        logger.test_step("Validate AMQ message processing completed")
        for thread in self.threads:
            thread.result(timeout=1800)
        logger.info("All AMQ message threads completed successfully")

    @pytest.mark.polarion_id("OCS-1278")
    @skipif_ibm_cloud
    def test_amq_after_shutdown_and_recovery_worker_node(self, nodes, amq_setup):
        """
        Test case to validate shutdown and recovery node
        shouldn't effect amq workloads running in background

        """
        logger.test_step("Get all AMQ pods and worker node for shutdown/recovery")
        pod_obj_list = get_all_pods(namespace=constants.AMQ_NAMESPACE)
        logger.info(f"Found {len(pod_obj_list)} AMQ pods")

        node = get_nodes(node_type="worker", num_of_nodes=1)
        logger.info(f"Selected worker node for shutdown: {node[0].name}")

        logger.test_step("Shutdown worker node")
        nodes.stop_nodes(nodes=node)
        logger.info(f"Worker node {node[0].name} stopped")

        waiting_time = 20
        logger.info(f"Waiting {waiting_time}s before recovery")
        time.sleep(waiting_time)

        logger.test_step("Start worker node (recovery)")
        nodes.start_nodes(nodes=node)
        logger.info(f"Worker node {node[0].name} started")

        logger.test_step("Wait for all nodes to be Ready")
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))
        logger.info("All nodes are in Ready state")

        logger.test_step("Verify cluster health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster health check passed")

        logger.test_step("Verify all AMQ pods are running")
        pods_running = POD.wait_for_resource(
            condition="Running", resource_count=len(pod_obj_list), timeout=300
        )
        logger.assertion(
            f"AMQ pods status: expected_count={len(pod_obj_list)}, "
            f"running={pods_running}, condition='Running'"
        )
        assert pods_running, "Not all AMQ pods are running after worker node recovery"

        logger.test_step("Validate AMQ message processing completed")
        for thread in self.threads:
            thread.result(timeout=1800)
        logger.info("All AMQ message threads completed successfully")
