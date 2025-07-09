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


log = logging.getLogger(__name__)
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
            log.warning(
                f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
            )
            if not_ready_nodes:
                nodes.restart_nodes_by_stop_and_start(not_ready_nodes)
                wait_for_nodes_status()

            log.info("All nodes are in Ready status")

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
        # Get all amq pods
        pod_obj_list = get_all_pods(namespace=constants.AMQ_NAMESPACE)

        # Get the node list
        node = get_nodes(node_type, num_of_nodes=1)

        # Reboot one master nodes
        nodes.restart_nodes(node, wait=False)

        # Wait some time after rebooting master
        waiting_time = 90
        log.info(f"Waiting {waiting_time} seconds...")
        time.sleep(waiting_time)

        # Validate all nodes and services are in READY state and up
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(ocp.wait_for_cluster_connectivity(tries=400))
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check(tries=40)

        # Check all amq pods are up and running
        assert POD.wait_for_resource(
            condition="Running", resource_count=len(pod_obj_list), timeout=300
        )

        # Validate the results
        log.info("Validate message run completely")
        for thread in self.threads:
            thread.result(timeout=1800)

    @pytest.mark.polarion_id("OCS-1278")
    @skipif_ibm_cloud
    def test_amq_after_shutdown_and_recovery_worker_node(self, nodes, amq_setup):
        """
        Test case to validate shutdown and recovery node
        shouldn't effect amq workloads running in background

        """
        # Get all amq pods
        pod_obj_list = get_all_pods(namespace=constants.AMQ_NAMESPACE)

        # Get the node list
        node = get_nodes(node_type="worker", num_of_nodes=1)

        # Reboot one master nodes
        nodes.stop_nodes(nodes=node)

        waiting_time = 20
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(waiting_time)

        nodes.start_nodes(nodes=node)

        # Validate all nodes are in READY state and up
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check(tries=40)

        # Check all amq pods are up and running
        assert POD.wait_for_resource(
            condition="Running", resource_count=len(pod_obj_list), timeout=300
        )

        # Validate the results
        log.info("Validate message run completely")
        for thread in self.threads:
            thread.result(timeout=1800)
