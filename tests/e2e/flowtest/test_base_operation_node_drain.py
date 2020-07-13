import pytest
import logging
from time import sleep

from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.pytest_customization.marks import aws_platform_required, vsphere_platform_required
from ocs_ci.ocs import node, constants
from ocs_ci.framework.testlib import E2ETest, tier2, config, ignore_leftovers
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import storage_cluster
from tests.e2e.flowtest import flowtest_helpers
from tests.e2e.flowtest.flowtest_helpers import create_pvc_delete, \
    obc_put_obj_create_delete, FlowOperations

logger = logging.getLogger(__name__)


@ignore_leftovers
@tier2
class TestFlowBasedTests(E2ETest):
    """
    Tests Story/Flow based test scenarios

    """
    @vsphere_platform_required
    @aws_platform_required
    @pytest.mark.polarion_id("OCS-xsadsa")
    def test_base_operation_node_drain(
        self, node_drain_teardown, node_restart_teardown, pgsql_factory_fixture,
        project_factory, multi_pvc_factory, mcg_obj, bucket_factory
    ):
        """
        Test covers following flow operations while running workloads in the background:
        1. Node drain
        2. Add capacity
        3. Node reboot
        4. Node n/w failure

        """
        logger.info("Starting IO operations in Background")
        project = project_factory()

        bg_wrap = flowtest_helpers.BackgroundOps()
        executor_run_bg_ios_ops = ThreadPoolExecutor(max_workers=3)

        logging.info("Started object IOs in background")
        obc_ios = executor_run_bg_ios_ops.submit(
            bg_wrap.wrap, obc_put_obj_create_delete, mcg_obj, bucket_factory, iterations=50
        )
        logging.info("Started pvc create and delete in background")
        pvc_create_delete = executor_run_bg_ios_ops.submit(
            bg_wrap.wrap,
            create_pvc_delete,
            multi_pvc_factory,
            project,
            iterations=100
        )
        logging.info("Started pgsql workload in background")
        pgsql_workload = executor_run_bg_ios_ops.submit(
            bg_wrap.wrap,
            pgsql_factory_fixture,
            replicas=1, clients=1, transactions=100,
            timeout=100, iterations=1
        )

        flow_ops = FlowOperations()

        logger.info("Starting operation 1: Node Drain")
        node_name, nodes = flow_ops.node_operations_entry_criteria(
            node_type='worker', number_of_nodes=1
        )
        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([node_name[0].name])
        # Make the node schedulable again
        node.schedule_nodes([node_name[0].name])
        logger.info("Verifying exit criteria of operation 1: Node Drain")
        flow_ops.node_operations_exit_criteria()

        logger.info("Starting operation 2: Add Capacity")
        osd_pods_before, restart_count_before = flow_ops.add_capacity_entry_criteria()
        # Add capacity
        osd_size = storage_cluster.get_osd_size()
        result = storage_cluster.add_capacity(osd_size)
        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=result * 3
        )
        logger.info("Verifying exit criteria of operation 2: Add Capacity")
        flow_ops.add_capacity_exit_criteria(restart_count_before, osd_pods_before)

        logger.info("Starting operation 3: Node Restart")
        node_name, node_platform = flow_ops.node_operations_entry_criteria(
            node_type='worker', number_of_nodes=1
        )
        # Node failure (reboot)
        node_platform.restart_nodes(nodes=node_name)
        logger.info("Verifying exit criteria of operation 3: Node Restart")
        flow_ops.node_operations_exit_criteria()

        logger.info("Starting operation 4: Node network fail")
        node_name, node_platform, nw_fail_time = flow_ops.node_operations_entry_criteria(
            node_type='worker', number_of_nodes=1, network_fail_time=100
        )
        # Node n/w interface failure
        node.node_network_failure(node_name[0].name)
        logger.info(f"Waiting for {nw_fail_time} seconds")
        sleep(nw_fail_time)
        # Reboot the unresponsive node(s)
        logger.info(f"Stop and start the unresponsive node(s): {node_name[0].name}")
        node_platform.restart_nodes_by_stop_and_start(nodes=node_name)
        logger.info("Verifying exit criteria of operation 4: Node network fail")
        flow_ops.node_operations_exit_criteria()

        logger.info("Waiting for final iteration of background operations to be completed")
        flowtest_helpers.BackgroundOps.OPERATION_COMPLETED = True
        bg_ops = [pvc_create_delete, obc_ios, pgsql_workload]
        bg_wrap.wait_for_bg_operations(bg_ops, timeout=600)
