import pytest
import logging
from time import sleep

from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    skipif_bm,
    skipif_aws_i3,
    skipif_vsphere_ipi,
)
from ocs_ci.ocs import node, constants
from ocs_ci.framework.testlib import E2ETest, flowtests, config, ignore_leftovers
from ocs_ci.ocs.cluster import is_flexible_scaling_enabled
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs import flowtest

logger = logging.getLogger(__name__)


@ignore_leftovers
@flowtests
class TestBaseOperationNodeDrain(E2ETest):
    """
    Tests Story/Flow based test scenario: Node Drain

    """

    @magenta_squad
    @skipif_aws_i3
    @skipif_bm
    @skipif_vsphere_ipi
    @pytest.mark.polarion_id("OCS-2188")
    def test_base_operation_node_drain(
        self,
        node_drain_teardown,
        node_restart_teardown,
        nodes,
        pgsql_factory_fixture,
        project_factory,
        multi_pvc_factory,
        mcg_obj,
        bucket_factory,
    ):
        """
        Test covers following flow operations while running workloads in the background:
        1. Node drain
        2. Add capacity
        3. Node reboot
        4. Node n/w failure

        """
        logger.test_step("Start background IO operations")
        logger.info("Initializing background workloads")
        project = project_factory()
        bg_handler = flowtest.BackgroundOps()
        executor_run_bg_ios_ops = ThreadPoolExecutor(max_workers=3)

        logger.info(
            "Starting PostgreSQL workload in background: replicas=1, clients=1, transactions=100"
        )
        pgsql_workload = executor_run_bg_ios_ops.submit(
            bg_handler.handler,
            pgsql_factory_fixture,
            replicas=1,
            clients=1,
            transactions=100,
            timeout=100,
            iterations=1,
        )
        logger.info("PostgreSQL workload started in background")

        flow_ops = flowtest.FlowOperations()

        logger.info("Starting OBC object IOs in background: 30 iterations")
        obc_ios = executor_run_bg_ios_ops.submit(
            bg_handler.handler,
            flow_ops.sanity_helpers.obc_put_obj_create_delete,
            mcg_obj,
            bucket_factory,
            iterations=30,
        )
        logger.info("OBC object IOs started in background")

        logger.info(
            "Starting PVC create/delete operations in background: 70 iterations"
        )
        pvc_create_delete = executor_run_bg_ios_ops.submit(
            bg_handler.handler,
            flow_ops.sanity_helpers.create_pvc_delete,
            multi_pvc_factory,
            project,
            iterations=70,
        )
        logger.info("PVC create/delete operations started in background")

        logger.test_step("Operation 1: Node Drain")
        logger.info("Selecting worker node for drain operation")
        node_name = flow_ops.node_operations_entry_criteria(
            node_type="worker", number_of_nodes=1, operation_name="Node Drain"
        )
        logger.info(f"Draining node: {node_name[0].name}")
        node.drain_nodes([node_name[0].name])
        logger.info(f"Making node schedulable again: {node_name[0].name}")
        node.schedule_nodes([node_name[0].name])
        logger.info("Verifying exit criteria for operation 1: Node Drain")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Node Drain"
        )
        logger.info("Operation 1 completed: Node Drain successful")

        logger.test_step("Operation 2: Add Capacity")
        logger.info("Capturing pre-add capacity state")
        osd_pods_before, restart_count_before = flow_ops.add_capacity_entry_criteria()
        osd_size = storage_cluster.get_osd_size()
        logger.info(f"Adding capacity with OSD size: {osd_size}")
        result = storage_cluster.add_capacity(osd_size)
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        if is_flexible_scaling_enabled():
            replica_count = 1
        else:
            replica_count = 3
        logger.info(
            f"Waiting for {result * replica_count} OSD pod(s) to be Running (replica_count={replica_count})"
        )
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=result * replica_count,
        )
        logger.info("Verifying exit criteria for operation 2: Add Capacity")
        flow_ops.add_capacity_exit_criteria(restart_count_before, osd_pods_before)
        logger.info("Operation 2 completed: Add Capacity successful")

        logger.test_step("Operation 3: Node Restart")
        logger.info("Selecting worker node for restart operation")
        node_name = flow_ops.node_operations_entry_criteria(
            node_type="worker", number_of_nodes=1, operation_name="Node Restart"
        )
        logger.info(f"Restarting node: {[n.name for n in node_name]}")
        nodes.restart_nodes(nodes=node_name)
        logger.info("Verifying exit criteria for operation 3: Node Restart")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Node Restart"
        )
        logger.info("Operation 3 completed: Node Restart successful")

        logger.test_step("Operation 4: Node Network Failure")
        logger.info("Selecting worker node for network failure operation")
        node_name, nw_fail_time = flow_ops.node_operations_entry_criteria(
            node_type="worker",
            number_of_nodes=1,
            network_fail_time=300,
            operation_name="Node N/W failure",
        )
        logger.info(f"Triggering network failure on node: {node_name[0].name}")
        node.node_network_failure(node_name[0].name)
        logger.info(f"Waiting {nw_fail_time} seconds for network failure impact")
        sleep(nw_fail_time)
        logger.info(f"Restarting unresponsive node: {node_name[0].name}")
        nodes.restart_nodes_by_stop_and_start(nodes=node_name)
        logger.info("Verifying exit criteria for operation 4: Node network fail")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Node N/W failure"
        )
        logger.info("Operation 4 completed: Node Network Failure recovery successful")

        logger.test_step("Wait for all background operations to complete")
        logger.info("Waiting for final iteration of background operations to complete")
        bg_ops = [pvc_create_delete, obc_ios, pgsql_workload]
        bg_handler.wait_for_bg_operations(bg_ops, timeout=600)
        logger.info("All background operations completed successfully")
