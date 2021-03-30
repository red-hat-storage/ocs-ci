import pytest
import logging
from time import sleep
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.pytest_customization.marks import (
    skipif_bm,
    skipif_aws_i3,
)
from ocs_ci.ocs import node, constants
from ocs_ci.framework.testlib import config, E2ETest, flowtests, ignore_leftovers
from ocs_ci.ocs import flowtest
from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.ocs.bucket_utils import BucketPolicyOps, ObcIOs
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.kms import noobaa_kms_validation, is_kms_enabled

logger = logging.getLogger(__name__)


@ignore_leftovers
@flowtests
class TestBaseOperationNodeRestart(E2ETest):
    """
    Tests Story/Flow based test scenario: Node Restart

    """

    @skipif_aws_i3
    @skipif_bm
    @pytest.mark.polarion_id("OCS-2487")
    def test_base_operation_node_restart(
        self,
        node_drain_teardown,
        node_restart_teardown,
        nodes,
        project_factory,
        pgsql_factory_fixture,
        couchbase_factory_fixture,
        multi_pvc_factory,
        mcg_obj,
        bucket_factory,
    ):
        """
        Test covers following flow operations while running workloads in the background:
        1. Node reboot
        2. Device replacement
        3. Bucket policy: put, modify, delete
        4. Node drain
        5. Node n/w failure
        6. nooba core pod delete with obc IO
        """
        logger.info("nooba kms validation..")
        if is_kms_enabled():
            noobaa_kms_validation()
        logger.info("Starting IO operations in Background")
        project = project_factory()

        logging.info("Started couchbase workload in background")

        bg_handler = flowtest.BackgroundOps()
        executor_run_bg_ios_ops = ThreadPoolExecutor(max_workers=5)

        cb_workload = executor_run_bg_ios_ops.submit(
            bg_handler.handler,
            couchbase_factory_fixture,
            replicas=3,
            skip_analyze=True,
            iterations=1,
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
        logging.info("Started pgsql workload in background")

        flow_ops = flowtest.FlowOperations()

        obc_ios = executor_run_bg_ios_ops.submit(
            bg_handler.handler,
            flow_ops.sanity_helpers.obc_put_obj_create_delete,
            mcg_obj,
            bucket_factory,
            iterations=40,
        )

        logger.info("Started pvc create delete operations")
        pvc_create_delete = executor_run_bg_ios_ops.submit(
            bg_handler.handler,
            flow_ops.sanity_helpers.create_pvc_delete,
            multi_pvc_factory,
            project,
            iterations=100,
        )
        logging.info("Started object IOs in background")
        logger.info("Starting operation 1: Node Restart")
        node_name = flow_ops.node_operations_entry_criteria(
            node_type="worker", number_of_nodes=1, operation_name="Node Restart"
        )

        # Node failure (reboot)
        nodes.restart_nodes(nodes=node_name)
        logger.info("Verifying exit criteria for operation 1: Node Restart")
        flow_ops.validate_cluster(node_status=True, operation_name="Node Restart")

        logger.info("TODO: Starting operation 2: Device replacement")

        logger.info("Starting operation 3: Add Capacity")
        # Add capacity
        osd_size = storage_cluster.get_osd_size()
        result = storage_cluster.add_capacity(osd_size)
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        pod.wait_for_resource(
            timeout=800,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=result * 3,
        )
        logger.info("Verifying exit criteria for operation 3: Add Capacity")

        logger.info("Starting operation 4: Bucket policy Put")

        bp_ops = BucketPolicyOps(bucket_factory, mcg_obj, interface="CLI")
        bp_ops.bucket_policy_put()

        logger.info("Starting operation 5: OSD pod failures")

        disruption = Disruptions()
        disruption.set_resource(resource="osd")
        disruption.delete_resource()

        logger.info("Starting operation 6: Bucket policy Modify")
        bp_ops.bucket_policy_modify()

        logger.info("Starting operation 7: Node Drain")
        node_name = flow_ops.node_operations_entry_criteria(
            node_type="worker", number_of_nodes=3, operation_name="Node Drain"
        )

        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([node_name[0].name], timeout=3600)
        # Make the node schedulable again
        node.schedule_nodes([node_name[0].name])
        logger.info("Verifying exit criteria for operation 7: Node Drain")
        flow_ops.validate_cluster(node_status=True, operation_name="Node Drain")

        logger.info("Starting operation 8: Bucket policy Modify")
        bp_ops.bucket_policy_delete()

        logger.info("Starting operation 9: network failure")

        node_name, nw_fail_time = flow_ops.node_operations_entry_criteria(
            node_type="worker",
            number_of_nodes=3,
            network_fail_time=300,
            operation_name="Node N/W failure",
        )

        #   Node n/w interface failure
        node.node_network_failure(node_name[0].name)
        logger.info(f"Waiting for {nw_fail_time} seconds")
        sleep(nw_fail_time)
        #   Reboot the unresponsive node(s)
        logger.info(f"Stop and start the unresponsive node(s): {node_name[0].name}")
        nodes.restart_nodes_by_stop_and_start(nodes=[node_name[0]])
        logger.info("Verifying exit criteria for operation 9: Node network fail")
        flow_ops.validate_cluster(node_status=True, operation_name="Node N/W failure")

        logger.info(
            "Waiting for final iteration of background operations to be completed"
        )

        bg_ops = [pvc_create_delete, obc_ios, pgsql_workload, cb_workload]
        bg_handler.wait_for_bg_operations(bg_ops, timeout=3600)

        bucket_name = bucket_factory(amount=1, interface="OC")[0].name
        obj1 = ObcIOs(mcg_obj, bucket_name)
        obc_ios_nooba_core_delete = executor_run_bg_ios_ops.submit(
            bg_handler.handler, obj1.obc_ios, iterations=1
        )

        if is_kms_enabled():
            logger.info("deleting nooba core pod..")
            disruption = Disruptions()
            disruption.set_resource(resource="nooba_core")
            disruption.delete_resource()
            bg_ops = [obc_ios_nooba_core_delete]
            bg_handler.wait_for_bg_operations(bg_ops, timeout=3600)
