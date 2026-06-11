import logging
import pytest
import random
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.ocs import node
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.node import get_node_resource_utilization_from_adm_top

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def pgsql(request):

    pgsql = Postgresql()

    def teardown():
        pgsql.cleanup()

    request.addfinalizer(teardown)
    return pgsql


@magenta_squad
@ignore_leftovers
@workloads
@pytest.mark.polarion_id("OCS-820")
class TestPgSQLNodeReboot(E2ETest):
    """
    Test running PGSQL and with Ceph pods respin
    """

    @pytest.fixture()
    def pgsql_setup(self, pgsql):
        """
        PGSQL test setup
        """
        logger.info(
            "Setting up PostgreSQL environment with node drain test configuration"
        )
        pgsql.setup_postgresql(replicas=1)
        logger.info("PostgreSQL deployed with 1 replica")

        self.sanity_helpers = Sanity()

    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_node_drain(self, pgsql, node_type="worker"):
        """
        Test pgsql workload
        """
        logger.test_step("Create pgbench benchmark: 1 replica, 600 transactions")
        pgsql.create_pgbench_benchmark(replicas=1, transactions=600)
        logger.info("pgbench benchmark created")

        start_time = datetime.now()
        logger.info(f"Benchmark start time: {start_time}")

        logger.test_step("Wait for pgbench to reach running state")
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)
        logger.info(f"pgbench reached status: {constants.STATUS_RUNNING}")

        logger.test_step("Check worker node resource utilization")
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        logger.test_step(f"Identify {node_type} node for drain operation")
        typed_nodes = [node1.name for node1 in node.get_nodes(node_type=node_type)]
        logger.debug(f"Available {node_type} nodes: {typed_nodes}")

        filter_list = pgsql.filter_pgbench_nodes_from_nodeslist(typed_nodes)
        logger.debug(f"Nodes not running pgbench: {filter_list}")

        typed_node_name = filter_list[random.randint(0, len(filter_list) - 1)]
        logger.info(f"Selected node for drain operation: {typed_node_name}")

        logger.test_step(f"Drain {node_type} node during pgbench execution")
        logger.info(f"Draining node: {typed_node_name}")
        node.drain_nodes([typed_node_name])
        logger.info("Node drain completed")

        logger.info(f"Making node schedulable again: {typed_node_name}")
        node.schedule_nodes([typed_node_name])
        logger.info("Node marked schedulable")

        logger.test_step("Verify cluster and Ceph health after node drain")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")

        logger.test_step("Wait for pgbench to complete")
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)
        logger.info(f"pgbench reached status: {constants.STATUS_COMPLETED}")

        end_time = datetime.now()
        diff_time = end_time - start_time
        logger.info(
            f"pgbench pod reached completed state after {diff_time.seconds} seconds"
        )

        logger.test_step("Validate pgbench results")
        pgbench_pods = pgsql.get_pgbench_pods()
        logger.info(f"Retrieved {len(pgbench_pods)} pgbench pod(s)")

        pgsql.validate_pgbench_run(pgbench_pods)
        logger.info("pgbench results validated successfully")
