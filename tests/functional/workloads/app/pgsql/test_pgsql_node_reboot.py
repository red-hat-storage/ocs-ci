import logging
import pytest
import random
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_node_objs,
    get_node_resource_utilization_from_adm_top,
)

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
            "Setting up PostgreSQL environment with node reboot test configuration"
        )
        pgsql.setup_postgresql(replicas=1)
        logger.info("PostgreSQL deployed with 1 replica")

        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["transactions", "pod_name"],
        argvalues=[
            pytest.param(*[600, "osd"], marks=pytest.mark.polarion_id("OCS-801"))
        ],
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_reboot_node(self, pgsql, nodes, transactions, pod_name):
        """
        Test pgsql workload
        """
        logger.test_step(
            f"Create pgbench benchmark: 1 replica, {transactions} transactions"
        )
        pgsql.create_pgbench_benchmark(replicas=1, transactions=transactions)
        logger.info("pgbench benchmark created")

        start_time = datetime.now()
        logger.info(f"Benchmark start time: {start_time}")

        logger.test_step("Wait for pgbench to reach running state")
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)
        logger.info(f"pgbench reached status: {constants.STATUS_RUNNING}")

        logger.test_step("Identify OSD node for reboot operation")
        osd_nodes_list = get_osd_running_nodes()
        logger.debug(f"OSD nodes: {osd_nodes_list}")

        node_list = pgsql.filter_pgbench_nodes_from_nodeslist(osd_nodes_list)
        logger.debug(f"OSD nodes not running pgbench: {node_list}")

        node_1 = get_node_objs(node_list[random.randint(0, len(node_list) - 1)])
        logger.info(f"Selected node for reboot operation: {[n.name for n in node_1]}")

        logger.test_step("Check worker node resource utilization")
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        logger.test_step("Reboot OSD node during pgbench execution")
        logger.info(f"Rebooting node: {[n.name for n in node_1]}")
        nodes.restart_nodes(node_1)
        logger.info("Node reboot completed")

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

        logger.test_step("Verify cluster and Ceph health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")
