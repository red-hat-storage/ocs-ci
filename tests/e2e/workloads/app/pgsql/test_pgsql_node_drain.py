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

log = logging.getLogger(__name__)


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
        # Deployment of postgres database
        pgsql.setup_postgresql(replicas=1)

        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_node_drain(self, pgsql, node_type="worker"):
        """
        Test pgsql workload
        """
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(replicas=1, transactions=600)

        # Start measuring time
        start_time = datetime.now()

        # Wait for pgbench pod to reach running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Check worker node utilization (adm_top)
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        # Select a node where pgbench is not running for drain
        typed_nodes = [node1.name for node1 in node.get_nodes(node_type=node_type)]
        filter_list = pgsql.filter_pgbench_nodes_from_nodeslist(typed_nodes)
        typed_node_name = filter_list[random.randint(0, len(filter_list) - 1)]
        log.info(f"Selected node {typed_node_name} for node drain operation")

        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([typed_node_name])

        # Make the node schedulable again
        node.schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=40)

        # Wait for pg_bench pod to complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Calculate the time from running state to completed state
        end_time = datetime.now()
        diff_time = end_time - start_time
        log.info(
            f"\npgbench pod reached to completed state after {diff_time.seconds} seconds\n"
        )

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pgsql.validate_pgbench_run(pgbench_pods)
