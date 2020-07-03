import logging
import pytest
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.ocs import node
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftover_label
)
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.node import get_node_resource_utilization_from_adm_top

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def pgsql(request):

    pgsql = Postgresql()

    def teardown():
        pgsql.cleanup()
    request.addfinalizer(teardown)
    return pgsql


@workloads
@ignore_leftover_label(constants.drain_canary_pod_label)
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
        pgsql.setup_postgresql(replicas=3)

        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_node_drain(self, pgsql):
        """
        Test pgsql workload
        """
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(
            replicas=3, clients=3, transactions=1600
        )

        # Start measuring time
        start_time = datetime.now()

        # Wait for pgbench pod to reach running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Check worker node utilization (adm_top)
        get_node_resource_utilization_from_adm_top(
            node_type='worker', print_table=True
        )

        # Node drain on a Pgsql pod running node (ignore pgbench pod
        # running node)
        pgsql_nodes = pgsql.get_pgsql_nodes()
        pgbench_nodes = pgsql.get_pgbench_nodes()
        node_list = list(set(pgsql_nodes) - set(pgbench_nodes))

        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([node_list[0]])

        # Make the node schedulable again
        node.schedule_nodes([node_list[0]])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

        # Wait for pg_bench pod to complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Calculate the time from running state to completed state
        end_time = datetime.now()
        diff_time = end_time - start_time
        log.info(
            f"pgbench pod reached to completed state after"
            f" {diff_time.seconds} seconds")

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pgsql.validate_pgbench_run(pgbench_pods)
