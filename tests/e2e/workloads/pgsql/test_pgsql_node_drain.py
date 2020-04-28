import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs import node
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, google_api_required, workloads, ignore_leftovers
)
from ocs_ci.ocs.pgsql import Postgresql


log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def pgsql(request):

    pgsql = Postgresql()

    def teardown():
        pgsql.cleanup()
    request.addfinalizer(teardown)
    return pgsql


@ignore_leftovers
@workloads
@google_api_required
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
    def test_run_pgsql_node_drain(self, transactions=900, node_type='master'):
        """
        Test pgsql workload
        """
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(replicas=4, transactions=transactions)

        # Wait for pgbench pod to reach running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Node drain with specific node type
        typed_nodes = node.get_typed_nodes(node_type=node_type, num_of_nodes=1)
        typed_node_name = typed_nodes[0].name

        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([typed_node_name])

        # Make the node schedulable again
        node.schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

        # Wait for pg_bench pod to complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pg_out = pgsql.validate_pgbench_run(pgbench_pods)

        # Collect data and export to Google doc spreadsheet
        pgsql.collect_data_to_googlesheet(
            pg_out, sheet_name="OCS PGSQL", sheet_index=3
        )
