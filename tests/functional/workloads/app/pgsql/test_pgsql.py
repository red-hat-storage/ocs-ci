import logging
import pytest
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, google_api_required
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
@google_api_required
@workloads
@pytest.mark.polarion_id("OCS-807")
class TestPgSQLWorkload(E2ETest):
    """
    Deploy an PGSQL workload using operator
    """

    def test_sql_workload_simple(self, pgsql):
        """
        This is a basic pgsql workload
        """
        # Deployment postgres
        pgsql.setup_postgresql(replicas=1)

        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(replicas=1, transactions=600)

        # Start measuring time
        start_time = datetime.now()

        # Check worker node utilization (adm_top)
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        # Wait for pg_bench pod to initialized and complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Calculate the time from running state to completed state
        end_time = datetime.now()
        diff_time = end_time - start_time
        log.info(
            f"\npgbench pod reached to completed state after "
            f"{diff_time.seconds} seconds\n"
        )

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pg_out = pgsql.validate_pgbench_run(pgbench_pods)

        # Export pgdata to google  google spreadsheet
        pgsql.export_pgoutput_to_googlesheet(
            pg_output=pg_out, sheet_name="E2E Workloads", sheet_index=0
        )
