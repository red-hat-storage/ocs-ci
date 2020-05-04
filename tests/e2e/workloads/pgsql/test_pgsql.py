import logging
import pytest
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.pgsql import Postgresql

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def pgsql(request):

    pgsql = Postgresql()

    def teardown():
        pgsql.cleanup()
    request.addfinalizer(teardown)
    return pgsql


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
        pgsql.setup_postgresql(replicas=3)

        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(replicas=3, clients=3)

        # Wait for pg_bench pod to initialized and complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pgsql.validate_pgbench_run(pgbench_pods)
