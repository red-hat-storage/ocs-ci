import logging
import pytest

from ocs_ci.ocs import constants
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
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
@pytest.mark.polarion_id("OCS-807")
class TestPgSQLWorkload(E2ETest):
    """
    Deploy an PGSQL workload using operator
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
    def test_sql_workload_simple(self, pgsql):
        """
        This is a basic pgsql workload
        """
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(
            replicas=3, transactions=600, clients=3
        )

        # Wait for pgbench pod to reach COMPLETED state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pgsql.validate_pgbench_run(pgbench_pods)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
