import logging
import pytest
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
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
        logger.test_step("Deploy PostgreSQL database")
        pgsql.setup_postgresql(replicas=1)
        logger.info("PostgreSQL deployed with 1 replica")

        logger.test_step("Create pgbench benchmark: 1 replica, 600 transactions")
        pgsql.create_pgbench_benchmark(replicas=1, transactions=600)
        logger.info("pgbench benchmark created")

        start_time = datetime.now()
        logger.info(f"Benchmark start time: {start_time}")

        logger.test_step("Check worker node resource utilization")
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        logger.test_step("Wait for pgbench benchmark to complete")
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
