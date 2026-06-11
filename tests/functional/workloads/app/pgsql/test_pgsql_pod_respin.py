import logging
import pytest
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.helpers import disruption_helpers
from ocs_ci.helpers.sanity_helpers import Sanity
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
class TestPgSQLPodRespin(E2ETest):
    """
    Test running PGSQL and with Ceph pods respin
    """

    @pytest.fixture()
    def pgsql_setup(self, pgsql):
        """
        PGSQL test setup
        """
        logger.info(
            "Setting up PostgreSQL environment with pod respin test configuration"
        )
        pgsql.setup_postgresql(replicas=1)
        logger.info("PostgreSQL deployed with 1 replica")

        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["transactions", "pod_name"],
        argvalues=[
            pytest.param(*[600, "mon"], marks=pytest.mark.polarion_id("OCS-802")),
            pytest.param(*[600, "osd"], marks=pytest.mark.polarion_id("OCS-803")),
            pytest.param(*[600, "mgr"], marks=pytest.mark.polarion_id("OCS-804")),
            pytest.param(*[600, "postgres"], marks=pytest.mark.polarion_id("OCS-809")),
        ],
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_respin_pod(self, pgsql, transactions, pod_name):
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

        logger.test_step("Check worker node resource utilization")
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        logger.test_step(f"Respin {pod_name} pod during pgbench execution")
        if pod_name == "postgres":
            logger.info("Respinning PostgreSQL application pod")
            pgsql.respin_pgsql_app_pod()
            logger.info("PostgreSQL pod respun successfully")
        else:
            logger.info(f"Respinning Ceph {pod_name} pod")
            disruption = disruption_helpers.Disruptions()
            disruption.set_resource(resource=f"{pod_name}")
            disruption.delete_resource()
            logger.info(f"Ceph {pod_name} pod respun successfully")

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
