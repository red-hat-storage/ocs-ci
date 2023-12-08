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
class TestPgSQLPodRespin(E2ETest):
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
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(replicas=1, transactions=transactions)
        # Start measuring time
        start_time = datetime.now()

        # Wait for pgbench pod to reach running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Check worker node utilization(adm_top)
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        # Respin relevant pod
        if pod_name == "postgres":
            pgsql.respin_pgsql_app_pod()
        else:
            log.info(f"Respin Ceph pod {pod_name}")
            disruption = disruption_helpers.Disruptions()
            disruption.set_resource(resource=f"{pod_name}")
            disruption.delete_resource()

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

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=40)
