import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
)
from ocs_ci.ocs.pgsql import Postgresql
from tests import disruption_helpers
from datetime import datetime

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
        pgsql.setup_postgresql(replicas=3)

    @pytest.mark.parametrize(
        argnames=[
            "transactions", "pod_name"
        ],
        argvalues=[
            pytest.param(
                *[600, 'mon'], marks=pytest.mark.polarion_id("OCS-802")
            ),
            pytest.param(
                *[600, 'osd'], marks=pytest.mark.polarion_id("OCS-803")
            ),
            pytest.param(
                *[600, 'mgr'], marks=pytest.mark.polarion_id("OCS-804")
            ),
            pytest.param(
                *[600, 'postgers'], marks=pytest.mark.polarion_id("OCS-809")
            )
        ]
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_respin_pod(self, pgsql, transactions, pod_name):
        """
        Test pgsql workload
        """
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(
            replicas=3, transactions=transactions, clients=3
        )
        # Start measuring time
        start_time = datetime.now()

        # Wait for pgbench pod to reach running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Check node utilization
        pgsql.get_node_utilization()

        # Respin pod
        if pod_name == 'postgers':
            pgsql.respin_pgsql_app_pod()
        else:
            log.info(f"Respin Ceph pod {pod_name}")
            disruption = disruption_helpers.Disruptions()
            disruption.set_resource(resource=f'{pod_name}')
            disruption.delete_resource()

        # Wait for pg_bench pod to complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Calculate the time from running state to completed state
        end_time = datetime.now()
        diff_time = end_time - start_time
        log.info(f"\npgbench pod reached to completed state after {diff_time.seconds} seconds\n")

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pgsql.validate_pgbench_run(pgbench_pods)
