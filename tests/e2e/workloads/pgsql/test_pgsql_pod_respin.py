import logging
import pytest

from ocs_ci.ocs import constants
from tests import disruption_helpers
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
class TestPgSQLCephPodRespin(E2ETest):
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
        ]
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql(self, transactions, pod_name):
        """
        Test pgsql workload
        """
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(replicas=3, transactions=transactions)

        # Wait for pgbench pod to reach running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Respin Ceph pod
        resource_osd = [f'{pod_name}']
        log.info(f"Respin Ceph pod {pod_name}")
        disruption = disruption_helpers.Disruptions()
        for resource in resource_osd:
            disruption.set_resource(resource=resource)
            disruption.delete_resource()

        # Wait for pg_bench pod to complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pg_out = pgsql.validate_pgbench_run(pgbench_pods)

        # Collect data and export to Google doc spreadsheet
        pgsql.collect_data_to_googlesheet(
            pg_out, sheet_name="OCS PGSQL", sheet_index=2
        )
