"""
Module to perform PGSQL workload
"""
import pytest
import logging
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.framework.testlib import (
    E2ETest, google_api_required, workloads, ignore_leftovers
)
from ocs_ci.utility.workloads.helpers import PgsqlE2E
log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def ripsaw(request):

    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)
    return ripsaw


@ignore_leftovers
@workloads
@google_api_required
class TestPgSQLAppPodRespin(E2ETest):
    """
    Test running PGSQL and with Ceph pods respin
    """
    @pytest.fixture()
    def pgsql_setup(self, ripsaw):
        """
        PGSQL test setup
        """
        # Deployment ripsaw and postgres database
        log.info("Deploying postgres database")
        ripsaw.apply_crd(
            'resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml'
        )
        ripsaw.setup_postgresql()

    @pytest.mark.parametrize(
        argnames=[
            "transactions", "pod_name", "name_space"
        ],
        argvalues=[
            pytest.param(
                *[600, 'mon', 'openshift-storage'], marks=pytest.mark.polarion_id("OCS-802")
            ),
            pytest.param(
                *[600, 'osd', 'openshift-storage'], marks=pytest.mark.polarion_id("OCS-803")
            ),
            pytest.param(
                *[600, 'mgr', 'openshift-storage'], marks=pytest.mark.polarion_id("OCS-804")
            ),
            pytest.param(
                *[600, 'postgres', 'my-ripsaw'], marks=pytest.mark.polarion_id("OCS-809")
            )
        ]
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_reset_app_pod_pgsql(self, transactions, pod_name, name_space):
        """
        1.Create new-project "my-ripsaw"
        2.Adding Service with name postgres
        3.Adding ConfigMap with name postgres-config
        4.Adding StatefulSet with name postgres
        5.Waiting app pod "postgres" reach Running mode
        6.Adding Benchmark with name pgbench-benchmark
        7.Wait Bench pod ready
        8.Delete Pod postgres
        9.Waiting app pod "postgres" reach Running mode Running
        10.Waiting for pgbench_client to complete:
        11.Delete pgsql components
        """
        pod_test = PgsqlE2E(transactions, pod_name, name_space)
        # Create resource file for pgbench workload and wait for pgbench pod to be created
        pod_test.create_benchmark()
        # Delete relevant pod and wait for pgbench_client to complete
        pod_test.reset_pod()
        # Running pgbench and parsing logs
        pod_test.run_pgbench()
        # Clean up pgbench benchmark
        pod_test.delete_pgbench()
        # Collect data and export to Google doc spreadsheet
        pod_test.collect_log_to_google()
