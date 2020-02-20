"""
Module to perform PGSQL workload
"""
import logging
import pytest
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating, utils
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def ripsaw(request):

    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
    request.addfinalizer(teardown)
    return ripsaw


@workloads
class TestPgSQLWorkload(E2ETest):
    """
    Deploy an PGSQL workload using operator
    """
    def test_sql_workload_simple(self, ripsaw):
        """
        This is a basic pgsql workload
        """
        # Deployment postgres
        log.info("Deploying postgres database")
        ripsaw.apply_crd(
            'resources/crds/'
            'ripsaw_v1alpha1_ripsaw_crd.yaml'
        )
        ripsaw.setup_postgresql()

        # Create pgbench benchmark
        log.info("Create resource file for pgbench workload")
        pg_data = templating.load_yaml(constants.PGSQL_BENCHMARK_YAML)
        pg_obj = OCS(**pg_data)
        pg_obj.create()

        # Wait for pgbench pod to be created
        for pgbench_pod in TimeoutSampler(
            300, 3, get_pod_name_by_pattern, 'pgbench-1-dbs-client', 'my-ripsaw'
        ):
            try:
                if pgbench_pod[0] is not None:
                    pgbench_client_pod = pgbench_pod[0]
                    break
            except IndexError:
                log.info("Bench pod not ready yet")

        # Wait for pg_bench pod to initialized and complete
        log.info("Waiting for pgbench_client to complete")
        pod_obj = OCP(kind='pod')
        pod_obj.wait_for_resource(
            condition='Completed',
            resource_name=pgbench_client_pod,
            timeout=800,
            sleep=10,
        )

        # Running pgbench and parsing logs
        output = run_cmd(f'oc logs {pgbench_client_pod}')
        pg_output = utils.parse_pgsql_logs(output)
        log.info(
            "*******PGBench output log*********\n"
            f"{pg_output}"
        )
        for data in pg_output:
            latency_avg = data['latency_avg']
            if not latency_avg:
                raise UnexpectedBehaviour(
                    "PGBench failed to run, "
                    "no data found on latency_avg"
                )
        log.info("PGBench has completed successfully")

        # Clean up pgbench benchmark
        log.info("Deleting PG bench benchmark")
        pg_obj.delete()
