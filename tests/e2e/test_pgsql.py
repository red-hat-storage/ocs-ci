"""
Module to perform PGSQL workload
"""
import logging
import pytest
import time
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating, utils
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, tier1
from tests import helpers
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def ripsaw(request):
    # Create Secret and Pool
    secret = helpers.create_secret(constants.CEPHBLOCKPOOL)
    pool = helpers.create_ceph_block_pool()

    # Create storage class
    log.info("Creating a Storage Class")
    sc = helpers.create_storage_class(
        sc_name='pgsql-workload',
        interface_type=constants.CEPHBLOCKPOOL,
        secret_name=secret.name,
        interface_name=pool.name
    )
    # Create RipSaw Operator
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
        sc.delete()
        secret.delete()
        pool.delete()
    request.addfinalizer(teardown)
    return ripsaw


@tier1
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
        run_cmd(
            'bin/oc wait --for condition=ready pod '
            '-l app=postgres '
            '--timeout=120s'
        )

        # Create pgbench benchmark
        log.info("Create resource file for pgbench workload")
        pg_data = templating.load_yaml_to_dict(constants.PGSQL_BENCHMARK_YAML)
        pg_obj = OCS(**pg_data)
        pg_obj.create()
        # Wait for pgbench pod to be created
        log.info(
            "waiting for pgbench benchmark to create, "
            f"PGbench pod name: {pg_obj.name} "
        )
        wait_time = 30
        log.info(f"Waiting {wait_time} seconds...")
        time.sleep(wait_time)

        pgbench_pod = run_cmd(
            'bin/oc get pods -l '
            'app=pgbench-client -o name'
        )
        pgbench_pod = pgbench_pod.split('/')[1]
        run_cmd(
            'bin/oc wait --for condition=Initialized '
            f'pods/{pgbench_pod} '
            '--timeout=60s'
        )
        run_cmd(
            'bin/oc wait --for condition=Complete jobs '
            '-l app=pgbench-client '
            '--timeout=300s'
        )

        # Running pgbench and parsing logs
        output = run_cmd(f'bin/oc logs {pgbench_pod}')
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
        log.info("Deleting PG bench benchmark:")
        pg_obj.delete()
