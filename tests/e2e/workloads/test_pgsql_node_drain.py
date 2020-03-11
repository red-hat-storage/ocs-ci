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
from ocs_ci.ocs import node
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, google_api_required, workloads, ignore_leftovers
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI

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
@pytest.mark.polarion_id("OCS-806")
class TestPgSQLNodeReboot(E2ETest):
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
            'resources/crds/'
            'ripsaw_v1alpha1_ripsaw_crd.yaml'
        )
        ripsaw.setup_postgresql()

        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_node_drain(self, transactions=900, node_type='master'):
        """
        Test pgsql workload
        """
        # Create pgbench benchmark
        log.info("Create resource file for pgbench workload")
        pg_trans = transactions
        timeout = pg_trans * 3
        pg_data = templating.load_yaml(constants.PGSQL_BENCHMARK_YAML)
        pg_data['spec']['workload']['args']['transactions'] = pg_trans
        pg_obj = OCS(**pg_data)
        pg_obj.create()

        # Getting pgbench podname
        for pgbench_pod in TimeoutSampler(
            pg_trans, 3, get_pod_name_by_pattern,
            'pgbench', 'my-ripsaw'
        ):
            try:
                if pgbench_pod[0] is not None:
                    pgbench_client_pod = pgbench_pod[0]
                    break
            except IndexError:
                log.info("Bench pod is not found")

        # Wait for pg_bench pod to be in running state
        log.info("Waiting for pgbench_pod to be in running state")
        pod_obj = OCP(kind='pod')
        pod_obj.wait_for_resource(
            condition='Running',
            resource_name=pgbench_client_pod,
            timeout=timeout,
            sleep=5,
        )

        # Node drain with specific node type
        typed_nodes = node.get_typed_nodes(node_type=node_type, num_of_nodes=1)
        typed_node_name = typed_nodes[0].name

        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([typed_node_name])

        # Make the node schedulable again
        node.schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

        # Wait for pg_bench pod to complete workload
        log.info("Waiting for pgbench_client to complete")
        pod_obj.wait_for_resource(
            condition='Completed',
            resource_name=pgbench_client_pod,
            timeout=timeout,
            sleep=10,
        )

        # Parsing the results
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
                    "PGBench failed to run, no data found on latency_avg"
                )
        log.info("PGBench has completed successfully")

        # Collect data and export to Google doc spreadsheet
        g_sheet = GoogleSpreadSheetAPI(sheet_name="OCS PGSQL", sheet_index=3)
        for lat in pg_output:
            lat_avg = lat['latency_avg']
            lat_stddev = lat['lat_stddev']
            tps_incl = lat['tps_incl']
            tps_excl = lat['tps_excl']
            g_sheet.insert_row(
                [int(lat_avg),
                 int(lat_stddev),
                 int(tps_incl),
                 int(tps_excl)], 2
            )
        # Clean up pgbench benchmark
        log.info("Deleting PG bench benchmark")
        pg_obj.delete()
