import logging
import pytest
import random

from ocs_ci.ocs import constants
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
)
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.node import get_osd_running_nodes, get_node_objs
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
class TestPgSQLNodeReboot(E2ETest):
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

        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=[
            "transactions", "pod_name"
        ],
        argvalues=[
            pytest.param(
                *[600, 'osd'], marks=pytest.mark.polarion_id("OCS-801")
            ),
            pytest.param(
                *[600, 'postgres'], marks=pytest.mark.polarion_id("OCS-799")
            )
        ]
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_reboot_node(
        self, pgsql, nodes, transactions, pod_name
    ):
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

        # Choose a node based on pod it contains
        if pod_name == 'postgres':
            node_list = pgsql.get_pgsql_nodes()
        elif pod_name == 'osd':
            node_list = get_osd_running_nodes()
        node_1 = get_node_objs(node_list[random.randint(0, len(node_list) - 1)])

        # Check node utilization
        pgsql.get_node_utilization()

        # Restart relevant node
        nodes.restart_nodes(node_1)

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

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
