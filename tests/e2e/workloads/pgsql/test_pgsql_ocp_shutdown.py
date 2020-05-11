import logging
import pytest
import time

from ocs_ci.ocs import constants
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
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
@pytest.mark.polarion_id("OCS-820")
class TestPgSQLNodeShut(E2ETest):
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
                *[600, 'postgres'], marks=pytest.mark.polarion_id("OCS-818")
            )
        ]
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_reboot_ocp(
        self, pgsql, nodes, transactions, pod_name
    ):
        """
        Test pgsql workload
        """
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(
            replicas=3, transactions=transactions, clients=3
        )

        # Wait for pgbench pod to reach running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Get all the nodes that contain postgres pods
        node_list = pgsql.get_nodes(pod_name=pod_name, all_nodes=True)

        # Stop relevant nodes
        nodes.stop_nodes(node_list)

        # Sleep 2 min
        time.sleep(120)

        # Start relevant nodes
        nodes.start_nodes(node_list)

        # Check that postgresql pods in running state
        pgsql.get_postgresql_status()

        # Check that pgbench pods in running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
