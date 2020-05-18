import logging
import pytest
import time

from ocs_ci.ocs import constants
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, workloads
)
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.node import get_node_objs
from tests.helpers import get_worker_nodes

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def pgsql(request):

    pgsql = Postgresql()

    def teardown():
        pgsql.cleanup()
    request.addfinalizer(teardown)
    return pgsql


@workloads
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
            "transactions"
        ],
        argvalues=[
            pytest.param(
                *[600], marks=pytest.mark.polarion_id("OCS-818")
            )
        ]
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_shutdown_nodes(
        self, pgsql, nodes, transactions
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

        # Get all the worker nodes
        node_list = get_worker_nodes()
        node_list_objs = get_node_objs(node_list)

        # Stop worker nodes
        nodes.stop_nodes(node_list_objs)

        # Sleep 5 min
        time.sleep(300)

        # Start worker nodes
        nodes.start_nodes(node_list_objs)

        # Check that postgresql pods in running state
        pgsql.wait_for_postgres_status(status=constants.STATUS_RUNNING, timeout=600)

        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(
            replicas=3, transactions=transactions, clients=3
        )

        # Wait for pg_bench pod to complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
