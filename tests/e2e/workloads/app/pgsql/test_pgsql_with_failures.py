import logging
import pytest

from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import node

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def pgsql(request):
    pgsql = Postgresql()

    def teardown():
        pgsql.cleanup()

    request.addfinalizer(teardown)
    return pgsql


@workloads
class TestPgSQLWorkload(E2ETest):
    """
    Deploy an PGSQL workload using operator
    """

    total_rows = 100
    run_time = 10
    num_of_ops = 10
    table_name = "testing1"

    def test_sql_workload_simple(self, pgsql, nodes):
        """
        This is a pgsql workload that runs PGSQL without pgbench, along with node drain and pod respin.
        """
        # Deployment postgres
        pgsql.setup_postgresql(replicas=1)

        # Wait for postgres pod to reach Running status
        pgsql.wait_for_postgres_status()
        postgres_pod = pgsql.get_postgres_pods()[0]

        # Run pgsql workload
        last_valid_state = pgsql.run_pgsql_workload(
            postgres_pod,
            self.table_name,
            self.total_rows,
            self.num_of_ops,
            self.run_time,
            False,
        )

        # Select a node where postgres pod is running for drain
        postgres_node = pod.get_pod_node(postgres_pod)

        # Node Drain
        node.drain_nodes([postgres_node.name])
        # Make the node schedulable again
        node.schedule_nodes([postgres_node.name])
        # Wait for postgres pod to reach Running status
        pgsql.wait_for_postgres_status()

        # Verify Data Integrity after Node Drain
        state_after_node_drain = pgsql.run_pgsql_command(
            postgres_pod, f"SELECT * FROM {self.table_name};", True
        )
        assert state_after_node_drain == last_valid_state, "Data corruption found"
        log.info("Data exists after node drain.")

        # POD Respin
        pgsql.respin_pgsql_app_pod()
        # Wait for postgres pod to reach Running status
        pgsql.wait_for_postgres_status()

        # Verify Data Integrity after POD Respin
        state_after_pod_respin = pgsql.run_pgsql_command(
            postgres_pod, f"SELECT * FROM {self.table_name};", True
        )
        assert state_after_pod_respin == last_valid_state, "Data corruption found"
        log.info("Data exists after pod respin.")

        pgsql.run_pgsql_command(postgres_pod, f"DROP TABLE {self.table_name};")
