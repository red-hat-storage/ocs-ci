import logging
import pytest
import random
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_node_objs,
    get_node_resource_utilization_from_adm_top,
)

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def pgsql(request):

    pgsql = Postgresql()

    def teardown():
        pgsql.cleanup()

    request.addfinalizer(teardown)
    return pgsql


@magenta_squad
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
        pgsql.setup_postgresql(replicas=1)

        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["transactions", "pod_name"],
        argvalues=[
            pytest.param(*[600, "osd"], marks=pytest.mark.polarion_id("OCS-801"))
        ],
    )
    @pytest.mark.usefixtures(pgsql_setup.__name__)
    def test_run_pgsql_reboot_node(self, pgsql, nodes, transactions, pod_name):
        """
        Test pgsql workload
        """
        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(replicas=1, transactions=transactions)

        # Start measuring time
        start_time = datetime.now()

        # Wait for pgbench pod to reach running state
        pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

        # Select a node where pgbench is not running and reboot
        osd_nodes_list = get_osd_running_nodes()
        node_list = pgsql.filter_pgbench_nodes_from_nodeslist(osd_nodes_list)

        node_1 = get_node_objs(node_list[random.randint(0, len(node_list) - 1)])
        log.info(f"Selected node {node_1} for reboot operation")

        # Check worker node utilization (adm_top)
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        # Restart relevant node
        nodes.restart_nodes(node_1)

        # Wait for pg_bench pod to complete
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Calculate the time from running state to completed state
        end_time = datetime.now()
        diff_time = end_time - start_time
        log.info(
            f"\npgbench pod reached to completed state after {diff_time.seconds} seconds\n"
        )

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pgsql.validate_pgbench_run(pgbench_pods)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=40)
