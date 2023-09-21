import logging
import pytest
from ocs_ci.ocs import constants
from ocs_ci.ocs import scale_pgsql
from ocs_ci.utility import utils
from ocs_ci.framework.pytest_customization.marks import orange_squad
from ocs_ci.framework.testlib import E2ETest, scale, ignore_leftovers
from ocs_ci.ocs.node import get_node_resource_utilization_from_adm_top

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def pgsql(request):

    pgsql = scale_pgsql.ScalePodPGSQL()

    def teardown():
        pgsql.cleanup()

    request.addfinalizer(teardown)
    return pgsql


@orange_squad
@scale
@ignore_leftovers
@pytest.mark.polarion_id("OCS-2239")
@pytest.mark.skip(
    reason="Skipped because of https://github.com/red-hat-storage/ocs-ci/issues/3983"
)
class TestPgsqlPodScale(E2ETest):
    """
    Scale test case using PGSQL Pods
    """

    def test_scale_pgsql(self, pgsql):
        """
        Test case to scale pgsql pods:
          * Add worker nodes to existing cluster
          * Label new worker node
          * Create pgsql to run on 200 pods on new added worker node
        """
        replicas = 200  # Number of postgres and pgbench pods to be deployed
        timeout = (
            replicas * 100
        )  # Time in seconds to wait for pgbench pods to be created

        # Add workers node to cluster
        scale_pgsql.add_worker_node()

        # Check ceph health status
        utils.ceph_health_check()

        # Deployment postgres
        pgsql.setup_postgresql(
            replicas=replicas, node_selector=constants.SCALE_NODE_SELECTOR
        )

        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(
            replicas=replicas, clients=5, transactions=60, timeout=timeout
        )

        # Check worker node utilization (adm_top)
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        # Wait for pg_bench pod to initialized and complete
        pgsql.wait_for_pgbench_status(
            status=constants.STATUS_COMPLETED, timeout=timeout
        )

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and parse logs
        pgsql.validate_pgbench_run(pgbench_pods)

        # Check ceph health status
        utils.ceph_health_check()
