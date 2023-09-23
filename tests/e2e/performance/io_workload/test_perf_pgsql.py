"""
Test to run PGSQL performance marker workload
"""
import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.perf_pgsql import PerfPGSQL
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import performance, performance_c
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.node import get_node_resource_utilization_from_adm_top


log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def pgsql(request):

    pgsql = PerfPGSQL()

    def teardown():
        pgsql.cleanup()

    request.addfinalizer(teardown)
    return pgsql


@grey_squad
@performance
@performance_c
@pytest.mark.polarion_id("OCS-2725")
class TestPGSQLPodPerf(PASTest):
    """
    PGSQL Performance benchmark using pgbench,
    benchmark operator

    """

    def test_pgsqlperf_workload(self, pgsql):
        """
        Testcase to setup postgres database pod and run
        pgbench benchmark to measure the performance marker

        """
        # Deployment of postgres pod
        pgsql.setup_postgresql(replicas=1)

        # Create pgbench benchmark
        pgsql.create_pgbench_benchmark(replicas=1, transactions=100)

        # Check worker node utilization
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        # Wait for pg_bench pod to be initialized and completed
        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

        # Get pgbench pods
        pgbench_pods = pgsql.get_pgbench_pods()

        # Validate pgbench run and measure the TPS, Latency
        pgsql.validate_pgbench_perf(pgbench_pods)
