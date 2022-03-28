import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, performance
from ocs_ci.ocs.cosbench import Cosbench

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def cosbench(request):

    cosbench = Cosbench()

    def teardown():
        cosbench.cosbench_teardown()

    request.addfinalizer(teardown)
    return cosbench


@performance
@pytest.mark.polarion_id("OCS-3694")
class TestMCGCosbench(E2ETest):
    """
    Test cosbench workloads for MCG
    """

    def test_mcg_cosbench_performance(self, cosbench):
        """
        This test to perform reads and write objects to a bucket with multiple of samples and sizes.
        The operation will be defined with number of % read and write.
        After running main workload, performance numbers will be collected and saved to a spreadsheet for
        performance analysing.
        """

        bucket_prefix = "bucket-"
        buckets = 1
        objects = 100
        timeout = 3600
        run_samples = 3

        # Sizes in KB
        sizes = [4, 16, 32, 128]

        # Operations to perform and its ratio(%)
        operations = {"read": 50, "write": 50}

        # Deployment of cosbench
        cosbench.setup_cosbench()

        for size in sizes:
            for i in range(run_samples):
                # Create initial containers and objects
                cosbench.run_init_workload(
                    prefix=bucket_prefix,
                    containers=buckets,
                    objects=objects,
                    validate=True,
                    size=size,
                    timeout=timeout,
                )

                # Run main workload
                cosbench.run_main_workload(
                    operation_type=operations,
                    prefix=bucket_prefix,
                    containers=buckets,
                    objects=objects,
                    validate=True,
                    result=True,
                    size=size,
                    timeout=timeout,
                )

                # Dispose containers and objects
                cosbench.run_cleanup_workload(
                    prefix=bucket_prefix,
                    containers=buckets,
                    objects=objects,
                    validate=True,
                    timeout=timeout,
                )

    # TODO add performance data to performance portal
