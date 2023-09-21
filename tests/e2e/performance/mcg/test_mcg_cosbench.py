import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import red_squad, mcg
from ocs_ci.framework.testlib import performance, performance_c
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.cosbench import Cosbench

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def cosbench(request):
    cosbench = Cosbench()

    def teardown():
        cosbench.cleanup()

    request.addfinalizer(teardown)
    return cosbench


@red_squad
@mcg
@performance
@performance_c
@pytest.mark.polarion_id("OCS-3694")
class TestMCGCosbench(PASTest):
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
        objects = 10000
        timeout = 3600
        run_samples = 3
        throughput_list = []
        bandwidth_list = []

        # Sizes in KB
        self.sizes = [4, 16, 32, 128]

        # Operations to perform and its ratio(%)
        operations = {"read": 50, "write": 50}

        # Deployment of cosbench
        cosbench.setup_cosbench()

        # Getting the start time of the test
        self.test_start_time = self.get_time()

        for size in self.sizes:
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
                throughput, bandwidth = cosbench.run_main_workload(
                    operation_type=operations,
                    prefix=bucket_prefix,
                    containers=buckets,
                    objects=objects,
                    validate=True,
                    result=True,
                    size=size,
                    timeout=timeout,
                )
                throughput_list.append(throughput)
                bandwidth_list.append(bandwidth)

                # Dispose containers and objects
                cosbench.run_cleanup_workload(
                    prefix=bucket_prefix,
                    containers=buckets,
                    objects=objects,
                    validate=True,
                    timeout=timeout,
                )
        # Getting the end time of the test
        self.test_end_time = self.get_time()

        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file
        full_results = self.init_full_results(
            ResultsAnalyse(self.uuid, self.crd_data, self.full_log_path, "mcg_cosbench")
        )
        # Add the result to ES report
        full_results.add_key(
            "test_time", {"start": self.test_start_time, "end": self.test_end_time}
        )
        full_results.add_key("number_of_bucket", buckets)
        full_results.add_key("number_of_objects", objects)
        full_results.add_key("size_of_file", self.sizes)
        full_results.add_key("throughput", throughput_list)
        full_results.add_key("bandwidth", bandwidth_list)

        self.results_path = get_full_test_logs_path(cname=self)
        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{self.sizes}"

        # Write test results to ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            log.info(f"Results can be found at: {res_link}")

            # Create text file with results
            self.write_result_to_file(res_link)

    def setup(self):
        """
        Setup cosbench test parameters
        """

        log.info("Setting up mcg cosbench performance test")
        super(TestMCGCosbench, self).setup()
        self.benchmark_name = "MCG Cosbench"

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server
        Args:
            full_results (obj): an ResultsAnalyse object
        Returns:
            full_results (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])
        full_results.add_key("index", full_results.new_index)
        full_results.add_key("size_of_file", self.sizes)
        return full_results
