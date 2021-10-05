"""
Test to <full description of this test file>
"""
# Builtin modules
import logging
import time
from uuid import uuid4

# 3rd party modules
import pytest

# Local modules
from ocs_ci.framework import config
from ocs_ci.framework.testlib import performance
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.perftests import PASTest


log = logging.getLogger(__name__)


class ResultsAnalyse(PerfResult):
    """
    This class generates results for all tests as one unit
    and saves them to an elastic search server on the cluster

    """

    def __init__(self, uuid, crd, full_log_path):
        """
        Initialize the object by reading some of the data from the CRD file and
        by connecting to the ES server and read all results from it.

        Args:
            uuid (str): the unique uid of the test
            crd (dict): dictionary with test parameters - the test yaml file
                        that modify it in the test itself.
            full_log_path (str): the path of the results files to be found

        """
        super(ResultsAnalyse, self).__init__(uuid, crd)

        # The self.new_index parameter should be unique in the OCS-CI and in the ES
        self.new_index = "testname_fullres"

        self.full_log_path = full_log_path

        # make sure we have connection to the elastic search server
        self.es_connect()

    def analyze_the_results(self, test_results):
        """
        Analyzing the test results

        Args:
            test_results (type) : description of argument

        Return:
            type : description of return value
        """

        # in this place do the in-test results analyze.
        # this function is not mandatory.

    def results_logging(self):
        """
        Logging relevant results to the test log

        """
        log.info("Test Results Summery:")

        # Log here the summer of the test results


@performance
class TestClassName(PASTest):
    """
    Test to ...

    """

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestClassName, self).setup()

        # Benchmark name need to be unique in OCS-CI
        self.benchmark_name = "<name>"
        self.uuid = uuid4().hex
        self.crd_data = {
            "spec": {
                # values of test_user and clustername are only 'Place holders'
                "test_user": "Homer simpson",
                "clustername": "test_cluster",
                "elasticsearch": {
                    "server": config.PERF.get("production_es_server"),
                    "port": config.PERF.get("production_es_port"),
                    "url": f"http://{config.PERF.get('production_es_server')}:{config.PERF.get('production_es_port')}",
                },
            }
        }
        # during development use the dev ES so the data in the Production ES will be clean.
        if self.dev_mode:
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("dev_es_server"),
                "port": config.PERF.get("dev_es_port"),
                "url": f"http://{config.PERF.get('dev_es_server')}:{config.PERF.get('dev_es_port')}",
            }

        # Any other test setup need to be here

    def teardown(self):
        """
        Teardown of the test

        """

        # teardown operation need to be here
        log.info("Cleanup the environment")

    def run(self):
        """
        The actual test function

        """

        # Test procedure need to be here

        # collect all results into internal object parameter, if the test run
        # more then once (preferred), collect them into a list(s).

    def get_time(self):
        """
        Getting the current GMT time in a specific format for the ES report

        Returns:
            str : current date and time in formatted way

        """
        return time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an empty FIOResultsAnalyse object

        Returns:
            FIOResultsAnalyse (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])

        # full_results.new_index is the index name in the ES
        full_results.add_key("index", full_results.new_index)

        # if you need, add here all other data you want to have in the report.
        return full_results

    # This is not mandatory, but if you want to parametrize the test, this is the way
    @pytest.mark.parametrize(
        argnames=["Arg1", "ArgN"],
        argvalues=[
            pytest.param(
                *["Parm1", "ParamN"],
                marks=[pytest.mark.performance, pytest.mark.polarion_id("Polarion ID")],
            ),
        ],
    )
    def function_test_name(self, Arg1, ArgN):
        """
        Description of the test purpose

        Args:
            Arg1 (type) : description of argument
            ...
            ArgN (type): description of argument
        """

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        # Full log contain the test arguments in the format :
        # self.full_log_path-{Arg1}-...-{ArgN}
        log.info(f"Logs file path name is : {self.full_log_path}")

        # Getting the test start time
        self.start_time = self.get_time()

        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        self.full_results = self.init_full_results(
            ResultsAnalyse(self.uuid, self.crd_data, self.full_log_path)
        )

        # in this section add all test parameters that you need in the ES results
        self.full_results.add_key("key_name", "key_value")

        # The actual test need to be here
        self.run()

        # Analyze the test results (if needed)
        self.full_results.analyze_the_results(self.test_results)

        # Push all test results into the full results object
        self.full_results.add_key("test_key", self.test_result_value)

        # Getting the test end time
        self.end_time = self.get_time()

        # Add the test time to the ES report
        self.full_results.add_key(
            "test_time", {"start": self.start_time, "end": self.end_time}
        )

        # Logging the results
        self.full_results.results_logging()

        # Write the test results into the ES server
        if self.full_results.es_write():
            # write the ES link to the test results in the test log.
            log.info(f"The Result can be found at : {self.full_results.results_link()}")
