import logging
import pytest
import time
from uuid import uuid4

from ocs_ci.framework import config
from ocs_ci.framework.testlib import performance
from ocs_ci.helpers.helpers import get_full_test_logs_path, pod_start_time
from ocs_ci.ocs import constants
import ocs_ci.ocs.exceptions as ex
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
        self.new_index = "pvc_attach_time_fullres"
        self.full_log_path = full_log_path
        # make sure we have connection to the elastic search server
        self.es_connect()


@performance
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2044")
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-2043")
        ),
    ],
)
class TestPodStartTime(PASTest):
    """
    Measure time to start pod with PVC attached
    """

    pvc_size = 5

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestPodStartTime, self).setup()
        self.benchmark_name = "pvc_attach_time"
        self.uuid = uuid4().hex
        self.crd_data = {
            "spec": {
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

        self.pvc_size = 5  # The size of the pv to create

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
        full_results.add_key("index", full_results.new_index)
        return full_results

    @pytest.fixture()
    def pod(self, interface, pod_factory, pvc_factory):
        """
        Prepare pod for the test

        Returns:
            pod obj: Pod instance

        """
        self.interface = interface
        pvc_obj = pvc_factory(interface=interface, size=self.pvc_size)
        pod_obj = pod_factory(pvc=pvc_obj)
        return pod_obj

    def get_time(self):
        """
        Getting the current GMT time in a specific format for the ES report

        Returns:
            str : current date and time in formatted way

        """
        return time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())

    def test_pod_start_time(self, pod):
        """
        Test to log pod start time
        """
        # Getting the test start time
        self.start_time = self.get_time()

        # The actual test
        start_time_dict = pod_start_time(pod)

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc = "RBD"
        elif self.interface == constants.CEPHFILESYSTEM:
            self.sc = "CephFS"
        elif self.interface == constants.CEPHBLOCKPOOL_THICK:
            self.sc = "RBD-Thick"
        self.full_log_path += f"-{self.sc}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        self.full_results = self.init_full_results(
            ResultsAnalyse(self.uuid, self.crd_data, self.full_log_path)
        )

        start_time = start_time_dict["web-server"]
        logging.info(f"pod start time: {start_time} seconds")
        if start_time > 30:
            raise ex.PerformanceException(
                f"pod start time is {start_time}," f"which is greater than 30 seconds"
            )
        self.full_results.add_key("storageclass", self.sc)
        self.full_results.add_key("attach_time", start_time)

        # Getting the test end time
        self.end_time = self.get_time()

        # Add the test time to the ES report
        self.full_results.add_key(
            "test_time", {"start": self.start_time, "end": self.end_time}
        )

        # Write the test results into the ES server
        self.full_results.es_write()

        # write the ES link to the test results in the test log.
        log.info(f"The Result can be found at : {self.full_results.results_link()}")
