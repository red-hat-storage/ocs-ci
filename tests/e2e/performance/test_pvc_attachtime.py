import logging
import pytest
import statistics
from uuid import uuid4

from ocs_ci.framework import config
from ocs_ci.framework.testlib import performance
from ocs_ci.helpers.helpers import get_full_test_logs_path, pod_start_time
from ocs_ci.helpers import helpers
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
    argnames=["interface", "samples_num", "pvc_size"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, 5, 5], marks=pytest.mark.polarion_id("OCS-2044")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, 5, 5], marks=pytest.mark.polarion_id("OCS-2043")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL_THICK, 5, 5],
            marks=pytest.mark.polarion_id("OCS-2630"),
        ),
    ],
)
class TestPodStartTime(PASTest):
    """
    Measure time to start pod with PVC attached
    """

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
    def pod_obj_list(
        self,
        interface,
        storageclass_factory,
        pod_factory,
        pvc_factory,
        samples_num,
        pvc_size,
    ):
        """
        Prepare sample pods for the test

        Returns:
            pod obj: List of pod instances

        """
        self.interface = interface
        pod_result_list = []

        self.msg_prefix = f"Interface: {self.interface}, PVC size: {pvc_size}."

        if self.interface == constants.CEPHBLOCKPOOL_THICK:
            self.sc_obj = storageclass_factory(
                interface=constants.CEPHBLOCKPOOL,
                new_rbd_pool=True,
                rbd_thick_provision=True,
            )
        else:
            self.sc_obj = storageclass_factory(self.interface)

        for i in range(samples_num):
            logging.info(f"{self.msg_prefix} Start creating PVC number {i + 1}.")
            pvc_obj = helpers.create_pvc(sc_name=self.sc_obj.name, size=pvc_size)
            timeout = 600 if self.interface == constants.CEPHBLOCKPOOL_THICK else 60
            helpers.wait_for_resource_state(
                pvc_obj, constants.STATUS_BOUND, timeout=timeout
            )
            pvc_obj.reload()

            logging.info(
                f"{self.msg_prefix} PVC number {i + 1} was successfully created ."
            )

            pod_obj = pod_factory(
                interface=self.interface, pvc=pvc_obj, status=constants.STATUS_RUNNING
            )

            pod_result_list.append(pod_obj)

        return pod_result_list

    def test_pod_start_time(self, pod_obj_list):
        """
        Test to log pod start times for all the sampled pods
        """
        # Getting the test start time
        self.test_start_time = PASTest.get_time()

        # Start of the actual test
        start_time_dict_list = []
        for pod in pod_obj_list:
            start_time_dict_list.append(pod_start_time(pod))

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
        self.full_results.add_key("storageclass", self.sc)

        time_measures = [t["web-server"] for t in start_time_dict_list]
        for index, start_time in enumerate(time_measures):
            logging.info(
                f"{self.msg_prefix} pod number {index} start time: {start_time} seconds"
            )
            if start_time > 30:
                raise ex.PerformanceException(
                    f"{self.msg_prefix} Pod number {index} start time is {start_time},"
                    f"which is greater than 30 seconds"
                )
        self.full_results.add_key("attach_time", time_measures)

        average = statistics.mean(time_measures)
        logging.info(
            f"{self.msg_prefix} The average time for the sampled {len(time_measures)} pods is {average} seconds."
        )
        self.full_results.add_key("attach_time_average", average)

        st_deviation = statistics.stdev(time_measures)
        st_deviation_percent = st_deviation / average * 100.0
        logging.info(
            f"{self.msg_prefix} The standard deviation percent for the sampled {len(time_measures)} pods"
            f" is {st_deviation_percent}"
        )
        self.full_results.add_key("attach_time_stdev_percent", st_deviation_percent)

        # Getting the test end time
        self.test_end_time = PASTest.get_time()

        # Add the test time to the ES report
        self.full_results.add_key(
            "test_time", {"start": self.test_start_time, "end": self.test_end_time}
        )

        # Write the test results into the ES server
        self.full_results.es_write()

        # write the ES link to the test results in the test log.
        log.info(
            f"{self.msg_prefix} The Result can be found at : {self.full_results.results_link()}"
        )
