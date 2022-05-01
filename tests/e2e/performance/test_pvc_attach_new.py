"""
Testing the POD start time.
In this test we are creating a PVC, then creating a POD which attach to this PVC.
the time is measure from the POD yaml file : started_time - creation_time.
"""
import logging
import statistics

from ocs_ci.framework import config
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import Interfaces_info
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
        Initialize the object by reading some data from the CRD file and
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

    def analyse_results(self, results_list, acceptable_time, msg_prefix):
        # Verify that all sample are in the acceptable time range,
        time_measures = [t["performance"] for t in results_list]
        self.all_results["attach_time"] = time_measures
        for index, start_time in enumerate(time_measures):
            log.info(
                f"{msg_prefix} pod number {index} start time: {start_time} seconds"
            )
            if start_time > acceptable_time:
                raise ex.PerformanceException(
                    f"{msg_prefix} Pod number {index} start time is {start_time},"
                    f"which is greater than {acceptable_time} seconds"
                )
        # Calculating the attachment average time, and the STD between all samples.
        samples = len(time_measures)
        average = statistics.mean(time_measures)
        log.info(
            f"{msg_prefix} The average time for the sampled {samples} pods is {average} seconds."
        )
        self.add_key("attach_time_average", average)

        st_deviation = statistics.stdev(time_measures)
        st_deviation_percent = st_deviation / average * 100.0
        log.info(
            f"{msg_prefix} The standard deviation percent for the sampled {samples} pods"
            f" is {st_deviation_percent}"
        )
        self.add_key("attach_time_stdev_percent", st_deviation_percent)


class TestPodStartTime(PASTest):
    """
    Measure time to start pod with PVC attached
    """

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        self.benchmark_name = "pvc_attach_time"
        try:
            self.params = config.TEST_CONF[self.benchmark_name]
            if not config.TEST_CONF[self.benchmark_name].get("enabled", True):
                return
        except KeyError:
            # Setting up default parameters
            log.warning(
                "No configuration is available for the test. Test will be skipped"
            )
            self.params = {"enabled": False}
            return

        super(TestPodStartTime, self).setup()

        # Pull the perf image to the nodes before the test is starting
        helpers.pull_images(constants.PERF_IMAGE)

        # Run the test in its own project (namespace)
        self.create_test_project()

        # Initialize some lists used in the test.
        self.pod_result_list = []
        self.pvc_list = []

        # The maximum acceptable attach time in sec.

    def teardown(self):
        """
        Cleanup the test environment
        """
        # teardown need to be run only if test wasn't skipped
        if self.params.get("enabled", True):
            log.info("Starting the test cleanup")
            # Deleting the namespace used by the test
            self.delete_test_project()
            super(TestPodStartTime, self).teardown()

    def cleanup(self):
        """
        Cleaning the cluster from pods and pvcs which created during the test.
        """
        # Delete All created pods
        log.info("Delete all pods.....")
        for pod in self.pod_result_list:
            pod.delete(wait=False)
        performance_lib.wait_for_resource_bulk_status(
            resource="pod",
            resource_count=0,
            namespace=self.namespace,
            status=constants.STATUS_RUNNING,
            timeout=120,
            sleep_time=5,
        )

        # Delete All created pvcs
        log.info("Delete all pvcs.....")
        for pvc in self.pvc_list:
            pvc.delete(wait=False)
        performance_lib.wait_for_resource_bulk_status(
            resource="pvc",
            resource_count=0,
            namespace=self.namespace,
            status=constants.STATUS_RUNNING,
            timeout=120,
            sleep_time=5,
        )
        self.pod_result_list = []
        self.pvc_list = []

    def run(self):
        """
        Running the test
        """
        self.start_time_dict_list = []
        for i in range(self.params["samples_num"]):

            index = i + 1
            # Creating PVC to attach POD to it
            log.info(f"{self.msg_prefix} Start creating PVC number {index}.")
            pvc_obj = helpers.create_pvc(
                pvc_name=f"pas-pvc-{Interfaces_info[self.interface]['name'].lower()}-{index}",
                sc_name=Interfaces_info[self.interface]["sc"],
                size=self.params["pvc_size"],
                namespace=self.namespace,
            )
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
            self.pvc_list.append(pvc_obj)
            log.info(f"{self.msg_prefix} PVC number {index} was successfully created .")

            # Create a POD and attach it to the PVC
            try:
                pod_obj = helpers.create_pod(
                    pod_name=f"pas-pod-{Interfaces_info[self.interface]['name'].lower()}-{index}",
                    interface_type=self.interface,
                    pvc_name=pvc_obj.name,
                    namespace=self.namespace,
                    pod_dict_path=constants.PERF_POD_YAML,
                )
                helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
                pod_obj.reload()
            except Exception as e:
                log.error(
                    f"Pod on PVC {pvc_obj.name} was not created, exception {str(e)}"
                )
                raise ex.PodNotCreated("Pod on PVC was not created.")
            self.pod_result_list.append(pod_obj)

            # Get the POD start time including the attached time
            self.start_time_dict_list.append(helpers.pod_start_time(pod_obj))

        # Cleanup the environment after each test
        self.cleanup()

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
        full_results.add_key("storageclass", Interfaces_info[self.interface]["name"])
        full_results.add_key("samples_number", self.params["samples_num"])
        full_results.add_key("pvc_size", self.params["pvc_size"])
        return full_results

    def test_pod_start_time(
        self,
    ):
        """
        Test to log pod start times for all the sampled pods
        """

        if not self.params.get("enabled", True):
            log.info(f"This test ({self.benchmark_name}) mark to skip")
            return

        self.results_path = helpers.get_full_test_logs_path(cname=self)

        for self.interface in self.params["interfaces"]:
            self.msg_prefix = (
                f"Interface: {self.interface}, PVC size: {self.params['pvc_size']}."
            )

            # Getting the test start time
            self.test_start_time = self.get_time()

            # The actual test start here
            self.run()

            # Collecting environment information
            self.get_env_info()

            # Initialize the results' doc file.
            self.full_results = self.init_full_results(
                ResultsAnalyse(self.uuid, self.crd_data, self.full_log_path)
            )

            # Analysing the test results
            self.full_results.analyse_results(
                results_list=self.start_time_dict_list,
                acceptable_time=self.params["acceptable_time"],
                msg_prefix=self.msg_prefix,
            )

            # Getting the test end time
            self.test_end_time = self.get_time()

            # Add the test time to the ES report
            self.full_results.add_key(
                "test_time", {"start": self.test_start_time, "end": self.test_end_time}
            )

            # Write the test results into the ES server
            if self.full_results.es_write():
                res_link = self.full_results.results_link()
                log.info(f"The Result can be found at : {res_link}")

                # Create text file with results of all subtest (4 - according to the parameters)
                self.write_result_to_file(res_link)

        self.results_path = self.results_path.split("/")[1:-1]

        # Push the test results into the performance dashboard - only if ALL tests (2) ran, if not, don't try to push
        # the results into the Performance dashboard
        if len(self.params["interfaces"]) == 2:
            self.add_test_to_results_check(
                test="test_pod_start_time", test_count=2, test_name="PVC Attach Time"
            )
            try:
                self.check_results_and_push_to_dashboard()
            except Exception as exp:
                log.error(f"Can not push the results into the performance DB [{exp}]")
