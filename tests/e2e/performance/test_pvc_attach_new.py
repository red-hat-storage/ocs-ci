"""
Testing the POD start time.
In this test we are creating a PVC, then creating a POD which attach to this PVC.
the time is measure from the POD yaml file : started_time - creation_time.
"""
import logging
import os
import statistics

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
import ocs_ci.ocs.exceptions as ex
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.perftests import PASTest

log = logging.getLogger(__name__)

# TODO: This dict can be move to the constant file to use in all other tests
Interfaces_info = {
    constants.CEPHBLOCKPOOL: {"name": "RBD", "sc": constants.CEPHBLOCKPOOL_SC},
    constants.CEPHFILESYSTEM: {"name": "CephFS", "sc": constants.CEPHFILESYSTEM_SC},
}


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
        # TODO - This part (reading test config from file) need to be done at global level
        try:
            if not config.TEST_CONF[self.benchmark_name].get("enabled"):
                self.skip = True
                return
            self.acceptable_time = config.TEST_CONF[self.benchmark_name].get(
                "acceptable_time", 30
            )
            self.interfaces = config.TEST_CONF[self.benchmark_name].get(
                "interface", [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]
            )
            self.samples_num = config.TEST_CONF[self.benchmark_name].get(
                "samples_num", 5
            )
            self.pvc_size = config.TEST_CONF[self.benchmark_name].get("pvc_size", 5)

        except KeyError:
            # Setting up default parameters
            log.info(
                "Setting up the test parameters to the defaults if no conf file provided"
            )
            self.acceptable_time = 30
            self.interfaces = [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]
            self.samples_num = 5
            self.pvc_size = 5

        super(TestPodStartTime, self).setup()

        # Pull the perf image to the nodes before the test is starting
        helpers.pull_images(constants.PERF_IMAGE)

        # Run the test in its own project (namespace)
        self.create_test_project()

        # Initialize some lists used in the test.
        self.pod_result_list = []
        self.start_time_dict_list = []
        self.pvc_list = []

        # The maximum acceptable attach time in sec.

    def teardown(self):
        """
        Cleanup the test environment
        """
        log.info("Starting the test cleanup")

        # Delete All created pods
        log.info("Delete all pods.....")
        for pod in self.pod_result_list:
            pod.delete()

        # Delete All created pvcs
        log.info("Delete all pvcs.....")
        for pvc in self.pvc_list:
            pvc.delete()

        # Deleting the namespace used by the test
        self.delete_test_project()

        super(TestPodStartTime, self).teardown()

    def run(self):
        """
        Running the test
        """
        for i in range(self.samples_num):

            # Creating PVC to attach POD to it
            log.info(f"{self.msg_prefix} Start creating PVC number {i + 1}.")
            pvc_obj = helpers.create_pvc(
                sc_name=Interfaces_info[self.interface]["sc"],
                size=self.pvc_size,
                namespace=self.namespace,
            )
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
            self.pvc_list.append(pvc_obj)
            log.info(f"{self.msg_prefix} PVC number {i + 1} was successfully created .")

            # Create a POD and attach it to the PVC
            try:
                pod_obj = helpers.create_pod(
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

            # Get the POD start time including the attache time
            self.start_time_dict_list.append(helpers.pod_start_time(pod_obj))

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
        full_results.add_key("samples_number", self.samples_num)
        full_results.add_key("pvc_size", self.pvc_size)
        return full_results

    def test_pod_start_time(
        self,
    ):
        """
        Test to log pod start times for all the sampled pods
        """

        if self.skip:
            log.info(f"This test ({self.benchmark_name}) mark to skip")
            return
        for self.interface in self.interfaces:
            self.msg_prefix = f"Interface: {self.interface}, PVC size: {self.pvc_size}."

            self.results_path = os.path.join(
                "/", *self.results_path, "test_pod_start_time"
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

            # Verify that all sample are in the acceptable time range,
            time_measures = [t["performance"] for t in self.start_time_dict_list]
            for index, start_time in enumerate(time_measures):
                log.info(
                    f"{self.msg_prefix} pod number {index} start time: {start_time} seconds"
                )
                if start_time > self.acceptable_time:
                    raise ex.PerformanceException(
                        f"{self.msg_prefix} Pod number {index} start time is {start_time},"
                        f"which is greater than {self.acceptable_time} seconds"
                    )
            self.full_results.all_results["attach_time"] = time_measures

            # Calculating the attachment average time, and the STD between all samples.
            average = statistics.mean(time_measures)
            log.info(
                f"{self.msg_prefix} The average time for the sampled {len(time_measures)} pods is {average} seconds."
            )
            self.full_results.add_key("attach_time_average", average)

            st_deviation = statistics.stdev(time_measures)
            st_deviation_percent = st_deviation / average * 100.0
            log.info(
                f"{self.msg_prefix} The standard deviation percent for the sampled {len(time_measures)} pods"
                f" is {st_deviation_percent}"
            )
            self.full_results.add_key("attach_time_stdev_percent", st_deviation_percent)

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

        # Push the test results into the performance dashboard
        self.add_test_to_results_check(
            test="test_pod_start_time", test_count=2, test_name="PVC Attach Time"
        )
        self.check_results_and_push_to_dashboard()
