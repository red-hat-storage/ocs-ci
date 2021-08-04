import logging
import pytest
import time
from uuid import uuid4

import numpy

from ocs_ci.framework import config
from ocs_ci.framework.testlib import performance
from ocs_ci.helpers.helpers import (
    get_full_test_logs_path,
    pod_start_time,
    pull_images,
    create_pvc,
    wait_for_resource_state,
    create_pod,
)
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

    def float_format(self, number):
        """
        Formatting floating point number to have only 2 digits after the floating point

        Args:
            number (float): floating point number

        Returns:
             float : formatted floating point number - N.xx

        """
        return float("{:.2f}".format(number))

    def analyse_results(self, results):
        """
        Analyze the test results, write them to the log and
        push them into the elastic-search document.

        Args:
            results (list) : list of all samples test results

        """
        log.info("Analyze the test results")
        attach_time = self.float_format(numpy.average(results))
        st_deviation = self.float_format(numpy.std(results))
        mean = self.float_format(numpy.mean(results))
        pct_dev = self.float_format((st_deviation / mean) * 100)

        self.add_key("attach_time", attach_time)
        self.add_key("std_deviation", st_deviation)
        self.add_key("pct_deviation", pct_dev)
        self.add_key("samples_results", results)

        log.info(f"All results are : {results}")
        log.info(f"Average of {len(results)} is {attach_time}")
        log.info(f"The samples standard deviation (%) is : {pct_dev}")

        # TODO: normalize the results (PR #4169 implement this)

        if pct_dev > 30:
            log.warning("Deviation between samples is more then 30%")

        for res in results:
            if res > 30:
                raise ex.PerformanceException(
                    "pod start time is greater than 30 seconds"
                )


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

        log.info(f"Pulling pod image {constants.PERF_IMAGE}")
        pull_images(constants.PERF_IMAGE)

        self.pvc_size = 5  # The size (in GiB) of the pv to create
        self.number_of_samples = 7  # The number of samples to run

    def teardown(self):
        """
        Cleanup the cluster from resources created during the test
        """
        log.info("Starting the test teardown")
        self.pod_obj.delete(wait=True)
        self.pvc_obj.delete(wait=True)
        super(TestPodStartTime, self).teardown()

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

    def get_time(self):
        """
        Getting the current GMT time in a specific format for the ES report

        Returns:
            str : current date and time in formatted way

        """
        return time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())

    def run(self):
        """
        Running the test

        Returns:
            results (list) : list of all samples pod attache time
        """

        # List of all samples results
        results = []

        # Setting timeout for pvc/pod creation
        timeout = 600 if self.interface == constants.CEPHBLOCKPOOL_THICK else 60

        for i in range(self.number_of_samples):

            test_num = i + 1  # since range start with 0

            log.info(f"Starting sample number {test_num}")

            # Creating PVC and wait until it bound
            log.info(f"Creating {self.pvc_size} GiB {self.sc} PVC")
            self.pvc_obj = create_pvc(sc_name=self.sc_name, size=f"{self.pvc_size}Gi")
            wait_for_resource_state(
                self.pvc_obj, constants.STATUS_BOUND, timeout=timeout
            )
            self.pvc_obj.reload()
            log.info(f"PVC number {test_num} was created in.")

            # Attach POD to the PVC and wait for it to be in Running state
            log.info(f"Creating Pod with pvc {self.pvc_obj.name}")
            self.pod_obj = create_pod(
                interface_type=self.interface,
                pvc_name=self.pvc_obj.name,
                namespace=self.pvc_obj.namespace,
                pod_dict_path=constants.PERF_POD_YAML,
            )
            wait_for_resource_state(
                self.pod_obj, constants.STATUS_RUNNING, timeout=timeout
            )
            self.pod_obj.reload()

            # Getting the pod start time
            start_time_dict = pod_start_time(self.pod_obj)
            start_time = start_time_dict["performance"]

            results.append(start_time)
            log.info(f"pod start time is : {start_time} seconds")

            # Delete the pod
            log.info(f"Delete pod number : {test_num}")
            self.pod_obj.delete(wait=True)

            # Delete the pvc
            log.info(f"Delete PVC number : {test_num}")
            self.pvc_obj.delete(wait=True)

            # Wait 30 sec. between samples
            time.sleep(30)

        return results

    def test_pod_start_time(self, interface):
        """
        Test to log pod start time
        """
        # Getting the test start time
        self.start_time = self.get_time()

        self.interface = interface

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc_name = constants.CEPHBLOCKPOOL_SC
            self.sc = "RBD"
        elif self.interface == constants.CEPHFILESYSTEM:
            self.sc_name = constants.CEPHFILESYSTEM_SC
            self.sc = "CephFS"
        elif self.interface == constants.CEPHBLOCKPOOL_THICK:
            self.sc_name = "ocs-storagecluster-ceph-rbd-thick"
            self.sc = "RBD-Thick"
        self.full_log_path += f"-{self.sc}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        # Collecting environment information
        self.get_env_info()

        # Run the test
        results = self.run()

        # Initialize the results doc file.
        self.full_results = self.init_full_results(
            ResultsAnalyse(self.uuid, self.crd_data, self.full_log_path)
        )

        # Analyze the results
        self.full_results.analyse_results(results)

        self.full_results.add_key("samples", self.number_of_samples)
        self.full_results.add_key("storageclass", self.sc)

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
