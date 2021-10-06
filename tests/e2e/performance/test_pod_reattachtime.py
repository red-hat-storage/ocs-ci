import logging
import pytest
import ocs_ci.ocs.exceptions as ex
import urllib.request
import time
import statistics
import os
from uuid import uuid4

from ocs_ci.framework.testlib import performance
from ocs_ci.framework import config
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.perftests import PASTest


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
        self.new_index = "pod_reattach_time_fullres"
        self.full_log_path = full_log_path
        # make sure we have connection to the elastic search server
        self.es_connect()


@pytest.mark.polarion_id("OCS-2208")
@performance
class TestPodReattachTimePerformance(PASTest):
    """
    Test to verify Pod Reattach Time Performance
    """

    def setup(self):
        """
        Setting up test parameters
        """
        logging.info("Starting the test setup")
        super(TestPodReattachTimePerformance, self).setup()
        self.benchmark_name = "pod_reattach_time"
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

        helpers.pull_images(constants.PERF_IMAGE)

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
    def base_setup(self, interface_iterate):
        """
        A setup phase for the test
        Args:
            interface_iterate: A fixture to iterate over ceph interfaces

        """

        self.interface = interface_iterate

        if self.interface == constants.CEPHFILESYSTEM:
            self.sc = "CephFS"
        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc = "RBD"

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{self.sc}"

    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pod_reattach_time_performance(self, pvc_factory, teardown_factory):
        """
        Test assign nodeName to a pod using RWX pvc
        Each kernel (unzipped) is 892M and 61694 files
        The test creates samples_num pvcs and pods and calculates average reattach time and standard deviation
        """
        kernel_url = "https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-4.19.5.tar.gz"
        download_path = "tmp"
        # Number of times we copy the kernel
        copies = 3

        samples_num = 10
        test_start_time = performance_lib.get_time()

        # Download a linux Kernel

        dir_path = os.path.join(os.getcwd(), download_path)
        file_path = os.path.join(dir_path, "file.gz")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        urllib.request.urlretrieve(kernel_url, file_path)

        worker_nodes_list = node.get_worker_nodes()
        assert len(worker_nodes_list) > 1
        node_one = worker_nodes_list[0]
        node_two = worker_nodes_list[1]

        time_measures = []
        for sample_index in range(1, samples_num + 1):
            # Create a PVC
            accessmode = constants.ACCESS_MODE_RWX
            if self.interface == constants.CEPHBLOCKPOOL:
                accessmode = constants.ACCESS_MODE_RWO

            try:
                pvc_obj = pvc_factory(
                    interface=self.interface,
                    access_mode=accessmode,
                    status=constants.STATUS_BOUND,
                    size="15",
                )
            except Exception as e:
                logging.error(f"The PVC sample was not created, exception {str(e)}")
                raise Exception("Test was not completed")

            # Create a pod on one node
            logging.info(f"Creating Pod with pvc {pvc_obj.name} on node {node_one}")

            try:
                pod_obj1 = helpers.create_pod(
                    interface_type=self.interface,
                    pvc_name=pvc_obj.name,
                    namespace=pvc_obj.namespace,
                    node_name=node_one,
                    pod_dict_path=constants.PERF_POD_YAML,
                )
            except Exception as e:
                logging.error(
                    f"Pod on PVC {pvc_obj.name} was not created, exception {str(e)}"
                )
                raise Exception("Test was not completed")

            # Confirm that pod is running on the selected_nodes
            logging.info("Checking whether pods are running on the selected nodes")
            helpers.wait_for_resource_state(
                resource=pod_obj1, state=constants.STATUS_RUNNING, timeout=120
            )

            pod_name = pod_obj1.name
            pod_path = "/mnt"

            _ocp = OCP(namespace=pvc_obj.namespace)

            rsh_cmd = f"rsync {dir_path} {pod_name}:{pod_path}"
            _ocp.exec_oc_cmd(rsh_cmd)

            rsh_cmd = (
                f"exec {pod_name} -- tar xvf {pod_path}/tmp/file.gz -C {pod_path}/tmp"
            )
            _ocp.exec_oc_cmd(rsh_cmd)

            for x in range(copies):
                rsh_cmd = f"exec {pod_name} -- mkdir -p {pod_path}/folder{x}"
                _ocp.exec_oc_cmd(rsh_cmd)
                rsh_cmd = (
                    f"exec {pod_name} -- cp -r {pod_path}/tmp {pod_path}/folder{x}"
                )
                _ocp.exec_oc_cmd(rsh_cmd)
                rsh_cmd = f"exec {pod_name} -- sync"
                _ocp.exec_oc_cmd(rsh_cmd)

            logging.info("Getting the amount of data written to the PVC")
            rsh_cmd = f"exec {pod_name} -- df -h {pod_path}"
            data_written = _ocp.exec_oc_cmd(rsh_cmd).split()[-4]
            logging.info(
                f"The Amount of data that was written to the pod is {data_written}"
            )
            logging.info("Deleting the pod")
            rsh_cmd = f"delete pod {pod_name}"
            _ocp.exec_oc_cmd(rsh_cmd)

            logging.info(f"Creating Pod with pvc {pvc_obj.name} on node {node_two}")

            try:
                pod_obj2 = helpers.create_pod(
                    interface_type=self.interface,
                    pvc_name=pvc_obj.name,
                    namespace=pvc_obj.namespace,
                    node_name=node_two,
                    pod_dict_path=constants.PERF_POD_YAML,
                )
            except Exception as e:
                logging.error(
                    f"Pod on PVC {pvc_obj.name} was not created, exception {str(e)}"
                )
                raise Exception("Test was not completed")

            start_time = time.time()

            pod_name = pod_obj2.name
            helpers.wait_for_resource_state(
                resource=pod_obj2, state=constants.STATUS_RUNNING, timeout=120
            )
            end_time = time.time()
            total_time = end_time - start_time
            if total_time > 60:
                logging.error(
                    f"Pod creation time is {total_time} and greater than 60 seconds"
                )
                raise ex.PerformanceException(
                    f"Pod creation time is {total_time} and greater than 60 seconds"
                )
            logging.info(
                f"PVC #{pvc_obj.name} pod {pod_name} creation time took {total_time} seconds"
            )
            time_measures.append(total_time)

            teardown_factory(pod_obj2)

        average = statistics.mean(time_measures)
        logging.info(
            f"The average time of {self.interface} pod creation on {samples_num} PVCs is {average} seconds"
        )

        st_deviation = statistics.stdev(time_measures)
        logging.info(
            f"The standard deviation of {self.interface} pod creation time on {samples_num} PVCs is {st_deviation}"
        )

        os.remove(file_path)
        os.rmdir(dir_path)

        # Produce ES report

        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(self.uuid, self.crd_data, self.full_log_path)
        )

        full_results.add_key("storageclass", self.sc)
        full_results.add_key("pod_reattach_time", time_measures)
        full_results.add_key("pod_reattach_time_average", average)
        full_results.add_key("pod_reattach_standard_deviation", st_deviation)

        test_end_time = performance_lib.get_time()

        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": test_start_time, "end": test_end_time}
        )

        # Write the test results into the ES server
        full_results.es_write()
        # write the ES link to the test results in the test log.
        logging.info(f"The result can be found at : {full_results.results_link()}")
