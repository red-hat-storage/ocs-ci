import logging
import pytest
import ocs_ci.ocs.exceptions as ex
import urllib.request
import time
import statistics
import os

from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import performance, performance_a
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.exceptions import PodNotCreated

logger = logging.getLogger(__name__)


@grey_squad
@performance
@performance_a
class TestPodReattachTimePerformance(PASTest):
    """
    Test to verify Pod Reattach Time Performance
    creates samples and measures average total and csi reattach times
    """

    def setup(self):
        """
        Setting up test parameters
        """
        logger.info("Starting the test setup")
        # Run the test in its own project (namespace)
        self.create_test_project()
        self.pvc_list = []

        super(TestPodReattachTimePerformance, self).setup()
        self.benchmark_name = "pod_reattach_time"

    def teardown(self):
        """
        Cleanup the test environment
        """
        logger.info("Starting the test cleanup")

        # Deleting the namespace used by the test
        self.delete_test_project()

        super(TestPodReattachTimePerformance, self).teardown()

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
    def base_setup(self, interface):
        """
        A setup phase for the test
        Args:
            interface: Interface parameter

        """

        self.interface = interface

        if self.interface == constants.CEPHFILESYSTEM:
            self.sc = "CephFS"
        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc = "RBD"

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{self.sc}"

    @pytest.mark.parametrize(
        argnames=["interface", "copies", "timeout", "total_time_limit"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 3, 120, 150],
                marks=pytest.mark.polarion_id("OCS-2043"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 13, 600, 720],
                marks=pytest.mark.polarion_id("OCS-2673"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 3, 120, 150],
                marks=pytest.mark.polarion_id("OCS-2044"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 13, 600, 720],
                marks=pytest.mark.polarion_id("OCS-2674"),
            ),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pod_reattach_time_performance(
        self, storageclass_factory, copies, timeout, total_time_limit
    ):
        """
        Test assign nodeName to a pod using RWX pvc
        Each kernel (unzipped) is 892M and 61694 files
        The test creates samples_num pvcs and pods, writes kernel files multiplied by number of copies
        and calculates average total and csi reattach times and standard deviation
        """
        kernel_url = "https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-4.19.5.tar.gz"
        download_path = "tmp"

        samples_num = 7
        if self.dev_mode:
            samples_num = 3

        test_start_time = PASTest.get_time()
        helpers.pull_images(constants.PERF_IMAGE)
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

        time_measures, csi_time_measures, files_written_list, data_written_list = (
            [],
            [],
            [],
            [],
        )

        self.sc_obj = storageclass_factory(self.interface)
        for sample_index in range(1, samples_num + 1):
            csi_start_time = self.get_time("csi")

            logger.info(f"Start creating PVC number {sample_index}.")
            pvc_obj = helpers.create_pvc(
                sc_name=self.sc_obj.name, size="100Gi", namespace=self.namespace
            )
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)

            # Create a pod on one node
            logger.info(f"Creating Pod with pvc {pvc_obj.name} on node {node_one}")

            pvc_obj.reload()
            self.pvc_list.append(pvc_obj)

            try:
                pod_obj1 = helpers.create_pod(
                    interface_type=self.interface,
                    pvc_name=pvc_obj.name,
                    namespace=pvc_obj.namespace,
                    node_name=node_one,
                    pod_dict_path=constants.PERF_POD_YAML,
                )
            except Exception as e:
                logger.error(
                    f"Pod on PVC {pvc_obj.name} was not created, exception {str(e)}"
                )
                raise PodNotCreated("Pod on PVC was not created.")

            # Confirm that pod is running on the selected_nodes
            logger.info("Checking whether pods are running on the selected nodes")
            helpers.wait_for_resource_state(
                resource=pod_obj1, state=constants.STATUS_RUNNING, timeout=timeout
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

            logger.info("Getting the amount of data written to the PVC")
            rsh_cmd = f"exec {pod_name} -- df -h {pod_path}"
            data_written_str = _ocp.exec_oc_cmd(rsh_cmd).split()[-4]
            logger.info(f"The amount of written data is {data_written_str}")
            data_written = float(data_written_str[:-1])

            rsh_cmd = f"exec {pod_name} -- find {pod_path} -type f"
            files_written = len(_ocp.exec_oc_cmd(rsh_cmd).split())
            logger.info(
                f"For {self.interface} - The number of files written to the pod is {files_written}"
            )
            files_written_list.append(files_written)
            data_written_list.append(data_written)

            logger.info("Deleting the pod")
            rsh_cmd = f"delete pod {pod_name}"
            _ocp.exec_oc_cmd(rsh_cmd)

            logger.info(f"Creating Pod with pvc {pvc_obj.name} on node {node_two}")

            try:
                pod_obj2 = helpers.create_pod(
                    interface_type=self.interface,
                    pvc_name=pvc_obj.name,
                    namespace=pvc_obj.namespace,
                    node_name=node_two,
                    pod_dict_path=constants.PERF_POD_YAML,
                )
            except Exception as e:
                logger.error(
                    f"Pod on PVC {pvc_obj.name} was not created, exception {str(e)}"
                )
                raise PodNotCreated("Pod on PVC was not created.")

            start_time = time.time()

            pod_name = pod_obj2.name
            helpers.wait_for_resource_state(
                resource=pod_obj2, state=constants.STATUS_RUNNING, timeout=timeout
            )
            end_time = time.time()
            total_time = end_time - start_time
            if total_time > total_time_limit:
                logger.error(
                    f"Pod creation time is {total_time} and greater than {total_time_limit} seconds"
                )
                raise ex.PerformanceException(
                    f"Pod creation time is {total_time} and greater than {total_time_limit} seconds"
                )

            csi_time = performance_lib.pod_attach_csi_time(
                self.interface, pvc_obj.backed_pv, csi_start_time, pvc_obj.namespace
            )
            csi_time_measures.append(csi_time)
            logger.info(
                f"PVC #{pvc_obj.name} pod {pod_name} creation time took {total_time} seconds, "
                f"csi time is {csi_time} seconds"
            )
            time_measures.append(total_time)

            logger.info("Deleting the pod")
            rsh_cmd = f"delete pod {pod_name}"
            _ocp.exec_oc_cmd(rsh_cmd)
            # teardown_factory(pod_obj2)

        average = statistics.mean(time_measures)
        logger.info(
            f"The average time of {self.interface} pod creation on {samples_num} PVCs is {average} seconds"
        )

        st_deviation = statistics.stdev(time_measures)
        logger.info(
            f"The standard deviation of {self.interface} pod creation time on {samples_num} PVCs is {st_deviation}"
        )

        csi_average = statistics.mean(csi_time_measures)
        logger.info(
            f"The average csi time of {self.interface} pod creation on {samples_num} PVCs is {csi_average} seconds"
        )

        csi_st_deviation = statistics.stdev(csi_time_measures)
        logger.info(
            f"The standard deviation of {self.interface} csi pod creation time on {samples_num} PVCs "
            f"is {csi_st_deviation}"
        )

        files_written_average = statistics.mean(files_written_list)
        data_written_average = statistics.mean(data_written_list)

        os.remove(file_path)
        os.rmdir(dir_path)

        # Produce ES report

        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "pod_reattach_time_fullres",
            )
        )

        full_results.add_key("storageclass", self.sc)
        full_results.add_key("pod_reattach_time", time_measures)
        full_results.add_key("copies_number", copies)
        full_results.add_key("files_number_average", files_written_average)
        full_results.add_key("data_average", data_written_average)
        full_results.add_key("pod_reattach_time_average", average)
        full_results.add_key("pod_reattach_standard_deviation", st_deviation)
        full_results.add_key("pod_csi_reattach_time_average", csi_average)
        full_results.add_key("pod_csi_reattach_standard_deviation", csi_st_deviation)

        test_end_time = PASTest.get_time()

        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": test_start_time, "end": test_end_time}
        )

        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            logger.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (4 - according to the parameters)
            self.results_path = get_full_test_logs_path(
                cname=self, fname="test_pod_reattach_time_performance"
            )
            self.write_result_to_file(res_link)

    def test_pod_reattach_time_results(self):
        """
        This is not a test - it is only check that previous test ran and finish as expected
        and reporting the full results (links in the ES) of previous tests (4)
        """

        self.number_of_tests = 4
        self.results_path = get_full_test_logs_path(
            cname=self, fname="test_pod_reattach_time_performance"
        )
        self.results_file = os.path.join(self.results_path, "all_results.txt")
        logger.info(f"Check results in {self.results_file}")

        self.check_tests_results()

        self.push_to_dashboard(test_name="POD Reattach")
