import logging
import pytest
import time
import statistics

from ocs_ci.framework.testlib import performance
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants, node
import ocs_ci.ocs.exceptions as ex
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.ocs.perftests import PASTest

logger = logging.getLogger(__name__)

Interfaces_info = {
    constants.CEPHBLOCKPOOL: {
        "name": "RBD",
        "sc_name": constants.DEFAULT_STORAGECLASS_RBD,
    },
    constants.CEPHFILESYSTEM: {
        "name": "CephFS",
        "sc_name": constants.DEFAULT_STORAGECLASS_CEPHFS,
    },
}


@performance
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

    def create_pod_and_wait_for_completion(self, **kwargs):
        # Creating pod yaml file to run as a Job, the command to run on the pod and
        # arguments to it will replace in the create_pod function
        self.create_fio_pod_yaml(pvc_size=100, filesize=kwargs.pop("filesize", "1M"))
        # Create a pod
        logger.info(f"Creating Pod with pvc {self.pvc_obj.name}")

        try:
            pod_object = helpers.create_pod(
                pvc_name=self.pvc_obj.name,
                namespace=self.namespace,
                interface_type=self.interface,
                pod_name="pod-pas-test",
                pod_dict_path=self.pod_yaml_file.name,
                **kwargs,
            )
        except Exception as e:
            logger.exception(
                f"Pod attached to PVC {pod_object.name} was not created, exception [{str(e)}]"
            )
            raise ex.PodNotCreated("Pod attached to PVC was not created.")

        # Confirm that pod is running on the selected_nodes
        logger.info("Checking whether the pod is running")
        helpers.wait_for_resource_state(
            resource=pod_object,
            state=constants.STATUS_COMPLETED,
            timeout=1200,
        )
        return pod_object

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
        return full_results

    @pytest.mark.parametrize(
        argnames=["interface", "copies", "timeout", "total_time_limit"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 3, 120, 70],
                marks=pytest.mark.polarion_id("OCS-2043"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 13, 600, 420],
                marks=pytest.mark.polarion_id("OCS-2673"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 3, 120, 70],
                marks=pytest.mark.polarion_id("OCS-2044"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 13, 600, 420],
                marks=pytest.mark.polarion_id("OCS-2674"),
            ),
        ],
    )
    def test_pod_reattach_time_performance(
        self, storageclass_factory, interface, copies, timeout, total_time_limit
    ):
        """
        Test assign nodeName to a pod using RWX pvc
        Each kernel (unzipped) is 892M and 61694 files
        The test creates samples_num pvcs and pods, writes kernel files multiplied by number of copies
        and calculates average total and csi reattach times and standard deviation
        """

        self.interface = interface

        samples_num = 7
        if self.dev_mode:
            samples_num = 3

        test_start_time = self.get_time()
        helpers.pull_images(constants.PERF_IMAGE)

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
            self.pvc_obj = helpers.create_pvc(
                sc_name=self.sc_obj.name, size="100Gi", namespace=self.namespace
            )
            helpers.wait_for_resource_state(self.pvc_obj, constants.STATUS_BOUND)

            # Create a pod on one node
            logger.info(f"Creating Pod with pvc {self.pvc_obj.name} on node {node_one}")

            self.pvc_obj.reload()
            self.pvc_list.append(self.pvc_obj)

            pod_obj1 = self.create_pod_and_wait_for_completion(
                command=["/opt/multiple_files.sh"],
                command_args=[f"{copies}", "/mnt"],
                node_name=node_one,
            )

            pod_name = pod_obj1.name

            # Get the number of files and total written data from the pod
            logger.info("Getting the amount of data written to the PVC")
            for line in pod_obj1.ocp.get_logs(name=pod_obj1.name).split("\n"):
                if "Number Of Files" in line:
                    files_written = line.split(" ")[-1]
                if "Total Data" in line:
                    data_written = line.split(" ")[-1]
            logger.info(f"The amount of written data is {data_written}")
            logger.info(
                f"For {self.interface} - The number of files written to the pod is {int(files_written):,}"
            )

            files_written_list.append(int(files_written))
            data_written_list.append(float(data_written[:-1]))

            logger.info("Deleting the pod")
            pod_obj1.delete()

            logger.info(f"Creating Pod with pvc {self.pvc_obj.name} on node {node_two}")

            try:
                pod_obj2 = helpers.create_pod(
                    interface_type=self.interface,
                    pvc_name=self.pvc_obj.name,
                    namespace=self.pvc_obj.namespace,
                    node_name=node_two,
                    pod_dict_path=constants.PERF_POD_YAML,
                )
            except Exception as e:
                logger.error(
                    f"Pod on PVC {self.pvc_obj.name} was not created, exception {str(e)}"
                )
                raise ex.PodNotCreated("Pod on PVC was not created.")

            start_time = time.time()

            pod_name = pod_obj2.name
            helpers.wait_for_resource_state(
                resource=pod_obj2, state=constants.STATUS_RUNNING, timeout=timeout
            )
            end_time = time.time()
            total_time = end_time - start_time
            if total_time > total_time_limit:
                logger.error(
                    f"Pod creation time is {total_time:,.3f} and greater than {total_time_limit:,.3f} seconds"
                )
                raise ex.PerformanceException(
                    f"Pod creation time is {total_time:,.3f} and greater than {total_time_limit:,.3f} seconds"
                )

            csi_time = performance_lib.pod_attach_csi_time(
                self.interface,
                self.pvc_obj.backed_pv,
                csi_start_time,
                self.pvc_obj.namespace,
            )[0]
            csi_time_measures.append(csi_time)
            logger.info(
                f"PVC {self.pvc_obj.name}, pod {pod_name} creation time took {total_time:,.3f} seconds, "
                f"csi time is {csi_time:,.3f} seconds"
            )
            time_measures.append(total_time)

            logger.info("Deleting the pod")
            pod_obj2.delete()

        average = statistics.mean(time_measures)
        st_deviation = statistics.stdev(time_measures)
        csi_average = statistics.mean(csi_time_measures)
        csi_st_deviation = statistics.stdev(csi_time_measures)
        files_written_average = statistics.mean(files_written_list)
        data_written_average = statistics.mean(data_written_list)

        logger.info("=================================================================")
        logger.info(f"Summery results for {self.interface} with {samples_num} samples:")
        logger.info("--------------------------------------------------------")
        logger.info(f"Average number of files on the PVC is {files_written_average:,}")
        logger.info(f"Average data on the PVC is {data_written_average:,.3f} GB")
        logger.info(f"The average pod creation time is {average:,.3f} seconds")
        logger.info(
            f"The standard deviation of pod creation time is {st_deviation:,.3f}"
        )
        logger.info(
            f"The average csi time of pod creation is {csi_average:,.3f} seconds"
        )
        logger.info(
            f"The standard deviation of csi pod creation time is {csi_st_deviation:,.3f}"
        )
        logger.info("=================================================================")

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

        full_results.add_key("storageclass", Interfaces_info[self.interface]["name"])
        full_results.add_key("pod_reattach_time", time_measures)
        full_results.add_key("copies_number", copies)
        full_results.add_key("files_number_average", files_written_average)
        full_results.add_key("data_average", data_written_average)
        full_results.add_key("pod_reattach_time_average", average)
        full_results.add_key("pod_reattach_standard_deviation", st_deviation)
        full_results.add_key("pod_csi_reattach_time_average", csi_average)
        full_results.add_key("pod_csi_reattach_standard_deviation", csi_st_deviation)

        test_end_time = self.get_time()

        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": test_start_time, "end": test_end_time}
        )

        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            logger.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (4 - according to the parameters)
            self.results_path = helpers.get_full_test_logs_path(
                cname=self, fname="test_pod_reattach_time_performance"
            )
            self.write_result_to_file(res_link)

    def test_pod_reattach_time_results(self):
        """
        This is not a test - it is only check that previous test ran and finish as expected
        and reporting the full results (links in the ES) of previous tests (4)
        """

        self.add_test_to_results_check(
            test="test_pod_reattach_time_performance",
            test_count=4,
            test_name="POD Reattach",
        )
        self.check_results_and_push_to_dashboard()
