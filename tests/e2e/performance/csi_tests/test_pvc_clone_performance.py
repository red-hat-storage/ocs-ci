"""
Test to verify clone creation and deletion performance for PVC with data written to it.
Performance is measured by collecting clone creation/deletion speed.
"""
import logging
import pytest
import statistics

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import performance, performance_b
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.utility.utils import convert_device_size
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.resources import pvc, ocs
from ocs_ci.ocs.exceptions import PVCNotCreated, PodNotCreated

logger = logging.getLogger(__name__)

Interfaces_info = {
    constants.CEPHBLOCKPOOL: {
        "name": "RBD",
        "sc": constants.CEPHBLOCKPOOL_SC,
        "clone_yaml": constants.CSI_RBD_PVC_CLONE_YAML,
        "accessmode": constants.ACCESS_MODE_RWO,
    },
    constants.CEPHFILESYSTEM: {
        "name": "CephFS",
        "sc": constants.CEPHFILESYSTEM_SC,
        "clone_yaml": constants.CSI_CEPHFS_PVC_CLONE_YAML,
        "accessmode": constants.ACCESS_MODE_RWX,
    },
}


class ClonesResultsAnalyse(ResultsAnalyse):
    """
    This class is reading all test results from elasticsearch server (which the
    benchmark-operator running of the benchmark is generate), aggregate them by :
        test operation (e.g. create / delete etc.)
        sample (for test to be valid it need to run with more the one sample)
        host (test can be run on more then one pod {called host})

    it generates results for all tests as one unit which will be valid only
    if the deviation between samples is less the 5%

    """

    def analyse_results(self, test_times, speed=False, total_data=0):
        op_types = ["create", "csi_create", "delete", "csi_delete"]
        all_data = {}
        avg_data = {}
        for op in op_types:
            all_data[op] = []
            avg_data[op] = []

        if speed:
            speeds = {"create": [], "delete": []}

        # Print the results into the log.
        for clone in test_times:
            logger.info(f"Test report for clone {clone} :")
            for op in op_types:
                data = test_times[clone][op]["time"]
                title = f"{op.capitalize()}ion time is"
                logger.info(f"{title:29} : {data} Secounds")
                if data is None:
                    logger.warning(f"   There is no {op.capitalize()}ion time !")
                else:
                    all_data[op].append(data)
                if speed and "csi" not in op:
                    speeds[op].append(float(total_data / test_times[clone][op]["time"]))
                    logger.info(
                        f"{op.capitalize()}ion speed is            : {speeds[op][-1]:,.2f} MB/Sec."
                    )

        logger.info("=============== Average results ================")
        for op in op_types:
            avg_data[op] = statistics.mean(all_data[op])
            title = f"Average {op.capitalize()}ion time is"
            logger.info(f"{title:29} : {avg_data[op]:.3f} Secounds")
            self.add_key(f"average_clone_{op}ion_time", avg_data[op])
            if speed and "csi" not in op:
                average_speed = statistics.mean(speeds[op])
                logger.info(
                    f"Average {op}ion speed is    : {average_speed:,.2f} MB/Sec."
                )
                self.add_key(f"average_clone_{op}ion_speed", average_speed)

        self.all_results = test_times
        logger.info("test_clones_creation_performance finished successfully.")


@grey_squad
@performance
@performance_b
class TestPVCClonePerformance(PASTest):
    """
    Test to verify clone creation and deletion performance for PVC with data written to it.
    Performance is this test is measured by collecting clone creation/deletion time and speed
    for 10 clone samples.
    """

    def setup(self):
        """
        Setting up test parameters
        """
        logger.info("Starting the test setup")
        super(TestPVCClonePerformance, self).setup()
        self.benchmark_name = "pvc_clone_permorance"
        helpers.pull_images(constants.PERF_IMAGE)

        self.create_test_project()

        # Collecting environment information
        self.get_env_info()

        self.number_of_clones = 11
        if self.dev_mode:
            self.number_of_clones = 3

        self.clones_list = []

    def teardown(self):
        """
        Cleanup the test environment
        """
        logger.info("Starting the test environment cleanup")
        try:
            logger.info(f"Deleting the test StorageClass : {self.sc_obj.name}")
            self.sc_obj.delete()
            logger.info("Wait until the SC is deleted.")
            self.sc_obj.ocp.wait_for_delete(resource_name=self.sc_obj.name)
        except Exception as ex:
            logger.warning(f"Can not delete the test sc : {ex}")
        # Delete the test project (namespace)
        self.delete_test_project()

        logger.info(f"Try to delete the Storage pool {self.pool_name}")
        try:
            self.delete_ceph_pool(self.pool_name)
        except Exception:
            pass
        finally:
            # Verify deletion by checking the backend CEPH pools using the toolbox
            if self.interface == constants.CEPHBLOCKPOOL:
                results = self.ceph_cluster.toolbox.exec_cmd_on_pod("ceph osd pool ls")
                logger.debug(f"Existing pools are : {results}")
                if self.pool_name in results.split():
                    logger.warning(
                        "The pool did not deleted by CSI, forcing delete it manually"
                    )
                    self.ceph_cluster.toolbox.exec_cmd_on_pod(
                        f"ceph osd pool delete {self.pool_name} {self.pool_name} "
                        "--yes-i-really-really-mean-it"
                    )
                else:
                    logger.info(f"The pool {self.pool_name} was deleted successfully")

        super(TestPVCClonePerformance, self).teardown()

    def create_new_pool_and_sc(self, secret_factory):
        self.pool_name = (
            f"pas-test-pool-{Interfaces_info[self.interface]['name'].lower()}"
        )
        secret = secret_factory(interface=self.interface)
        self.create_new_pool(self.pool_name)
        # Creating new StorageClass (pool) for the test.
        self.sc_obj = helpers.create_storage_class(
            interface_type=self.interface,
            interface_name=self.pool_name,
            secret_name=secret.name,
            sc_name=self.pool_name,
            fs_name=self.pool_name,
        )
        logger.info(f"The new SC is : {self.sc_obj.name}")

    def create_pvc_and_wait_for_bound(self):
        logger.info("Creating PVC to be cloned")
        try:
            self.pvc_obj = helpers.create_pvc(
                sc_name=self.sc_obj.name,
                pvc_name="pvc-pas-test",
                size=f"{self.pvc_size}Gi",
                namespace=self.namespace,
                access_mode=Interfaces_info[self.interface]["accessmode"],
            )
        except Exception as e:
            logger.exception(f"The PVC was not created, exception [{str(e)}]")
            raise PVCNotCreated("PVC did not reach BOUND state.")
        # Wait for the PVC to be Bound
        performance_lib.wait_for_resource_bulk_status(
            "pvc", 1, self.namespace, constants.STATUS_BOUND, self.timeout, 5
        )
        logger.info(f"The PVC {self.pvc_obj.name} was created and in Bound state.")

    def create_pod_and_wait_for_completion(self, **kwargs):
        # Creating pod yaml file to run as a Job, the command to run on the pod and
        # arguments to it will replace in the create_pod function
        self.create_fio_pod_yaml(
            pvc_size=int(self.pvc_size), filesize=kwargs.pop("filesize", "1M")
        )
        # Create a pod
        logger.info(f"Creating Pod with pvc {self.pvc_obj.name}")

        try:
            self.pod_object = helpers.create_pod(
                pvc_name=self.pvc_obj.name,
                namespace=self.namespace,
                interface_type=self.interface,
                pod_name="pod-pas-test",
                pod_dict_path=self.pod_yaml_file.name,
                **kwargs,
                # pod_dict_path=constants.PERF_POD_YAML,
            )
        except Exception as e:
            logger.exception(
                f"Pod attached to PVC {self.pod_object.name} was not created, exception [{str(e)}]"
            )
            raise PodNotCreated("Pod attached to PVC was not created.")

        # Confirm that pod is running on the selected_nodes
        logger.info("Checking whether the pod is running")
        helpers.wait_for_resource_state(
            resource=self.pod_object,
            state=constants.STATUS_COMPLETED,
            timeout=self.timeout,
        )

    def create_and_delete_clones(self):
        # Creating the clones one by one and wait until they bound
        logger.info(
            f"Start creating {self.number_of_clones} clones on {self.interface} PVC of size {self.pvc_size} GB."
        )
        clones_list = []
        for i in range(self.number_of_clones):
            index = i + 1
            logger.info(f"Start creation of clone number {index}.")
            cloned_pvc_obj = pvc.create_pvc_clone(
                sc_name=self.pvc_obj.backed_sc,
                parent_pvc=self.pvc_obj.name,
                pvc_name=f"clone-pas-test-{index}",
                clone_yaml=Interfaces_info[self.interface]["clone_yaml"],
                namespace=self.namespace,
                storage_size=self.pvc_size + "Gi",
            )
            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, self.timeout
            )
            # TODO: adding flattern for RBD devices
            cloned_pvc_obj.reload()
            clones_list.append(cloned_pvc_obj)
            logger.info(
                f"Clone with name {cloned_pvc_obj.name} for {self.pvc_size} pvc {self.pvc_obj.name} was created."
            )

        # Delete the clones one by one and wait for deletion
        logger.info(
            f"Start deleteing {self.number_of_clones} clones on {self.interface} PVC of size {self.pvc_size} GB."
        )
        index = 0
        for clone in clones_list:
            index += 1
            pvc_reclaim_policy = clone.reclaim_policy
            clone.delete()
            logger.info(
                f"Deletion of clone number {index} , the clone name is {clone.name}."
            )
            clone.ocp.wait_for_delete(clone.name, self.timeout)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(clone.backed_pv)

        return clones_list

    @pytest.mark.parametrize(
        argnames=["interface_type", "pvc_size", "file_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "1", "600Mi"],
                marks=pytest.mark.polarion_id("OCS-2356"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "25", "15Gi"],
                marks=pytest.mark.polarion_id("OCS-2340"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "50", "30Gi"],
                marks=pytest.mark.polarion_id("OCS-2357"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "100", "60Gi"],
                marks=pytest.mark.polarion_id("OCS-2358"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "1", "600Mi"],
                marks=pytest.mark.polarion_id("2341"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "25", "15Gi"],
                marks=pytest.mark.polarion_id("2355"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "50", "30Gi"],
                marks=pytest.mark.polarion_id("2359"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "100", "60Gi"],
                marks=pytest.mark.polarion_id("2360"),
            ),
        ],
    )
    def test_clone_create_delete_performance(
        self, secret_factory, interface_type, pvc_size, file_size
    ):
        """
        Write data (60% of PVC capacity) to the PVC created in setup
        Create clones for an existing pvc,
        Measure clones average creation time and speed
        Delete the created clone
        Measure clone average deletion time and speed
        Note: by increasing max_num_of_clones value you increase number of the clones to be created/deleted
        """

        # Initialize some variabels
        self.interface = interface_type
        self.timeout = 18000
        self.pvc_size = pvc_size
        self.results_path = get_full_test_logs_path(cname=self)
        file_size_mb = convert_device_size(file_size, "MB")
        # Initialize the results doc file.
        full_results = self.init_full_results(
            ClonesResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "pvc_clone_performance",
            )
        )

        test_start_time = self.get_time()

        # Create new pool and sc only for RBD, for CepgFS use thr default
        if self.interface == constants.CEPHBLOCKPOOL:
            # Creating new pool to run the test on it
            self.create_new_pool_and_sc(secret_factory)
        else:
            self.sc_obj = ocs.OCS(
                kind="StorageCluster",
                metadata={
                    "namespace": self.namespace,
                    "name": Interfaces_info[self.interface]["sc"],
                },
            )
            self.pool_name = "ocs-storagecluster-cephfilesystem"
        # Create a PVC
        self.create_pvc_and_wait_for_bound()
        # Create a POD
        self.create_pod_and_wait_for_completion(filesize=f"{file_size_mb}M")
        # taking the time, so parsing the provision log will be faster.
        start_time = self.get_time("csi")
        self.clones_list = self.create_and_delete_clones()

        # Mesure Creation / Deletion time of all clones
        results_times = performance_lib.get_pvc_provision_times(
            interface=self.interface,
            pvc_name=self.clones_list,
            start_time=start_time,
        )

        test_end_time = self.get_time()

        logger.info(
            f"Printing clone creation time and speed for {self.number_of_clones} clones "
            f"on {self.interface} PVC of size {self.pvc_size} GB:"
        )
        # Produce ES report
        speed = True if self.interface == constants.CEPHFILESYSTEM else False
        full_results.analyse_results(
            results_times, total_data=file_size_mb, speed=speed
        )
        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": test_start_time, "end": test_end_time}
        )
        full_results.add_key("total_clone_number", self.number_of_clones)
        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            logger.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (8 - according to the parameters)
            self.write_result_to_file(res_link)

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an FIOResultsAnalyse object

        Returns:
            FIOResultsAnalyse (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])
        full_results.add_key("interface", Interfaces_info[self.interface]["name"])
        full_results.add_key("pvc_size", self.pvc_size)
        return full_results

    @pytest.mark.parametrize(
        argnames=["interface", "copies", "timeout"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 7, 1800],
                marks=pytest.mark.polarion_id("OCS-2673"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 7, 1800],
                marks=[
                    pytest.mark.polarion_id("OCS-2674"),
                    pytest.mark.bugzilla("2101874"),
                ],
            ),
        ],
    )
    def test_pvc_clone_performance_multiple_files(
        self,
        secret_factory,
        interface,
        copies,
        timeout,
    ):
        """
        Test assign nodeName to a pod using RWX pvc
        Each kernel (unzipped) is 892M and 61694 files
        The test creates a pvc and a pods, writes kernel files multiplied by number of copies
        The test creates number of clones samples, calculates creation and deletion times for each one the clones
        and calculates the average creation and average deletion times
        """

        # Initialize some variabels
        self.interface = interface
        self.timeout = timeout
        self.pvc_size = "100"
        if self.dev_mode:
            self.pvc_size = "10"
            copies = 1
        self.results_path = get_full_test_logs_path(cname=self)
        # Initialize the results doc file.
        full_results = self.init_full_results(
            ClonesResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "test_pvc_clone_performance_multiple_files",
            )
        )
        files_written = ""
        data_written = ""

        test_start_time = self.get_time()

        # Create new pool and sc only for RBD, for CepgFS use thr default
        if self.interface == constants.CEPHBLOCKPOOL:
            # Creating new pool to run the test on it
            self.create_new_pool_and_sc(secret_factory)
        else:
            self.sc_obj = ocs.OCS(
                kind="StorageCluster",
                metadata={
                    "namespace": self.namespace,
                    "name": Interfaces_info[self.interface]["sc"],
                },
            )
            self.pool_name = "ocs-storagecluster-cephfilesystem"
        # Create a PVC
        self.create_pvc_and_wait_for_bound()
        # Create a POD
        self.create_pod_and_wait_for_completion(
            command=["/opt/multiple_files.sh"],
            command_args=[f"{copies}", "/mnt"],
        )

        # Get the number of files and total written data from the pod
        for line in self.pod_object.ocp.get_logs(name=self.pod_object.name).split("\n"):
            if "Number Of Files" in line:
                files_written = line.split(" ")[-1]
            if "Total Data" in line:
                data_written = line.split(" ")[-1]
        logger.info("Getting the amount of data written to the PVC")
        logger.info(f"The amount of written data is {data_written}")
        logger.info(
            f"For {self.interface} - The number of files written to the pod is {int(files_written):,}"
        )

        # increasing the timeout since clone creation time is longer than pod attach time
        self.timeout = 18000

        # taking the time, so parsing the provision log will be faster.
        start_time = self.get_time("csi")
        clones_list = self.create_and_delete_clones()

        # Mesure Creation / Deletion time of all clones
        results_times = performance_lib.get_pvc_provision_times(
            interface=self.interface,
            pvc_name=clones_list,
            start_time=start_time,
        )

        test_end_time = self.get_time()

        logger.info(
            f"Printing clone creation and deletion times for {self.number_of_clones} clones "
            f"on {self.interface} PVC of size {self.pvc_size} GB:"
        )
        # Produce ES report
        full_results.analyse_results(results_times, speed=False)
        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": test_start_time, "end": test_end_time}
        )
        full_results.add_key("clones_number", self.number_of_clones)
        full_results.add_key("files_number", files_written)
        full_results.all_results = results_times
        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            logger.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (2 - according to the parameters)
            self.results_path = get_full_test_logs_path(
                cname=self, fname="test_pvc_clone_performance_multiple_files"
            )
            self.write_result_to_file(res_link)

    def test_pvc_clone_performance_results(self):
        """
        This is not a test - it is only check that previous tests ran and finished as expected
        and reporting the full results (links in the ES) of previous tests (8 + 2)
        """
        # Define variables for the teardown phase
        self.interface = None
        self.pool_name = None

        self.add_test_to_results_check(
            test="test_clone_create_delete_performance",
            test_count=8,
            test_name="PVC Clone",
        )
        self.add_test_to_results_check(
            test="test_pvc_clone_performance_multiple_files",
            test_count=2,
            test_name="PVC Clone Multiple Files",
        )
        self.check_results_and_push_to_dashboard()
