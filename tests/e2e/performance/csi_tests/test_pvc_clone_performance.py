"""
Test to verify clone creation and deletion performance for PVC with data written to it.
Performance is measured by collecting clone creation/deletion speed.
"""
import datetime
import logging
import pytest
import urllib.request
import os
import statistics

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import performance
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.utility.utils import convert_device_size
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.exceptions import PVCNotCreated, PodNotCreated
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@performance
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

        self.create_test_project()

    def teardown(self):
        """
        Cleanup the test environment
        """
        logger.info("Starting the test environment celanup")

        # Delete the test project (namespace)
        self.delete_test_project()

        super(TestPVCClonePerformance, self).teardown()

    @pytest.fixture()
    def base_setup(
        self, interface_type, pvc_size, pvc_factory, pod_factory, storageclass_factory
    ):
        """
        create resources for the test
        Args:
            interface_type(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            pvc_size: Size of the created PVC
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod

        """
        self.interface = interface_type
        self.pvc_size = pvc_size

        self.pvc_obj = pvc_factory(
            interface=interface_type, size=pvc_size, status=constants.STATUS_BOUND
        )

        self.pod_object = pod_factory(
            interface=interface_type, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )
        logger.info(f"pod object is : {self.pod_object}")
        logger.info(f"pod object is : {self.pod_object.name}")

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
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_clone_create_delete_performance(
        self, interface_type, pvc_size, file_size, teardown_factory
    ):
        """
        Write data (60% of PVC capacity) to the PVC created in setup
        Create clones for an existing pvc,
        Measure clones average creation time and speed
        Delete the created clone
        Measure clone average deletion time and speed
        Note: by increasing max_num_of_clones value you increase number of the clones to be created/deleted
        """

        file_size_for_io = file_size[:-1]

        performance_lib.write_fio_on_pod(self.pod_object, file_size_for_io)

        max_num_of_clones = 10
        if self.dev_mode:
            max_num_of_clones = 3

        clone_creation_measures = []
        csi_clone_creation_measures = []
        clones_list = []
        timeout = 18000
        sc_name = self.pvc_obj.backed_sc
        parent_pvc = self.pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        namespace = self.pvc_obj.namespace
        if interface_type == constants.CEPHFILESYSTEM:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
        file_size_mb = convert_device_size(file_size, "MB")

        logger.info(
            f"Start creating {max_num_of_clones} clones on {interface_type} PVC of size {pvc_size} GB."
        )

        # taking the time, so parsing the provision log will be faster.
        start_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        for i in range(max_num_of_clones):
            logger.info(f"Start creation of clone number {i + 1}.")
            cloned_pvc_obj = pvc.create_pvc_clone(
                sc_name, parent_pvc, clone_yaml, namespace, storage_size=pvc_size + "Gi"
            )
            teardown_factory(cloned_pvc_obj)
            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, timeout
            )

            cloned_pvc_obj.reload()
            logger.info(
                f"Clone with name {cloned_pvc_obj.name} for {pvc_size} pvc {parent_pvc} was created."
            )
            clones_list.append(cloned_pvc_obj)
            create_time = helpers.measure_pvc_creation_time(
                interface_type, cloned_pvc_obj.name
            )
            creation_speed = int(file_size_mb / create_time)
            logger.info(
                f"Clone number {i+1} creation time is {create_time} secs for {pvc_size} GB pvc."
            )
            logger.info(
                f"Clone number {i+1} creation speed is {creation_speed} MB/sec for {pvc_size} GB pvc."
            )
            creation_measures = {
                "clone_num": i + 1,
                "time": create_time,
                "speed": creation_speed,
            }
            clone_creation_measures.append(creation_measures)
            csi_clone_creation_measures.append(
                performance_lib.csi_pvc_time_measure(
                    self.interface, cloned_pvc_obj, "create", start_time
                )
            )

        # deleting one by one and measuring deletion times and speed for each one of the clones create above
        # in case of single clone will run one time
        clone_deletion_measures = []
        csi_clone_deletion_measures = []

        logger.info(
            f"Start deleting {max_num_of_clones} clones on {interface_type} PVC of size {pvc_size} GB."
        )

        for i in range(max_num_of_clones):
            cloned_pvc_obj = clones_list[i]
            pvc_reclaim_policy = cloned_pvc_obj.reclaim_policy
            cloned_pvc_obj.delete()
            logger.info(
                f"Deletion of clone number {i + 1} , the clone name is {cloned_pvc_obj.name}."
            )
            cloned_pvc_obj.ocp.wait_for_delete(cloned_pvc_obj.name, timeout)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(cloned_pvc_obj.backed_pv)
            delete_time = helpers.measure_pvc_deletion_time(
                interface_type, cloned_pvc_obj.backed_pv
            )
            logger.info(
                f"Clone number {i + 1} deletion time is {delete_time} secs for {pvc_size} GB pvc."
            )

            deletion_speed = int(file_size_mb / delete_time)
            logger.info(
                f"Clone number {i+1} deletion speed is {deletion_speed} MB/sec for {pvc_size} GB pvc."
            )
            deletion_measures = {
                "clone_num": i + 1,
                "time": delete_time,
                "speed": deletion_speed,
            }
            clone_deletion_measures.append(deletion_measures)
            csi_clone_deletion_measures.append(
                performance_lib.csi_pvc_time_measure(
                    self.interface, cloned_pvc_obj, "delete", start_time
                )
            )

        logger.info(
            f"Printing clone creation time and speed for {max_num_of_clones} clones "
            f"on {interface_type} PVC of size {pvc_size} GB:"
        )
        for c in clone_creation_measures:
            logger.info(
                f"Clone number {c['clone_num']} creation time is {c['time']} secs for {pvc_size} GB pvc ."
            )
            logger.info(
                f"Clone number {c['clone_num']} creation speed is {c['speed']} MB/sec for {pvc_size} GB pvc."
            )
        logger.info(
            f"Clone deletion time and speed for {interface_type} PVC of size {pvc_size} GB are:"
        )
        creation_time_list = [r["time"] for r in clone_creation_measures]
        creation_speed_list = [r["speed"] for r in clone_creation_measures]
        average_creation_time = statistics.mean(creation_time_list)
        average_csi_creation_time = statistics.mean(csi_clone_creation_measures)
        average_creation_speed = statistics.mean(creation_speed_list)
        logger.info(f"Average creation time is  {average_creation_time} secs.")
        logger.info(f"Average creation speed is  {average_creation_speed} Mb/sec.")

        for d in clone_deletion_measures:
            logger.info(
                f"Clone number {d['clone_num']} deletion time is {d['time']} secs for {pvc_size} GB pvc."
            )
            logger.info(
                f"Clone number {d['clone_num']} deletion speed is {d['speed']} MB/sec for {pvc_size} GB pvc."
            )

        deletion_time_list = [r["time"] for r in clone_deletion_measures]
        deletion_speed_list = [r["speed"] for r in clone_deletion_measures]
        average_deletion_time = statistics.mean(deletion_time_list)
        average_csi_deletion_time = statistics.mean(csi_clone_deletion_measures)
        average_deletion_speed = statistics.mean(deletion_speed_list)
        logger.info(f"Average deletion time is  {average_deletion_time} secs.")
        logger.info(f"Average deletion speed is  {average_deletion_speed} Mb/sec.")
        logger.info("test_clones_creation_performance finished successfully.")

        self.results_path = get_full_test_logs_path(cname=self)
        # Produce ES report
        # Collecting environment information
        self.get_env_info()

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.results_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{self.interface}-{pvc_size}-{file_size}"
        logger.info(f"Logs file path name is : {self.full_log_path}")

        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "pvc_clone_performance",
            )
        )

        full_results.add_key("interface", self.interface)
        full_results.add_key("total_clone_number", max_num_of_clones)
        full_results.add_key("pvc_size", self.pvc_size)
        full_results.add_key("average_clone_creation_time", average_creation_time)
        full_results.add_key(
            "average_csi_clone_creation_time", average_csi_creation_time
        )
        full_results.add_key("average_clone_deletion_time", average_deletion_time)
        full_results.add_key(
            "average_csi_clone_deletion_time", average_csi_deletion_time
        )
        full_results.add_key("average_clone_creation_speed", average_creation_speed)
        full_results.add_key("average_clone_deletion_speed", average_deletion_speed)

        full_results.all_results = {
            "clone_creation_time": creation_time_list,
            "csi_clone_creation_time": csi_clone_creation_measures,
            "clone_deletion_time": deletion_time_list,
            "csi_clone_deletion_time": csi_clone_deletion_measures,
            "clone_creation_speed": creation_speed_list,
            "clone_deletion_speed": deletion_speed_list,
        }

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
        full_results.add_key("index", full_results.new_index)
        return full_results

    @pytest.fixture()
    def base_multiple_setup(self, interface):
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
        argnames=["interface", "copies", "timeout"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 13, 1800],
                marks=pytest.mark.polarion_id("OCS-2673"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 13, 1800],
                marks=pytest.mark.polarion_id("OCS-2674"),
            ),
        ],
    )
    @pytest.mark.usefixtures(base_multiple_setup.__name__)
    def test_pvc_clone_performance_multiple_files(
        self,
        pvc_factory,
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
        kernel_url = "https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-4.19.5.tar.gz"
        download_path = "tmp"

        test_start_time = self.get_time()
        helpers.pull_images(constants.PERF_IMAGE)
        # Download a linux Kernel

        dir_path = os.path.join(os.getcwd(), download_path)
        file_path = os.path.join(dir_path, "file.gz")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        urllib.request.urlretrieve(kernel_url, file_path)

        # Create a PVC
        accessmode = constants.ACCESS_MODE_RWX
        if interface == constants.CEPHBLOCKPOOL:
            accessmode = constants.ACCESS_MODE_RWO

        pvc_size = "100"
        try:
            pvc_obj = pvc_factory(
                interface=interface,
                access_mode=accessmode,
                status=constants.STATUS_BOUND,
                size=pvc_size,
            )
        except Exception as e:
            logger.error(f"The PVC sample was not created, exception {str(e)}")
            raise PVCNotCreated("PVC did not reach BOUND state.")

        # Create a pod on one node
        logger.info(f"Creating Pod with pvc {pvc_obj.name}")

        try:
            pod_obj = helpers.create_pod(
                interface_type=interface,
                pvc_name=pvc_obj.name,
                namespace=pvc_obj.namespace,
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
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=timeout
        )

        pod_name = pod_obj.name
        pod_path = "/mnt"

        _ocp = OCP(namespace=pvc_obj.namespace)

        rsh_cmd = f"rsync {dir_path} {pod_name}:{pod_path}"
        _ocp.exec_oc_cmd(rsh_cmd)

        rsh_cmd = f"exec {pod_name} -- tar xvf {pod_path}/tmp/file.gz -C {pod_path}/tmp"
        _ocp.exec_oc_cmd(rsh_cmd)

        for x in range(copies):
            rsh_cmd = f"exec {pod_name} -- mkdir -p {pod_path}/folder{x}"
            _ocp.exec_oc_cmd(rsh_cmd)
            rsh_cmd = f"exec {pod_name} -- cp -r {pod_path}/tmp {pod_path}/folder{x}"
            _ocp.exec_oc_cmd(rsh_cmd)
            rsh_cmd = f"exec {pod_name} -- sync"
            _ocp.exec_oc_cmd(rsh_cmd)

        logger.info("Getting the amount of data written to the PVC")
        rsh_cmd = f"exec {pod_name} -- df -h {pod_path}"
        data_written = _ocp.exec_oc_cmd(rsh_cmd).split()[-4]
        logger.info(f"The amount of written data is {data_written}")

        rsh_cmd = f"exec {pod_name} -- find {pod_path} -type f"
        files_written = len(_ocp.exec_oc_cmd(rsh_cmd).split())
        logger.info(
            f"For {interface} - The number of files written to the pod is {files_written}"
        )

        # delete the pod
        pod_obj.delete(wait=False)

        logger.info("Wait for the pod to be deleted")
        performance_lib.wait_for_resource_bulk_status(
            "pod", 0, pvc_obj.namespace, constants.STATUS_COMPLETED, timeout, 5
        )
        logger.info("The pod was deleted")

        num_of_clones = 11
        # increasing the timeout since clone creation time is longer than pod attach time
        timeout = 18000

        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        if interface == constants.CEPHFILESYSTEM:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML

        clone_creation_measures = []
        csi_clone_creation_measures = []
        clone_deletion_measures = []
        csi_clone_deletion_measures = []

        # taking the time, so parsing the provision log will be faster.
        start_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        for i in range(num_of_clones):
            logger.info(f"Start creation of clone number {i + 1}.")
            cloned_pvc_obj = pvc.create_pvc_clone(
                pvc_obj.backed_sc,
                pvc_obj.name,
                clone_yaml,
                pvc_obj.namespace,
                storage_size=pvc_size + "Gi",
            )
            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, timeout
            )

            cloned_pvc_obj.reload()
            logger.info(
                f"Clone with name {cloned_pvc_obj.name} for {pvc_size} pvc {pvc_obj.name} was created."
            )
            create_time = helpers.measure_pvc_creation_time(
                interface, cloned_pvc_obj.name
            )
            logger.info(
                f"Clone number {i+1} creation time is {create_time} secs for {pvc_size} GB pvc."
            )
            clone_creation_measures.append(create_time)
            csi_clone_creation_measures.append(
                performance_lib.csi_pvc_time_measure(
                    interface, cloned_pvc_obj, "create", start_time
                )
            )

            pvc_reclaim_policy = cloned_pvc_obj.reclaim_policy
            cloned_pvc_obj.delete()
            logger.info(
                f"Deletion of clone number {i + 1} , the clone name is {cloned_pvc_obj.name}."
            )
            cloned_pvc_obj.ocp.wait_for_delete(cloned_pvc_obj.name, timeout)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(cloned_pvc_obj.backed_pv)
            delete_time = helpers.measure_pvc_deletion_time(
                interface, cloned_pvc_obj.backed_pv
            )
            logger.info(
                f"Clone number {i + 1} deletion time is {delete_time} secs for {pvc_size} GB pvc."
            )

            clone_deletion_measures.append(delete_time)
            csi_clone_deletion_measures.append(
                performance_lib.csi_pvc_time_measure(
                    interface, cloned_pvc_obj, "delete", start_time
                )
            )

        os.remove(file_path)
        os.rmdir(dir_path)
        pvc_obj.delete()

        average_creation_time = statistics.mean(clone_creation_measures)
        logger.info(f"Average creation time is  {average_creation_time} secs.")
        average_csi_creation_time = statistics.mean(csi_clone_creation_measures)
        logger.info(f"Average csi creation time is  {average_csi_creation_time} secs.")

        average_deletion_time = statistics.mean(clone_deletion_measures)
        logger.info(f"Average deletion time is  {average_deletion_time} secs.")
        average_csi_deletion_time = statistics.mean(csi_clone_deletion_measures)
        logger.info(f"Average csi deletion time is  {average_csi_deletion_time} secs.")

        # Produce ES report

        # Collecting environment information
        self.get_env_info()
        self.results_path = get_full_test_logs_path(cname=self)
        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "test_pvc_clone_performance_multiple_files_fullres",
            )
        )

        full_results.add_key("files_number", files_written)

        test_end_time = self.get_time()

        full_results.add_key(
            "test_time", {"start": test_start_time, "end": test_end_time}
        )

        full_results.add_key("interface", interface)
        full_results.add_key("clones_number", num_of_clones)
        full_results.add_key("pvc_size", pvc_size)
        full_results.add_key("average_clone_creation_time", average_creation_time)
        full_results.add_key(
            "average_csi_clone_creation_time", average_csi_creation_time
        )
        full_results.add_key("average_clone_deletion_time", average_deletion_time)
        full_results.add_key(
            "average_csi_clone_deletion_time", average_csi_deletion_time
        )

        full_results.all_results = {
            "clone_creation_time": clone_creation_measures,
            "csi_clone_creation_time": csi_clone_creation_measures,
            "clone_deletion_time": clone_deletion_measures,
            "csi_clone_deletion_time": csi_clone_deletion_measures,
        }

        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            logger.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (4 - according to the parameters)
            self.results_path = get_full_test_logs_path(
                cname=self, fname="test_pvc_clone_performance_multiple_files"
            )
            self.write_result_to_file(res_link)

    def test_pvc_clone_performance_results(self):
        """
        This is not a test - it is only check that previous tests ran and finished as expected
        and reporting the full results (links in the ES) of previous tests (8 + 2)
        """

        workloads = [
            {
                "name": "test_clone_create_delete_performance",
                "tests": 8,
                "test_name": "PVC Clone Performance",
            },
            {
                "name": "test_pvc_clone_performance_multiple_files",
                "tests": 2,
                "test_name": "PVC Clone Multiple Files Performance",
            },
        ]
        for wl in workloads:
            self.number_of_tests = wl["tests"]
            self.results_path = get_full_test_logs_path(cname=self, fname=wl["name"])
            self.results_file = os.path.join(self.results_path, "all_results.txt")
            logger.info(f"Check results for [{wl['name']}] in : {self.results_file}")
            self.check_tests_results()
            self.push_to_dashboard(test_name=wl["test_name"])
