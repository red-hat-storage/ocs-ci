"""
Tests to measure PVC clones creation performance ( time and speed)
The test is supposed to create the maximum number of clones for one PVC
"""

import logging
import statistics

import pytest

from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    performance,
    performance_b,
)
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.resources import ocs, pvc
from ocs_ci.helpers import helpers, performance_lib

log = logging.getLogger(__name__)

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

ERR_MSG = "Error in command"


@grey_squad
@performance
@performance_b
@skipif_ocp_version("<4.6")
@skipif_ocs_version("<4.6")
class TestPvcMultiClonePerformance(PASTest):
    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestPvcMultiClonePerformance, self).setup()
        self.benchmark_name = "pvc_multi_clone_performance"

        # Run the test in its own project (namespace)
        self.create_test_project()

        self.num_of_clones = 512

        # Getting the total Storage capacity
        self.ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        # Getting the free Storage capacity
        self.ceph_free_capacity = int(self.ceph_cluster.get_ceph_free_capacity())
        # Use 70% of the free storage capacity in the test
        self.capacity_to_use = int(self.ceph_free_capacity * 0.7)

        # since we do not want to use more then 65%, we add 35% to the needed
        # capacity, and minimum PVC size is 1 GiB
        self.need_capacity = int((self.num_of_clones + 2) * 1.35)
        # Test will run only on system with enough capacity
        if self.capacity_to_use < self.need_capacity:
            err_msg = (
                f"The system has only {self.ceph_capacity} GiB, "
                f"Of which {self.ceph_free_capacity} GiB is free, "
                f"we want to use  {self.capacity_to_use} GiB, "
                f"and we need {self.need_capacity} GiB to run the test"
            )
            log.error(err_msg)
            raise exceptions.StorageNotSufficientException(err_msg)

        # Calculating the PVC size in GiB
        self.pvc_size = int(self.capacity_to_use / (self.num_of_clones + 2))

        if self.dev_mode:
            self.num_of_clones = 10
            self.pvc_size = 3

        # Calculating the file size as 70% of the PVC size - in MB
        self.filesize = int(self.pvc_size * 0.70 * constants.GB2MB)
        # Change the file size to MB for the FIO function
        self.file_size = f"{self.filesize}M"

    def teardown(self):
        """
        Cleanup the test environment
        """

        # Deleting the namespace used by the test
        self.delete_test_project()

        if not self.teardown_needed:
            return

        log.info("Starting the test cleanup")

        # Delete The test POD
        self.cleanup_testing_pod()

        # Delete the created clones
        try:
            for clone in self.cloned_obj_list:
                performance_lib.run_oc_command(
                    cmd=f"delete pvc {clone}", namespace=self.namespace
                )
        except Exception:
            log.warning("Clones were not deleted")

        # Delete the test PVC
        self.cleanup_testing_pvc()

        # Delete the test StorageClass
        try:
            log.info(f"Deleting the test StorageClass : {self.sc_obj.name}")
            self.sc_obj.delete()
            log.info("Wait until the SC is deleted.")
            self.sc_obj.ocp.wait_for_delete(resource_name=self.sc_obj.name)
        except Exception as ex:
            log.error(f"Can not delete the test sc : {ex}")

        # Delete the test storage pool

        log.info(f"Try to delete the Storage pool {self.pool_name}")
        try:
            self.delete_ceph_pool(self.pool_name)
        except Exception:
            pass
        finally:
            if self.interface == constants.CEPHBLOCKPOOL:
                # Verify deletion by checking the backend CEPH pools using the toolbox
                results = self.ceph_cluster.toolbox.exec_cmd_on_pod("ceph osd pool ls")
                log.debug(f"Existing pools are : {results}")
                if self.pool_name in results.split():
                    log.warning(
                        "The pool did not deleted by CSI, forcing delete it manually"
                    )
                    self.ceph_cluster.toolbox.exec_cmd_on_pod(
                        f"ceph osd pool delete {self.pool_name} {self.pool_name} "
                        "--yes-i-really-really-mean-it"
                    )
                else:
                    log.info(f"The pool {self.pool_name} was deleted successfully")

        super(TestPvcMultiClonePerformance, self).teardown()

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
        log.info(f"The new SC is : {self.sc_obj.name}")

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an FIOResultsAnalyse object

        Returns:
            ResultsAnalyse (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])
        full_results.add_key("interface", self.interface)
        full_results.add_key("clones_num", self.num_of_clones)
        full_results.add_key("clone_size", self.pvc_size)
        return full_results

    @pytest.mark.polarion_id("OCS-2622")
    def test_pvc_multiple_clone_performance(
        self,
        interface_iterate,
        secret_factory,
    ):
        """
        1. Creating PVC
           PVC size is calculated in the test and depends on the storage capacity, but not less then 1 GiB
           it will use ~75% capacity of the Storage, Min storage capacity 1 TiB
        2. Fill the PVC with 70% of data
        3. Take a clone of the PVC and measure Total time and speed of creation of each clone
            by reading start creation and end creation times from relevant logs
        4. Measure CSI time for creation of each clone
        5. Repeat the previous steps number of times (maximal num_of_clones is 512)
        6. Print and push to the ES all the measured statistics for all the clones.

        Raises:
            StorageNotSufficientException: in case of not enough capacity on the cluster

        """

        self.teardown_needed = True
        log.info(
            f"Total capacity size is : {self.ceph_capacity} GiB, "
            f"Free capacity size is : {self.ceph_free_capacity} GiB, "
            f"Going to use {self.need_capacity} GiB, "
            f"With {self.num_of_clones} clones to {self.pvc_size} GiB PVC. "
            f"File size to be written is : {self.file_size} "
        )

        self.interface = interface_iterate

        # Create new pool and sc only for RBD, for CepgFS use the++ default
        if self.interface == constants.CEPHBLOCKPOOL:
            # Creating new pool to run the test on it
            self.create_new_pool_and_sc(secret_factory)
        else:
            # use the default ceph filesystem pool
            self.sc_obj = ocs.OCS(
                kind="StorageCluster",
                metadata={
                    "namespace": self.namespace,
                    "name": Interfaces_info[self.interface]["sc"],
                },
            )
            self.pool_name = "ocs-storagecluster-cephfilesystem"

        # Create a PVC
        self.create_testing_pvc_and_wait_for_bound()

        # Create a POD
        self.create_testing_pod_and_wait_for_completion(filesize=self.file_size)

        # Running the test
        creation_time_list, creation_speed_list, csi_creation_time_list = ([], [], [])
        self.cloned_obj_list = []
        for test_num in range(1, self.num_of_clones + 1):
            log.info(f"Starting test number {test_num}")
            try:
                cloned_obj, ct, csi_ct = self.create_clone(test_num)
            except Exception as e:
                log.error(f"Failed to create clone number {test_num} : [{e}]")
                break
            self.cloned_obj_list.append(cloned_obj)
            speed = self.filesize / ct
            creation_time_list.append(ct)
            creation_speed_list.append(speed)
            csi_creation_time_list.append(csi_ct)

        # Analyse the results and log the results
        for i, val in enumerate(self.cloned_obj_list):
            log.info(f"The Results for clone number {i+1} ({val}) :")
            log.info(f"  Creation time is     : {creation_time_list[i]:,.3f} secs.")
            log.info(f"  Csi Creation time is : {csi_creation_time_list[i]:,.3f} secs.")
            log.info(f"  Creation speed is    : {creation_speed_list[i]:,.3f} MB/sec.")

        average_creation_time = statistics.mean(creation_time_list)
        average_creation_speed = statistics.mean(creation_speed_list)
        average_csi_creation_time = statistics.mean(csi_creation_time_list)

        log.info("The Average results are :")
        log.info(f"  Average creation time is     : {average_creation_time:,.3f} secs.")
        log.info(
            f"  Average csi creation time is : {average_csi_creation_time:,.3f} secs."
        )
        log.info(
            f"  Average creation speed is    : {average_creation_speed:,.3f} MB/sec."
        )

        if len(self.cloned_obj_list) != self.num_of_clones:
            log.error("Not all clones created.")
            raise exceptions.BenchmarkTestFailed("Not all clones created.")

        self.results_path = get_full_test_logs_path(cname=self)
        # Produce ES report
        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "pvc_multiple_clone_measurement",
            )
        )

        full_results.add_key("multi_clone_creation_time", creation_time_list)
        full_results.add_key("multi_clone_creation_time_average", average_creation_time)
        full_results.add_key("multi_clone_creation_speed", creation_speed_list)
        full_results.add_key(
            "multi_clone_creation_speed_average", average_creation_speed
        )
        full_results.add_key("multi_clone_csi_creation_time", csi_creation_time_list)
        full_results.add_key(
            "multi_clone_csi_creation_time_average", average_csi_creation_time
        )

        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (4 - according to the parameters)
            self.write_result_to_file(res_link)

    def create_clone(self, clone_num):
        """
        Creating clone for pvc, measure the creation time

        Args:
            clone_num (int) the number of clones to create

        Returns:
            str: The created clone name
            int: the creation time of the clone (in secs.)
            int: the csi creation time of the clone (in secs.)

        """

        csi_start_time = self.get_time("csi")
        cloned_pvc_obj = pvc.create_pvc_clone(
            sc_name=self.pvc_obj.backed_sc,
            parent_pvc=self.pvc_obj.name,
            pvc_name=f"pvc-clone-pas-test-{clone_num}",
            clone_yaml=Interfaces_info[self.interface]["clone_yaml"],
            namespace=self.namespace,
            storage_size=f"{self.pvc_obj.size}Gi",
        )
        helpers.wait_for_resource_state(cloned_pvc_obj, constants.STATUS_BOUND, 600)
        cloned_pvc_obj.reload()
        clone_name = cloned_pvc_obj.name
        create_time = performance_lib.measure_pvc_creation_time(
            self.interface, clone_name, csi_start_time
        )
        csi_create_time = performance_lib.csi_pvc_time_measure(
            self.interface, cloned_pvc_obj, "create", csi_start_time
        )
        del cloned_pvc_obj
        return (clone_name, create_time, csi_create_time)

    def test_multi_clone_performance_results(self):
        """
        This is not a test - it is only check that previous tests ran and finished as expected
        and reporting the full results (links in the ES) of previous tests (2)
        """
        self.teardown_needed = False

        self.add_test_to_results_check(
            test="test_pvc_multiple_clone_performance",
            test_count=2,
            test_name="PVC Multi Clone",
        )
        self.check_results_and_push_to_dashboard()
