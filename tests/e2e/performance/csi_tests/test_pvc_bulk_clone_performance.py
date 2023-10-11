"""
Test to measure pvc scale creation total and csi times. Total pvc count would be 50, 1 clone per PVC
Total number of clones in bulk will be 50
The results are uploaded to the ES server
"""
import logging
import re
import json

import pytest

from ocs_ci.utility import utils, templating
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import performance, performance_b
from ocs_ci.helpers import performance_lib
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    get_full_test_logs_path,
    get_provision_time,
)
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.perfresult import ResultsAnalyse

log = logging.getLogger(__name__)

Interfaces_info = {
    constants.CEPHBLOCKPOOL: {
        "name": "RBD",
        "sc_name": constants.DEFAULT_STORAGECLASS_RBD,
        "clone_yaml": constants.CSI_RBD_PVC_CLONE_YAML,
        "accessmode": constants.ACCESS_MODE_RWO,
    },
    constants.CEPHFILESYSTEM: {
        "name": "CephFS",
        "sc_name": constants.DEFAULT_STORAGECLASS_CEPHFS,
        "clone_yaml": constants.CSI_CEPHFS_PVC_CLONE_YAML,
        "accessmode": constants.ACCESS_MODE_RWX,
    },
}


@grey_squad
@performance
@performance_b
class TestBulkCloneCreation(PASTest):
    """
    Base class for bulk creation of PVC clones
    """

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestBulkCloneCreation, self).setup()
        self.benchmark_name = "pod_bulk_clone_creation_time"
        self.pvc_count = 50
        self.vol_size = "5Gi"
        self.pvc_size_int = 5
        self.file_size_mb = int(self.pvc_size_int * 0.6) * constants.GB2MB
        self.total_files_size = self.file_size_mb * self.pvc_count
        self.file_size_mb_str = str(self.file_size_mb) + "M"

        self.create_fio_pod_yaml(
            pvc_size=self.pvc_size_int, filesize=self.file_size_mb_str
        )

        self.create_test_project()

    def teardown(self):
        """
        Cleanup the test environment
        """
        log.info("Starting the test environment cleanup")

        # Delete the test project (namespace)
        self.delete_test_project()

        super(TestBulkCloneCreation, self).teardown()

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

    def attach_pvcs_to_pod_dict(self, pvc_list):
        """
        Function to construct pod.yaml with multiple PVC's each one of the PVC
        connected to different POD

        Args:
            pvc_list (list): list of PVCs to be attached to a pod

        Returns:
            pod_data (list): pods data with multiple PVC

        """
        pods_list = []
        for pvc_name in pvc_list:
            # Update pod yaml with required params
            pod_data = templating.load_yaml(self.pod_yaml_file.name)
            pod_data["metadata"]["namespace"] = self.namespace
            pod_data["metadata"]["name"] = create_unique_resource_name("perf", "pod")
            pod_data["spec"]["volumes"][0]["persistentVolumeClaim"][
                "claimName"
            ] = pvc_name
            pods_list.append(pod_data)

        return pods_list

    @pytest.mark.polarion_id("OCS-2621")
    def test_bulk_clone_performance(self, tmp_path, interface_iterate):
        """
        Creates number of PVCs in a bulk using kube job
        Write 60% of PVC capacity to each one of the created PVCs
        Creates 1 clone per each PVC altogether in a bulk
        Measuring total and csi creation times for bulk of clones

        """
        self.interface = interface_iterate
        job_pod_file, job_pvc_file, job_clone_file = [None, None, None]
        log.info(f"Start creating {self.interface} {self.pvc_count} PVC")

        try:
            pvc_dict_list = scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
                no_of_pvc=self.pvc_count,
                access_mode=Interfaces_info[self.interface]["accessmode"],
                sc_name=Interfaces_info[self.interface]["sc_name"],
                pvc_size=self.vol_size,
            )

            job_pvc_file = ObjectConfFile(
                name="job_profile_pvc",
                obj_dict_list=pvc_dict_list,
                project=self.namespace,
                tmp_path=tmp_path,
            )

            # Create kube_job
            job_pvc_file.create(namespace=self.namespace)

            # Check all the PVC reached Bound state
            performance_lib.wait_for_resource_bulk_status(
                resource="pvc",
                resource_count=self.pvc_count,
                namespace=self.namespace,
                status=constants.STATUS_BOUND,
                timeout=120,
                sleep_time=5,
            )
            log.info(
                f"All the PVCs ({self.pvc_count}) was created and are in Bound state"
            )

            # Getting the list of the PVC names
            pvc_bound_list = [
                p.name for p in pvc.get_all_pvc_objs(namespace=self.namespace)
            ]

            # Kube_job to Create pod
            log.info("Attaching PODs to the PVCs and filling them with data (60%)")
            pod_dict_list = self.attach_pvcs_to_pod_dict(pvc_bound_list)
            job_pod_file = ObjectConfFile(
                name="job_profile_pod",
                obj_dict_list=pod_dict_list,
                project=self.namespace,
                tmp_path=tmp_path,
            )

            log.debug(f"PODs data list is : {json.dumps(pod_dict_list, indent=3)}")

            job_pod_file.create(namespace=self.namespace)

            # Check all PODs are in Completed state
            performance_lib.wait_for_resource_bulk_status(
                resource="pod",
                resource_count=self.pvc_count,
                namespace=self.namespace,
                status=constants.STATUS_COMPLETED,
                timeout=5400,  # old_value=1200
                sleep_time=30,
            )
            log.info("All the PODs completed writing data to the PVC's")

            clone_dict_list = scale_lib.construct_pvc_clone_yaml_bulk_for_kube_job(
                pvc_dict_list,
                Interfaces_info[self.interface]["clone_yaml"],
                Interfaces_info[self.interface]["sc_name"],
            )

            log.info("Created clone dict list")

            csi_bulk_start_time = self.get_time(time_format="csi")

            job_clone_file = ObjectConfFile(
                name="job_profile_clone",
                obj_dict_list=clone_dict_list,
                project=self.namespace,
                tmp_path=tmp_path,
            )

            # Create kube_job that creates clones
            job_clone_file.create(namespace=self.namespace)

            log.info("Going to check bound status for clones")
            # Check all the clones reached Bound state
            try:
                performance_lib.wait_for_resource_bulk_status(
                    resource="pvc",
                    resource_count=self.pvc_count * 2,
                    namespace=self.namespace,
                    status=constants.STATUS_BOUND,
                    timeout=5400,  # old_value=1200
                    sleep_time=30,
                )
            except Exception as ex:
                log.error("Failed to cvreate clones for PVCs")
                raise ex

            log.info(
                f"All the Clones ({self.pvc_count}) was created and are in Bound state"
            )

            all_pvc_objs = pvc.get_all_pvc_objs(namespace=self.namespace)
            clone_objs = [cl for cl in all_pvc_objs if re.match("clone", cl.name)]
            for clone_yaml in clone_dict_list:
                name = clone_yaml["metadata"]["name"]
                size = clone_yaml["spec"]["resources"]["requests"]["storage"]
                log.info(f"Clone {name} of size {size} created")

            start_time = get_provision_time(self.interface, clone_objs, status="start")
            end_time = get_provision_time(self.interface, clone_objs, status="end")
            total_time = (end_time - start_time).total_seconds()
            speed = round(self.total_files_size / total_time, 2)

            csi_creation_time = performance_lib.csi_bulk_pvc_time_measure(
                self.interface, clone_objs, "create", csi_bulk_start_time
            )

            log.info(
                f"Total creation time = {total_time} secs, csi creation time = {csi_creation_time},"
                f" data size = {self.total_files_size} MB, speed = {speed} MB/sec "
                f"for {self.interface} clone in bulk of {self.pvc_count} clones."
            )

            # Produce ES report
            # Collecting environment information
            self.get_env_info()

            # Initialize the results' doc file.
            full_results = self.init_full_results(
                ResultsAnalyse(
                    self.uuid,
                    self.crd_data,
                    self.full_log_path,
                    "bulk_clone_perf_fullres",
                )
            )

            full_results.add_key("interface", self.interface)
            full_results.add_key("bulk_size", self.pvc_count)
            full_results.add_key("clone_size", self.vol_size)
            full_results.add_key("bulk_creation_time", total_time)
            full_results.add_key("bulk_csi_creation_time", csi_creation_time)
            full_results.add_key("data_size(MB)", self.total_files_size)
            full_results.add_key("speed", speed)
            full_results.add_key("es_results_link", full_results.results_link())

            # Write the test results into the ES server
            full_results.es_write()
            self.results_path = get_full_test_logs_path(cname=self)
            res_link = full_results.results_link()
            # write the ES link to the test results in the test log.
            log.info(f"The result can be found at : {res_link}")

            # Create text file with results of all subtest (3 - according to the parameters)
            self.write_result_to_file(res_link)

        # Finally, is used to clean up the resources created
        # Irrespective of try block pass/fail finally will be executed.
        finally:
            # Cleanup activities
            log.info("Cleanup of all the resources created during test execution")
            for object_file in [job_pod_file, job_clone_file, job_pvc_file]:
                if object_file:
                    object_file.delete(namespace=self.namespace)
                    try:
                        object_file.wait_for_delete(
                            resource_name=object_file.name, namespace=self.namespace
                        )
                    except Exception:
                        log.error(f"{object_file['name']} didnt deleted !")

            # Check ceph health status
            utils.ceph_health_check(tries=20)

    def test_bulk_clone_performance_results(self):
        """
        This is not a test - it only checks that previous test completed and finish
        as expected with reporting the full results (links in the ES) of previous 2 tests
        """
        self.add_test_to_results_check(
            test="test_bulk_clone_performance",
            test_count=2,
            test_name="Bulk Clone Creation",
        )
        self.check_results_and_push_to_dashboard()
