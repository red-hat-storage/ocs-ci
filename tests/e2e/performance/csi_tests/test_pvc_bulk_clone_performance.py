"""
Test to measure pvc scale creation total and csi times. Total pvc count would be 50, 1 clone per PVC
Total number of clones in bulk will be 50
The results are uploaded to the ES server
"""
import logging
import pytest
import os
from uuid import uuid4

from ocs_ci.utility import utils
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.framework.testlib import performance
from ocs_ci.framework import config
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.ocs.resources import pvc, pod
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.perfresult import ResultsAnalyse

log = logging.getLogger(__name__)


@performance
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
    def namespace(self, project_factory, interface_iterate):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace
        self.interface = interface_iterate

        if self.interface == constants.CEPHFILESYSTEM:
            sc = "CephFS"
        if self.interface == constants.CEPHBLOCKPOOL:
            sc = "RBD"

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{sc}"

    @pytest.mark.usefixtures(namespace.__name__)
    @pytest.mark.polarion_id("OCS-2621")
    def test_bulk_clone_performance(self, namespace, tmp_path):
        """
        Creates number of PVCs in a bulk using kube job
        Write 60% of PVC capacity to each one of the created PVCs
        Creates 1 clone per each PVC altogether in a bulk
        Measuring total and csi creation times for bulk of clones

        """
        pvc_count = 50
        vol_size = "5Gi"
        job_pod_file, job_pvc_file, job_clone_file = [None, None, None]
        log.info(f"Start creating {self.interface} {pvc_count} PVC")
        if self.interface == constants.CEPHBLOCKPOOL:
            sc_name = constants.DEFAULT_STORAGECLASS_RBD
            clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        elif self.interface == constants.CEPHFILESYSTEM:
            sc_name = constants.DEFAULT_STORAGECLASS_CEPHFS
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML

        try:
            pvc_dict_list = scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
                no_of_pvc=pvc_count,
                access_mode=constants.ACCESS_MODE_RWO,
                sc_name=sc_name,
                pvc_size=vol_size,
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
            pvc_bound_list = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
                kube_job_obj=job_pvc_file,
                namespace=self.namespace,
                no_of_pvc=pvc_count,
            )

            log.info(f"Number of PVCs in Bound state {len(pvc_bound_list)}")

            # Kube_job to Create pod
            pod_dict_list = scale_lib.attach_multiple_pvc_to_pod_dict(
                pvc_list=pvc_bound_list,
                namespace=self.namespace,
                pvcs_per_pod=1,
                start_io=False,
                pod_yaml=constants.NGINX_POD_YAML,
            )
            job_pod_file = ObjectConfFile(
                name="job_profile_pod",
                obj_dict_list=pod_dict_list,
                project=self.namespace,
                tmp_path=tmp_path,
            )
            job_pod_file.create(namespace=self.namespace)

            # Check all PODs in Running state
            scale_lib.check_all_pod_reached_running_state_in_kube_job(
                kube_job_obj=job_pod_file,
                namespace=self.namespace,
                no_of_pod=len(pod_dict_list),
                timeout=90,
            )
            log.info(f"Number of PODs in Running state {len(pod_dict_list)}")

            total_files_size = self.run_fio_on_pvcs(vol_size)

            clone_dict_list = scale_lib.construct_pvc_clone_yaml_bulk_for_kube_job(
                pvc_dict_list, clone_yaml, sc_name
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
            clone_bound_list = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
                kube_job_obj=job_clone_file,
                namespace=self.namespace,
                no_of_pvc=pvc_count,
                timeout=180,
            )

            log.info(f"Number of clones in Bound state {len(clone_bound_list)}")

            clone_objs = []
            all_pvc_objs = pvc.get_all_pvc_objs(namespace=self.namespace)
            for clone_yaml in clone_dict_list:
                name = clone_yaml["metadata"]["name"]
                size = clone_yaml["spec"]["resources"]["requests"]["storage"]
                log.info(f"Clone {name} of size {size} created")
                for pvc_obj in all_pvc_objs:
                    if pvc_obj.name == name:
                        clone_objs.append(pvc_obj)

            assert len(clone_bound_list) == len(
                clone_objs
            ), "Not all clones reached BOUND state, cannot measure time"
            start_time = helpers.get_provision_time(
                self.interface, clone_objs, status="start"
            )
            end_time = helpers.get_provision_time(
                self.interface, clone_objs, status="end"
            )
            total_time = (end_time - start_time).total_seconds()
            speed = round(total_files_size / total_time, 2)

            csi_creation_time = performance_lib.csi_bulk_pvc_time_measure(
                self.interface, clone_objs, "create", csi_bulk_start_time
            )

            log.info(
                f"Total creation time = {total_time} secs, csi creation time = {csi_creation_time},"
                f" data size = {total_files_size} MB, speed = {speed} MB/sec "
                f"for {self.interface} clone in bulk of {pvc_count} clones."
            )

            # Produce ES report
            # Collecting environment information
            self.get_env_info()

            # Initialize the results doc file.
            full_results = self.init_full_results(
                ResultsAnalyse(
                    self.uuid,
                    self.crd_data,
                    self.full_log_path,
                    "bulk_clone_perf_fullres",
                )
            )

            full_results.add_key("interface", self.interface)
            full_results.add_key("bulk_size", pvc_count)
            full_results.add_key("clone_size", vol_size)
            full_results.add_key("bulk_creation_time", total_time)
            full_results.add_key("bulk_csi_creation_time", csi_creation_time)
            full_results.add_key("data_size(MB)", total_files_size)
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

        # Finally is used to clean-up the resources created
        # Irrespective of try block pass/fail finally will be executed.
        finally:
            # Cleanup activities
            log.info("Cleanup of all the resources created during test execution")
            if job_pod_file:
                job_pod_file.delete(namespace=self.namespace)
                job_pod_file.wait_for_delete(
                    resource_name=job_pod_file.name, namespace=self.namespace
                )

            if job_clone_file:
                job_clone_file.delete(namespace=self.namespace)
                job_clone_file.wait_for_delete(
                    resource_name=job_clone_file.name, namespace=self.namespace
                )

            if job_pvc_file:
                job_pvc_file.delete(namespace=self.namespace)
                job_pvc_file.wait_for_delete(
                    resource_name=job_pvc_file.name, namespace=self.namespace
                )

            # Check ceph health status
            utils.ceph_health_check(tries=20)

    def run_fio_on_pvcs(self, pvc_size):
        searched_pvc_objs = pvc.get_all_pvc_objs(namespace=self.namespace)
        pod_objs = pod.get_all_pods(namespace=self.namespace)
        log.info(f"Found {len(searched_pvc_objs)} PVCs")
        pvc_size_int = int(pvc_size[:-2])  # without "Gi"
        file_size_mb = int(pvc_size_int * 0.6) * constants.GB2MB
        total_files_size = file_size_mb * len(searched_pvc_objs)
        file_size_mb_str = str(file_size_mb) + "M"
        log.info(f"Writing file of size {file_size_mb_str} in each PVC")

        for objs in pod_objs:
            performance_lib.write_fio_on_pod(objs, file_size_mb_str)

        return total_files_size

    def test_bulk_clone_performance_results(self):
        """
        This is not a test - it only check that previous test completed and finish
        as expected with reporting the full results (links in the ES) of previous 2 tests
        """
        self.number_of_tests = 2
        results_path = get_full_test_logs_path(
            cname=self, fname="test_bulk_clone_performance"
        )
        self.results_file = os.path.join(results_path, "all_results.txt")
        log.info(f"Check results in {self.results_file}.")
        self.check_tests_results()

        self.push_to_dashboard(test_name="Bulk Clone Creation")
