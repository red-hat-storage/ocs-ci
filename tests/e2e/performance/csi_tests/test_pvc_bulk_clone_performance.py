"""
Test to measure pvc scale creation time. Total pvc count would be 50, 1 clone per PVC
Total number of clones in bulk will be 50
"""
import logging

import pytest

from ocs_ci.framework.testlib import performance, E2ETest
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile

log = logging.getLogger(__name__)


@performance
class TestBulkCloneCreation(E2ETest):
    """
    Base class for bulk creation of PVC clones
    """

    @pytest.fixture()
    def namespace(self, project_factory, interface_iterate):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace
        self.interface = interface_iterate

    @pytest.mark.usefixtures(namespace.__name__)
    @pytest.mark.polarion_id("OCS-2621")
    def test_bulk_clone_performance(self, namespace, tmp_path, pod_factory):
        """
        Creates number of PVCs in a bulk using kube job
        Write 60% of PVC capacity to each one of the created PVCs
        Creates 1 clone per each PVC altogether in a bulk
        Measuring time for bulk of clones creation

        """
        pvc_count = 50
        log.info(f"Start creating {self.interface} {pvc_count} PVC")
        if self.interface == constants.CEPHBLOCKPOOL:
            sc_name = constants.DEFAULT_STORAGECLASS_RBD
            clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        elif self.interface == constants.CEPHFILESYSTEM:
            sc_name = constants.DEFAULT_STORAGECLASS_CEPHFS
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML

        pvc_dict_list = scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=pvc_count,
            access_mode=constants.ACCESS_MODE_RWO,
            sc_name=sc_name,
            pvc_size="5Gi",
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

        logging.info(f"Number of PVCs in Bound state {len(pvc_bound_list)}")

        total_files_size = self.run_fio_on_pvcs(pvc_dict_list, pod_factory)

        clone_dict_list = scale_lib.construct_pvc_clone_yaml_bulk_for_kube_job(
            pvc_dict_list, clone_yaml, sc_name
        )

        logging.info("Created clone dict list")

        job_clone_file = ObjectConfFile(
            name="job_profile_clone",
            obj_dict_list=clone_dict_list,
            project=self.namespace,
            tmp_path=tmp_path,
        )

        # Create kube_job that creates clones
        job_clone_file.create(namespace=self.namespace)

        logging.info("Going to check bound status for clones")
        # Check all the clones reached Bound state
        clone_bound_list = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=job_clone_file,
            namespace=self.namespace,
            no_of_pvc=pvc_count,
            timeout=200,
        )

        logging.info(f"Number of clones in Bound state {len(clone_bound_list)}")

        clone_objs = []
        all_pvc_objs = pvc.get_all_pvc_objs(namespace=self.namespace)
        for clone_yaml in clone_dict_list:
            name = clone_yaml["metadata"]["name"]
            size = clone_yaml["spec"]["resources"]["requests"]["storage"]
            logging.info(f"Clone {name} of size {size} created")
            for pvc_obj in all_pvc_objs:
                if pvc_obj.name == name:
                    clone_objs.append(pvc_obj)

        assert len(clone_bound_list) == len(
            clone_objs
        ), "Not all clones reached BOUND state, cannot measure time"
        start_time = helpers.get_provision_time(
            self.interface, clone_objs, status="start"
        )
        end_time = helpers.get_provision_time(self.interface, clone_objs, status="end")
        total_time = (end_time - start_time).total_seconds()
        speed = round(total_files_size / total_time, 2)
        logging.info(
            f"Total creation time = {total_time} secs, data size = {total_files_size} MB, speed = {speed} MB/sec "
            f"for {self.interface} clone in bulk of {pvc_count} clones."
        )

    def run_fio_on_pvcs(self, pvc_dict_list, pod_factory):
        total_files_size = 0
        searched_pvc_objs = pvc.get_all_pvc_objs(namespace=self.namespace)
        logging.info(f"Found {len(searched_pvc_objs)} PVCs")
        for pvc_yaml in pvc_dict_list:
            pvc_name = pvc_yaml["metadata"]["name"]
            pvc_size = pvc_yaml["spec"]["resources"]["requests"]["storage"]
            logging.info(f"Size of pvc {pvc_name} is {pvc_size}")
            pvc_size_int = int(pvc_size[:-2])  # without "Gi"
            file_size_mb = int(pvc_size_int * 0.6) * constants.GB2MB
            total_files_size += file_size_mb
            file_size_mb_str = str(file_size_mb) + "M"
            logging.info(f"Writing file of size {file_size_mb_str}")

            # now find pvc_obj by name and create pod_obj to write to
            pvc_obj = None
            for obj in searched_pvc_objs:
                if obj.name == pvc_name:
                    pvc_obj = obj
                    searched_pvc_objs.remove(obj)
                    break
            assert pvc_obj is not None, f"Cannot find PVC with name {pvc_name}"

            logging.info(f"PVC with name {pvc_name} found")
            pod_obj = pod_factory(
                interface=self.interface, pvc=pvc_obj, status=constants.STATUS_RUNNING
            )

            performance_lib.write_fio_on_pod(pod_obj, file_size_mb_str)

        return total_files_size
