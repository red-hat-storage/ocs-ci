import logging
import pytest
import time

from ocs_ci.ocs import constants, scale_noobaa_lib
from ocs_ci.framework import config
from ocs_ci.framework.testlib import scale, E2ETest
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.framework.pytest_customization.marks import (
    vsphere_platform_required,
    orange_squad,
)

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def teardown(request):
    def finalizer():
        scale_noobaa_lib.cleanup(config.ENV_DATA["cluster_namespace"])

    request.addfinalizer(finalizer)


@orange_squad
@scale
class TestScaleOCBCreation(E2ETest):
    """
    OBC scale creation, creating up to max support number of OBCs.
    OBCs are created in the Multicloud Object Gateway and
    Ceph Object Gateway (RGW)

    """

    namespace = config.ENV_DATA["cluster_namespace"]
    sc_name = constants.NOOBAA_SC
    sc_rgw_name = constants.DEFAULT_STORAGECLASS_RGW
    scale_obc_count = 1000
    num_obc_batch = 50

    @pytest.mark.polarion_id("OCS-2478")
    def test_scale_mcg_obc_creation(self, tmp_path, timeout=60):
        """
        MCG OBC creation using Noobaa storage class
        """

        log.info(
            f"Start creating  {self.scale_obc_count} "
            f"OBC in a batch of {self.num_obc_batch}"
        )
        for i in range(int(self.scale_obc_count / self.num_obc_batch)):
            obc_dict_list = (
                scale_noobaa_lib.construct_obc_creation_yaml_bulk_for_kube_job(
                    no_of_obc=self.num_obc_batch,
                    sc_name=self.sc_name,
                    namespace=self.namespace,
                )
            )
            # Create job profile
            job_file = ObjectConfFile(
                name="job_profile",
                obj_dict_list=obc_dict_list,
                project=self.namespace,
                tmp_path=tmp_path,
            )
            # Create kube_job
            job_file.create(namespace=self.namespace)
            time.sleep(timeout * 5)

            # Check all the OBC reached Bound state
            obc_bound_list = (
                scale_noobaa_lib.check_all_obc_reached_bound_state_in_kube_job(
                    kube_job_obj=job_file,
                    namespace=self.namespace,
                    no_of_obc=self.num_obc_batch,
                )
            )
            log.info(f"Number of OBCs in Bound state {len(obc_bound_list)}")

    @vsphere_platform_required
    @pytest.mark.polarion_id("OCS-2479")
    def test_scale_rgw_obc_creation(self, tmp_path, timeout=60):
        """
        OBC creation using RGW storage class
        This test case only runs on vSphere cluster deployment
        """

        log.info(
            f"Start creating  {self.scale_obc_count} "
            f"OBC in a batch of {self.num_obc_batch}"
        )
        for i in range(int(self.scale_obc_count / self.num_obc_batch)):
            obc_dict_list = (
                scale_noobaa_lib.construct_obc_creation_yaml_bulk_for_kube_job(
                    no_of_obc=self.num_obc_batch,
                    sc_name=self.sc_rgw_name,
                    namespace=self.namespace,
                )
            )
            # Create job profile
            job_file = ObjectConfFile(
                name="job_profile",
                obj_dict_list=obc_dict_list,
                project=self.namespace,
                tmp_path=tmp_path,
            )
            # Create kube_job
            job_file.create(namespace=self.namespace)
            time.sleep(timeout * 5)

            # Check all the OBC reached Bound state
            obc_bound_list = (
                scale_noobaa_lib.check_all_obc_reached_bound_state_in_kube_job(
                    kube_job_obj=job_file,
                    namespace=self.namespace,
                    no_of_obc=self.num_obc_batch,
                )
            )
            log.info(f"Number of OBCs in Bound state {len(obc_bound_list)}")

    @vsphere_platform_required
    @pytest.mark.polarion_id("OCS-2480")
    def test_scale_mcg_rgw_obc_creation(self, tmp_path, timeout=60):
        """
        OBC creation for both MCG and RGW storage class
        This test case only runs on vSphere cluster deployment
        """

        log.info(
            f"Start creating  {self.scale_obc_count} OBC in a batch of {self.num_obc_batch}"
        )
        for i in range(int(self.scale_obc_count / self.num_obc_batch)):
            obc_dict_list1 = (
                scale_noobaa_lib.construct_obc_creation_yaml_bulk_for_kube_job(
                    no_of_obc=int(self.num_obc_batch / 2),
                    sc_name=self.sc_name,
                    namespace=self.namespace,
                )
            )
            obc_dict_list2 = (
                scale_noobaa_lib.construct_obc_creation_yaml_bulk_for_kube_job(
                    no_of_obc=int(self.num_obc_batch / 2),
                    sc_name=self.sc_rgw_name,
                    namespace=self.namespace,
                )
            )
            # Create job profile
            job_file1 = ObjectConfFile(
                name="job_profile1",
                obj_dict_list=obc_dict_list1,
                project=self.namespace,
                tmp_path=tmp_path,
            )
            job_file2 = ObjectConfFile(
                name="job_profile2",
                obj_dict_list=obc_dict_list2,
                project=self.namespace,
                tmp_path=tmp_path,
            )
            # Create kube_job
            job_file1.create(namespace=self.namespace)
            time.sleep(timeout * 3)
            job_file2.create(namespace=self.namespace)
            time.sleep(timeout * 3)

            # Check all the OBC reached Bound state
            obc_mcg_bound_list = (
                scale_noobaa_lib.check_all_obc_reached_bound_state_in_kube_job(
                    kube_job_obj=job_file1,
                    namespace=self.namespace,
                    no_of_obc=int(self.num_obc_batch / 2),
                )
            )
            obc_rgw_bound_list = (
                scale_noobaa_lib.check_all_obc_reached_bound_state_in_kube_job(
                    kube_job_obj=job_file2,
                    namespace=self.namespace,
                    no_of_obc=int(self.num_obc_batch / 2),
                )
            )
            log.info(
                f"Number of OBCs in Bound state MCG: {len(obc_mcg_bound_list)},"
                f" RGW: {len(obc_rgw_bound_list)}"
            )
