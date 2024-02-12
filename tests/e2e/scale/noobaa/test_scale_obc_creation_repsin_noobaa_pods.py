import logging
import pytest

from ocs_ci.ocs import constants, scale_noobaa_lib
from ocs_ci.framework.testlib import scale, E2ETest
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.framework.pytest_customization.marks import (
    on_prem_platform_required,
    mcg,
    rgw,
)

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def teardown(request):
    def finalizer():
        scale_noobaa_lib.cleanup(constants.OPENSHIFT_STORAGE_NAMESPACE)

    request.addfinalizer(finalizer)


@scale
class TestScaleOCBCreation(E2ETest):
    """
    OBC scale creation, creating up to max support number of OBCs.
    OBCs are created in the Multicloud Object Gateway and
    Ceph Object Gateway (RGW)

    """

    namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
    scale_obc_count = 1000
    # Will increase number of obc with i/o when issue is fixed
    # BZ https://bugzilla.redhat.com/show_bug.cgi?id=2010560
    scale_obc_count_io = 5
    num_obc_batch = 50

    @pytest.mark.parametrize(
        argnames=["pod_name", "sc_name"],
        argvalues=[
            pytest.param(
                *["noobaa-core", constants.NOOBAA_SC],
                marks=[
                    pytest.mark.polarion_id("OCS-2645"),
                    mcg,
                ],
            ),
            pytest.param(
                *["noobaa-db", constants.NOOBAA_SC],
                marks=[
                    pytest.mark.polarion_id("OCS-2646"),
                    mcg,
                ],
            ),
            pytest.param(
                *["noobaa-core", constants.DEFAULT_STORAGECLASS_RGW],
                marks=[
                    on_prem_platform_required,
                    pytest.mark.polarion_id("OCS-2647"),
                    rgw,
                ],
            ),
            pytest.param(
                *["noobaa-db", constants.DEFAULT_STORAGECLASS_RGW],
                marks=[
                    on_prem_platform_required,
                    pytest.mark.polarion_id("OCS-2648"),
                    rgw,
                ],
            ),
        ],
    )
    def test_scale_obc_creation_noobaa_pod_respin(
        self, tmp_path, pod_name, sc_name, mcg_job_factory
    ):
        """
        OBC creation using RGW storage class
        This test case only runs on vSphere cluster deployment
        """

        # Create OBCs with FIO running using mcg_job_factory()
        for i in range(self.scale_obc_count_io):
            exec(f"job{i} = mcg_job_factory()")

        log.info(
            f"Start creating  {self.scale_obc_count} "
            f"OBC in a batch of {self.num_obc_batch}"
        )
        for i in range(int(self.scale_obc_count / self.num_obc_batch)):
            obc_dict_list = (
                scale_noobaa_lib.construct_obc_creation_yaml_bulk_for_kube_job(
                    no_of_obc=self.num_obc_batch,
                    sc_name=sc_name,
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

            # Check all the OBCs reached Bound state
            obc_bound_list = (
                scale_noobaa_lib.check_all_obc_reached_bound_state_in_kube_job(
                    kube_job_obj=job_file,
                    namespace=self.namespace,
                    no_of_obc=self.num_obc_batch,
                )
            )
            log.info(f"Number of OBCs in Bound state: {len(obc_bound_list)}")

        # Reset node which noobaa pods is running on
        # And validate noobaa pods are re-spinned and in running state
        scale_noobaa_lib.noobaa_running_node_restart(pod_name=pod_name)

        # Verify all OBCs are in Bound state after node restart
        log.info("Verify all OBCs are in Bound state after node restart.....")
        obc_status_list = scale_noobaa_lib.check_all_obcs_status(
            namespace=self.namespace
        )
        log.info(
            f"Number of OBCs in Bound state after node reset: "
            f"{len(obc_status_list[0])}"
        )
        assert (
            len(obc_status_list[0]) == self.scale_obc_count
        ), "Not all OBCs in Bound state"
