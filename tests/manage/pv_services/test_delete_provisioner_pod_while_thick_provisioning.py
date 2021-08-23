import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    tier4,
    tier4a,
    polarion_id,
    bugzilla,
    skipif_ocs_version,
    ignore_data_rebalance,
)
from ocs_ci.helpers.helpers import (
    verify_volume_deleted_in_backend,
    default_thick_storage_class,
    check_rbd_image_used_size,
    default_ceph_block_pool,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.helpers import helpers, disruption_helpers


logger = logging.getLogger(__name__)
DISRUPTION_OPS = disruption_helpers.Disruptions()


@tier4
@tier4a
@ignore_data_rebalance
@skipif_ocs_version("<=4.9")
class TestDeleteProvisionerPodWhileThickProvisioning(ManageTest):
    """
    Test to delete rbd provisioner leader pod while thick provisioning is progressing
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory):
        """
        Create Project for the test

        Returns:
            OCP: An OCP instance of project
        """
        self.proj_obj = project_factory()

    @polarion_id("OCS-2531")
    @bugzilla("1961647")
    def test_delete_provisioner_pod_while_thick_provisioning(
        self,
        pvc_factory,
        pod_factory,
    ):
        """
        Test to delete RBD provisioner leader pod while creating a PVC using thick provision enabled storage class
        """
        pvc_size = 20
        pool_name = default_ceph_block_pool()
        executor = ThreadPoolExecutor(max_workers=1)
        DISRUPTION_OPS.set_resource(
            resource="rbdplugin_provisioner", leader_type="provisioner"
        )

        # Start creation of PVC
        pvc_create = executor.submit(
            pvc_factory,
            interface=constants.CEPHBLOCKPOOL,
            project=self.proj_obj,
            storageclass=default_thick_storage_class(),
            size=pvc_size,
            access_mode=constants.ACCESS_MODE_RWO,
            status="",
        )

        # Ensure that the PVC is being created before deleting the rbd provisioner pod
        ret = helpers.wait_for_resource_count_change(
            get_all_pvcs, 0, self.proj_obj.namespace, "increase"
        )
        assert ret, "Wait timeout: PVC is not being created."
        logger.info("PVC creation has started.")
        DISRUPTION_OPS.delete_resource()
        logger.info("Deleted RBD provisioner leader pod.")

        pvc_obj = pvc_create.result()

        # Confirm that the PVC is Bound
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=600
        )
        pvc_obj.reload()
        logger.info(f"Verified: PVC {pvc_obj.name} reached Bound state.")
        image_name = pvc_obj.get_rbd_image_name
        pv_obj = pvc_obj.backed_pv_obj

        # Verify thick provision by checking the image used size
        assert check_rbd_image_used_size(
            pvc_objs=[pvc_obj],
            usage_to_compare=f"{pvc_size}GiB",
            rbd_pool=pool_name,
            expect_match=True,
        ), f"PVC {pvc_obj.name} is not thick provisioned.\n PV describe :\n {pv_obj.describe()}"
        logger.info("Verified: The PVC is thick provisioned")

        # Create pod and run IO
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        pod_obj.run_io(
            storage_type="fs",
            size=f"{pvc_size-1}G",
            fio_filename=f"{pod_obj.name}_io",
            end_fsync=1,
        )

        # Get IO result
        get_fio_rw_iops(pod_obj)

        logger.info(f"Deleting pod {pod_obj.name}")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        # Fetch image id for verification
        image_uid = pvc_obj.image_uuid

        logger.info(f"Deleting PVC {pvc_obj.name}")
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(pvc_obj.name), f"PVC {pvc_obj.name} is not deleted"
        logger.info(f"Verified: PVC {pvc_obj.name} is deleted.")
        pv_obj.ocp.wait_for_delete(pv_obj.name), f"PV {pv_obj.name} is not deleted"
        logger.info(f"Verified: PV {pv_obj.name} is deleted.")

        # Verify the rbd image is deleted
        logger.info(f"Wait for the RBD image {image_name} to get deleted")
        assert verify_volume_deleted_in_backend(
            interface=constants.CEPHBLOCKPOOL,
            image_uuid=image_uid,
            pool_name=pool_name,
            timeout=300,
        ), f"Wait timeout - RBD image {image_name} is not deleted"
        logger.info(f"Verified: RBD image {image_name} is deleted")
