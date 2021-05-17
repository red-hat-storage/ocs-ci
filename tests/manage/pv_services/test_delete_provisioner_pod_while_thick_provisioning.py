import logging
from concurrent.futures import ThreadPoolExecutor
import pytest
from functools import partial

from ocs_ci.framework.testlib import ManageTest, tier4, tier4a, polarion_id
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import verify_volume_deleted_in_backend
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.helpers import helpers, disruption_helpers


logger = logging.getLogger(__name__)

DISRUPTION_OPS = disruption_helpers.Disruptions()


@tier4
@tier4a
@polarion_id("")
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

    def test_delete_provisioner_pod_while_thick_provisioning(
        self,
        pvc_factory,
        pod_factory,
    ):
        """
        Test to delete rbd provisioner leader pod while creating a PVC using thick provision enabled storage class
        """
        pvc_size = 5

        DISRUPTION_OPS.set_resource(resource="rbdplugin_provisioner", leader_type="provisioner")


        executor = ThreadPoolExecutor()

        # Start creation of PVC
        pvc_create = executor.submit(
            pvc_factory,
            interface=constants.CEPHBLOCKPOOL,
            project=self.proj_obj,
            storageclass=None,
            size=pvc_size,
            access_mode=constants.ACCESS_MODE_RWO,
            status="",
        )

        # Ensure PVC is being created before deleting the resource
        ret = helpers.wait_for_resource_count_change(
            get_all_pvcs, 0, self.proj_obj.namespace, "increase"
        )
        assert ret, "Wait timeout: PVC is not being created."
        logger.info("PVC creation has started.")
        DISRUPTION_OPS.delete_resource()

        pvc_obj = pvc_create.result()

        # Confirm that the PVC is Bound
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=600
        )
        pvc_obj.reload()
        logger.info(f"Verified: PVC {pvc_obj.name} reached Bound state.")

        image_uid = pvc_obj.image_uuid

        # Create pod
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        pod_obj.run_io(
            storage_type="fs", size=f"{pvc_size-1}G", fio_filename=f"{pod_obj.name}_io"
        )

        get_fio_rw_iops(pod_obj)

        pod_obj.delete()

        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(
            pvc_obj.name
        ), f"PVC {pvc_obj.name} is not deleted"
        logger.info(f"Verified: PVC {pvc_obj.name} is deleted.")

        verify_volume_deleted_in_backend(
            interface=constants.CEPHBLOCKPOOL, image_uuid=image_uid, pool_name=constants.DEFAULT_BLOCKPOOL,
            timeout=300
        )




