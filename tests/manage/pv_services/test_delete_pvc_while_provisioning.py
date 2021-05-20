import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.framework.testlib import ManageTest, tier2, tier4, tier4a, polarion_id, bugzilla
from ocs_ci.helpers.helpers import (
    verify_volume_deleted_in_backend,
    default_thick_storage_class,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops, get_ceph_tools_pod
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.helpers import helpers, disruption_helpers


log = logging.getLogger(__name__)
DISRUPTION_OPS = disruption_helpers.Disruptions()


class TestDeletePvcWhileProvisioning(ManageTest):
    """
    Tests to verify that deleting a PVC while provisioning will not create any stale volume
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory):
        """
        Create Project for the test
        Returns:
            OCP: An OCP instance of project
        """
        self.proj_obj = project_factory()

    @tier4
    @tier4a
    @polarion_id("")
    def test_delete_pvc_while_thick_provisioning(
        self,
        pvc_factory,
        pod_factory,
    ):
        """
        Test to delete RBD PVC and RBD provisioner leader pod while thick provisioning
        is progressing and verify that no stale image is present
        """
        pvc_size = 30
        executor = ThreadPoolExecutor(max_workers=3)
        DISRUPTION_OPS.set_resource(
            resource="rbdplugin_provisioner", leader_type="provisioner"
        )

        ct_pod = get_ceph_tools_pod()

        # Collect the list of RBD images
        image_list_out_initial = ct_pod.exec_ceph_cmd(ceph_cmd=f"rbd ls -p {constants.DEFAULT_BLOCKPOOL}", format="")
        image_list_initial = image_list_out_initial.strip().split()
        log.info(f"List of RBD images before creating the PVC {image_list_initial}")

        # Start creation of PVC
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.proj_obj,
            storageclass=default_thick_storage_class(),
            size=pvc_size,
            access_mode=constants.ACCESS_MODE_RWO,
            status="",
        )

        # Ensure that the PVC is being created before deleting the rbd provisioner leader pod and the PVC itself
        ret = helpers.wait_for_resource_count_change(
            get_all_pvcs, 0, self.proj_obj.namespace, "increase"
        )
        assert ret, "Wait timeout: PVC is not being created."
        log.info("PVC creation has started.")

        log.info("Deleting RBD provisioner leader pod.")
        delete_provisioner = executor.submit(DISRUPTION_OPS.delete_resource)

        # Delete PVC
        log.info(f"Deleting PVC {pvc_obj.name}")
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(pvc_obj.name), f"PVC {pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {pvc_obj.name} is deleted.")

        delete_provisioner.result()

        # Collect the list of RBD images
        image_list_out_final = ct_pod.exec_ceph_cmd(ceph_cmd=f"rbd ls -p {constants.DEFAULT_BLOCKPOOL}", format="")
        image_list_final = image_list_out_final.strip().split()
        log.info(f"List of RBD images after deleting the RBD provisioner leader pod and the PVC {image_list_final}")

        stale_images = [image for image in image_list_final if image not in image_list_initial]

        # Raise error if stale image is present
        assert stale_images, f"List of stale images are present - {stale_images}"
