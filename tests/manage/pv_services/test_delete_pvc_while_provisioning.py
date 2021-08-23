import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    tier4,
    polarion_id,
    skipif_ocs_version,
    bugzilla,
    ignore_data_rebalance,
)
from ocs_ci.helpers.helpers import (
    verify_volume_deleted_in_backend,
    default_thick_storage_class,
    default_ceph_block_pool,
    wait_for_resource_count_change,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.helpers import disruption_helpers
from ocs_ci.ocs.exceptions import UnexpectedBehaviour


log = logging.getLogger(__name__)
DISRUPTION_OPS = disruption_helpers.Disruptions()


@ignore_data_rebalance
@pytest.mark.parametrize(
    argnames=["resource_to_delete"],
    argvalues=[
        pytest.param(
            *[""],
            marks=[polarion_id("OCS-2533"), tier2],
        ),
        pytest.param(
            *["rbdplugin_provisioner"],
            marks=[
                polarion_id("OCS-2534"),
                tier4,
                pytest.mark.tier4a,
                bugzilla("1962956"),
            ],
        ),
    ],
)
class TestDeletePvcWhileProvisioning(ManageTest):
    """
    Tests to verify that deleting a PVC while provisioning will not create any stale volume.
    Based on the value of "resource_to_delete", provisioner pod also will be deleted.
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory):
        """
        Create Project for the test
        Returns:
            OCP: An OCP instance of project
        """
        self.proj_obj = project_factory()

    @skipif_ocs_version("<=4.9")
    def test_delete_rbd_pvc_while_thick_provisioning(
        self,
        resource_to_delete,
        pvc_factory,
        pod_factory,
    ):
        """
        Test to delete RBD PVC while thick provisioning is progressing and verify that no stale image is present.
        Based on the value of "resource_to_delete", provisioner pod also will be deleted.
        """
        pvc_size = 15
        executor = ThreadPoolExecutor(max_workers=1)

        if resource_to_delete:
            DISRUPTION_OPS.set_resource(
                resource=resource_to_delete, leader_type="provisioner"
            )

        ct_pod = get_ceph_tools_pod()

        # Collect the list of RBD images
        image_list_out_initial = ct_pod.exec_ceph_cmd(
            ceph_cmd=f"rbd ls -p {constants.DEFAULT_BLOCKPOOL}", format=""
        )
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

        # Ensure that the PVC is being created
        ret = wait_for_resource_count_change(
            get_all_pvcs, 0, self.proj_obj.namespace, "increase"
        )
        assert ret, "Wait timeout: PVC is not being created."
        log.info("PVC creation has started.")

        if resource_to_delete:
            log.info(f"Deleting {resource_to_delete} pod.")
            delete_provisioner = executor.submit(DISRUPTION_OPS.delete_resource)

        # Delete PVC
        log.info(f"Deleting PVC {pvc_obj.name}")
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(pvc_obj.name)
        log.info(f"Verified: PVC {pvc_obj.name} is deleted.")

        if resource_to_delete:
            delete_provisioner.result()

        # Collect the list of RBD images
        image_list_out_final = ct_pod.exec_ceph_cmd(
            ceph_cmd=f"rbd ls -p {default_ceph_block_pool()}", format=""
        )
        image_list_final = image_list_out_final.strip().split()
        log.info(f"List of RBD images after deleting the PVC {image_list_final}")

        stale_images = [
            image for image in image_list_final if image not in image_list_initial
        ]

        # Check whether more than one new image is present
        if len(stale_images) > 1:
            raise UnexpectedBehaviour(
                f"Could not verify the test result. Found more than one new rbd image - {stale_images}."
            )

        if stale_images:
            stale_image = stale_images[0].strip()
            # Wait for the image to get deleted
            image_deleted = verify_volume_deleted_in_backend(
                constants.CEPHBLOCKPOOL,
                image_uuid=stale_image.split("csi-vol-")[1],
                pool_name=default_ceph_block_pool(),
                timeout=300,
            )
            if not image_deleted:
                du_out = ct_pod.exec_ceph_cmd(
                    ceph_cmd=f"rbd du -p {default_ceph_block_pool()} {stale_image}",
                    format="",
                )
            assert image_deleted, (
                f"Wait timeout: RBD image {stale_image} is not deleted. Check the logs to ensure that"
                f" this is the stale image of the deleted PVC. rbd du output of the image : {du_out}"
            )
            log.info(f"Image {stale_image} deleted within the wait time period")
        else:
            log.info("No stale image found")
