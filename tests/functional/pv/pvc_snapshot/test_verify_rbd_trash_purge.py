import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_external_mode,
)
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

logger = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.8")
@pytest.mark.polarion_id("OCS-2595")
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_external_mode
class TestVerifyRbdTrashPurge(ManageTest):
    """
    Verify RBD trash purge command when the RBD image have snapshots

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        storageclass_factory,
        multi_pvc_factory,
        snapshot_factory,
    ):
        """
        Create RBD pool, storage class, PVCs and snapshots

        """
        self.num_of_pvc = 6

        # Create storage class
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            new_rbd_pool=True,
        )

        # Create PVC
        logger.info(f"Creating {self.num_of_pvc} RBD PVCs")
        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            storageclass=self.sc_obj,
            size=3,
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvc,
            wait_each=False,
        )

        # Create snapshot
        logger.info(f"Creating snapshots for {self.num_of_pvc} PVCs")
        self.snap_objs = [snapshot_factory(pvc_obj, False) for pvc_obj in self.pvc_objs]

        # Verify snapshots are ready
        # Setting timeout as 600 seconds due to the bug 1972013
        logger.info("Verify snapshots are ready")
        for snap_obj in self.snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=600,
            )

    def test_verify_rbd_trash_purge_when_snapshots_present(self):
        """
        Verify RBD trash purge command when the RBD image in trash have snapshots. Verifies bug 1964373.

        """
        pool_name = self.sc_obj.get()["parameters"]["pool"]
        ct_pod = get_ceph_tools_pod()

        # Delete the PVCs
        logger.test_step(f"Delete all {self.num_of_pvc} PVCs and half of the snapshots")
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
        for pvc_obj in self.pvc_objs:
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        # Delete half of the snapshots
        for snap_obj in self.snap_objs[: int(self.num_of_pvc / 2)]:
            snap_obj.delete()
        for snap_obj in self.snap_objs[: int(self.num_of_pvc / 2)]:
            snap_obj.ocp.wait_for_delete(resource_name=snap_obj.name)

        # Collect the list of RBD images in trash
        logger.test_step(
            f"Run rbd trash purge on pool '{pool_name}' and verify it fails for images with snapshots"
        )
        image_list_out = ct_pod.exec_ceph_cmd(
            ceph_cmd=f"rbd trash ls {pool_name}", format=""
        )
        image_list_initial = image_list_out.strip().split("\n")
        logger.info(f"RBD images in trash for pool '{pool_name}': {image_list_initial}")

        # Try to delete all images using rbd trash purge command.
        # The command should fail because some images cannot be removed from trash.
        try:
            ct_pod.exec_ceph_cmd(ceph_cmd=f"rbd trash purge {pool_name}", format="")
            raise UnexpectedBehaviour(
                "Unexpected: Rbd trash rm purge command completed successfully"
            )
        except CommandFailed as cfe:
            if "rbd: some expired images could not be removed" not in str(cfe):
                raise

        # Collect the list of RBD images remaining in trash
        image_list_out = ct_pod.exec_ceph_cmd(
            ceph_cmd=f"rbd trash ls {pool_name}", format=""
        )
        image_final = image_list_out.strip().split("\n")[0]
        logger.info(
            f"List of RBD images remaining in trash after running trash purge- {image_final}"
        )

        # Try to delete each of the remaining images from trash. Rbd trash rm command should fail.
        logger.test_step(
            "Verify rbd trash rm fails for individual images that have snapshots"
        )
        for image_id in image_final.split()[::2]:
            try:
                ct_pod.exec_ceph_cmd(
                    ceph_cmd=f"rbd trash rm {image_id} -p {pool_name}",
                    format="",
                )
                raise UnexpectedBehaviour(
                    f"Rbd trash rm command to delete the image with id {image_id} completed successfully"
                )
            except CommandFailed as cfe:
                if "rbd: image has snapshots" not in str(cfe):
                    raise

        # Check if any image removal is in progress
        logger.test_step("Verify no image removal is in progress after trash purge")
        ceph_progress = ct_pod.exec_ceph_cmd(ceph_cmd="ceph progress json", format="")
        for progress_item in ceph_progress.get("events", []):
            assert (
                f"Removing image {pool_name}" not in progress_item["message"]
            ), f"Some image deletion is in progress after running trash purge command\n{ceph_progress}"
