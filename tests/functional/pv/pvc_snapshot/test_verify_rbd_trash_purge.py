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

log = logging.getLogger(__name__)


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
        log.info("Create PVCs")
        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            storageclass=self.sc_obj,
            size=3,
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvc,
            wait_each=False,
        )

        # Create snapshot
        log.info("Create snapshots")
        self.snap_objs = [snapshot_factory(pvc_obj, False) for pvc_obj in self.pvc_objs]

        # Verify snapshots are ready
        # Setting timeout as 600 seconds due to the bug 1972013
        log.info("Verify snapshots are ready")
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
        image_list_out = ct_pod.exec_ceph_cmd(
            ceph_cmd=f"rbd trash ls {pool_name}", format=""
        )
        image_list_initial = image_list_out.strip().split("\n")
        log.info(f"List of RBD images in trash - {image_list_initial}")

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
        log.info(
            f"List of RBD images remaining in trash after running trash purge- {image_final}"
        )

        # Try to delete each of the remaining images from trash. Rbd trash rm command should fail.
        # Get the image IDs from image_final str.
        # Eg : image_final = '61379c647069 csi-vol-7b1da20d-f107-11eb-99c7-0a580a80020d 6137b6f29eb7
        # csi-vol-7e40651f-f107-11eb-99c7-0a580a80020d 6137c642615f csi-vol-77f43877-f107-11eb-99c7-0a580a80020d'
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
        ceph_progress = ct_pod.exec_ceph_cmd(ceph_cmd="ceph progress json", format="")
        for progress_item in ceph_progress.get("events", []):
            assert (
                f"Removing image {pool_name}" not in progress_item["message"]
            ), f"Some image deletion is in progress after running trash purge command\n{ceph_progress}"
