import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    polarion_id,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


@green_squad
@skipif_ocs_version("<4.16")
@skipif_ocp_version("<4.16")
@polarion_id("OCS-6176")
class TestRestoreSnapshotWhenParentPVCDeleted(ManageTest):
    """
    Tests to verify restore a pvc from snapshot when the parent PVC is deleted

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        project_factory,
        snapshot_factory,
        snapshot_restore_factory,
        pvc_clone_factory,
        create_pvcs_and_pods,
    ):
        """
        Create PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(pvc_size=3, pods_for_rwx=1)

    @tier2
    def test_restore_snapshot_when_parent_pvc_deleted(
        self, snapshot_factory, snapshot_restore_factory, pvc_clone_factory
    ):
        """
        Restore a pvc from snapshot when the parent PVC is deleted

        """
        file_name = "fio_test"
        access_modes_dict = {
            constants.CEPHBLOCKPOOL: {
                constants.VOLUME_MODE_FILESYSTEM: [constants.ACCESS_MODE_RWO],
                constants.VOLUME_MODE_BLOCK: [
                    constants.ACCESS_MODE_RWX,
                    constants.ACCESS_MODE_RWO,
                ],
            },
            constants.CEPHFILESYSTEM: {
                constants.VOLUME_MODE_FILESYSTEM: [
                    constants.ACCESS_MODE_RWX,
                    constants.ACCESS_MODE_RWO,
                    constants.ACCESS_MODE_ROX,
                ]
            },
        }

        logger.test_step(f"Run IO on all {len(self.pods)} pods and calculate md5sum")
        for pod_obj in self.pods:
            storage_type = (
                "block"
                if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK
                else "fs"
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=20,
                fio_filename=file_name,
                end_fsync=1,
                direct=int(pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK),
            )
            logger.debug(f"IO started on pod {pod_obj.name}")

        logger.info("Waiting for IO to finish on all pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            logger.debug(f"IO finished on pod {pod_obj.name}")
            # Calculate md5sum to compare after restoring
            file_name_pod = (
                file_name
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM)
                else pod_obj.get_storage_path(storage_type="block")
            )
            pod_obj.pvc.md5sum = pod.cal_md5sum(
                pod_obj,
                file_name_pod,
                pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
        logger.info("IO finished on all pods")

        logger.test_step("Create snapshots of all PVCs and wait for Ready state")
        snap_objs = []
        for pvc_obj in self.pvcs:
            logger.debug(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snap_obj.interface = pvc_obj.interface
            snap_objs.append(snap_obj)

        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snap_obj.reload()
        logger.info(f"All {len(snap_objs)} snapshots are Ready")

        logger.test_step("Delete parent PVCs and their attached pods")
        for pod_obj in self.pods:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        for pvc_obj in self.pvcs:
            logger.debug(f"Deleting PVC {pvc_obj.name}")
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        logger.test_step("Restore snapshots with same and different access modes")
        restore_pvcs = []
        for snap_obj in snap_objs:
            access_modes = access_modes_dict[snap_obj.interface][
                snap_obj.parent_volume_mode
            ]
            for access_mode in access_modes:
                restore_obj = snapshot_restore_factory(
                    snapshot_obj=snap_obj,
                    volume_mode=snap_obj.parent_volume_mode,
                    access_mode=access_mode,
                    status="",
                )
                restore_obj.interface = snap_obj.interface
                restore_obj.md5sum = snap_obj.md5sum
                logger.debug(
                    f"Created PVC {restore_obj.name} with accessMode "
                    f"{access_mode} from snapshot {snap_obj.name} "
                    f"(parent accessMode: {snap_obj.parent_access_mode})"
                )
                restore_pvcs.append(restore_obj)
        logger.info(
            f"Restored {len(restore_pvcs)} PVCs from snapshots with various access modes"
        )

        logger.test_step("Verify restored PVCs reach Bound state")
        for pvc_obj in restore_pvcs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=200
            )
            pvc_obj.reload()
        logger.info("Verified: Restored PVCs are Bound")

        logger.test_step("Delete snapshots and clone restored PVCs")
        for snap_obj in snap_objs:
            logger.debug(f"Deleting snapshot {snap_obj.name}")
            snap_obj.delete()
            snap_obj.ocp.wait_for_delete(resource_name=snap_obj.name)
        logger.info(f"Deleted {len(snap_objs)} snapshots")

        logger.info("Creating clones of the restored PVCs")
        for restore_obj in restore_pvcs:
            if restore_obj.get_pvc_access_mode != constants.ACCESS_MODE_ROX:
                pvc_clone_factory(restore_obj, timeout=360)
        logger.info("Created clone of the PVCs. Cloned PVCs are Bound")

        logger.test_step(
            "Create new snapshots of restored PVCs, delete them, and restore again"
        )
        new_snap_objs = []
        for restore_obj in restore_pvcs:
            if restore_obj.get_pvc_access_mode != constants.ACCESS_MODE_ROX:
                snap_obj = snapshot_factory(restore_obj, wait=False)
                snap_obj.md5sum = restore_obj.md5sum
                snap_obj.interface = restore_obj.interface
                new_snap_objs.append(snap_obj)
                logger.debug(f"Created snapshot of restored PVC {restore_obj.name}")
        logger.info(f"Created {len(new_snap_objs)} snapshots of restored PVCs")
        for snap_obj in new_snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snap_obj.reload()
        logger.info("Snapshots are Ready")

        # Delete restored PVCs
        for restore_obj in restore_pvcs:
            logger.debug(f"Deleting restore PVC {restore_obj.name}")
            restore_obj.delete()
            restore_obj.ocp.wait_for_delete(resource_name=restore_obj.name)
        logger.info(f"Deleted {len(restore_pvcs)} restored PVCs")

        logger.info("Restoring second-generation snapshots to create new PVCs")
        for snap_obj in new_snap_objs:
            snapshot_restore_factory(
                snapshot_obj=snap_obj,
                volume_mode=snap_obj.parent_volume_mode,
                status=constants.STATUS_BOUND,
                timeout=360,
            )
        logger.info(
            "Restored all the snapshots to create PVCs with different access modes"
        )
