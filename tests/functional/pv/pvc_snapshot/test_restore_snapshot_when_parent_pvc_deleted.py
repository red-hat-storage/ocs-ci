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

log = logging.getLogger(__name__)


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

        # Start IO
        log.info("Starting IO on all pods")
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
            log.info(f"IO started on pod {pod_obj.name}")
        log.info("Started IO on all pods")

        # Wait for IO to finish
        log.info("Wait for IO to finish on pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            log.info(f"IO finished on pod {pod_obj.name}")
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
        log.info("IO finished on all pods")

        # Create snapshots
        log.info("Creating snapshot of the PVCs")
        snap_objs = []
        for pvc_obj in self.pvcs:
            log.info(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snap_obj.interface = pvc_obj.interface
            snap_objs.append(snap_obj)
            log.info(f"Created snapshot of PVC {pvc_obj.name}")

        log.info("Snapshots are created. Wait for the snapshots to be in Ready state")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snap_obj.reload()
        log.info("Snapshots are Ready")

        # Delete the parent PVCs
        log.info("Deleting the Parent PVCs and it's attached pod")
        for pod_obj in self.pods:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        for pvc_obj in self.pvcs:
            log.info(f"Deleting PVC {pvc_obj.name}")
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        # Restore snapshots with same and different access mode
        log.info("Restoring snapshots to create new PVCs")
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
                log.info(
                    f"Created PVC {restore_obj.name} with accessMode "
                    f"{access_mode} from snapshot {snap_obj.name}. "
                    f"Parent PVC accessMode: {snap_obj.parent_access_mode}"
                )
                restore_pvcs.append(restore_obj)
        log.info(
            "Restored all the snapshots to create PVCs with different access modes"
        )

        log.info("Verifying restored PVCs are Bound")
        for pvc_obj in restore_pvcs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=200
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound")

        # Delete snapshots
        log.info("Deleting snapshots")
        for snap_obj in snap_objs:
            log.info(f"Deleting snapshot {snap_obj.name}")
            snap_obj.delete()
            snap_obj.ocp.wait_for_delete(resource_name=snap_obj.name)

        # Clone PVC from the restored PVCs
        log.info("Creating clone of the restored PVCs")
        for restore_obj in restore_pvcs:
            if restore_obj.get_pvc_access_mode != constants.ACCESS_MODE_ROX:
                pvc_clone_factory(restore_obj, timeout=360)
        log.info("Created clone of the PVCs. Cloned PVCs are Bound")

        # Create Snapshot2 of restored PVCs
        log.info("Creating snapshot of the restored PVCs")
        new_snap_objs = []
        for restore_obj in restore_pvcs:
            log.info(f"Creating snapshot of restored PVC {restore_obj.name}")
            if restore_obj.get_pvc_access_mode != constants.ACCESS_MODE_ROX:
                snap_obj = snapshot_factory(restore_obj, wait=False)
                snap_obj.md5sum = restore_obj.md5sum
                snap_obj.interface = restore_obj.interface
                new_snap_objs.append(snap_obj)
                log.info(f"Created snapshot of PVC {restore_obj.name}")

        log.info("Snapshots are created. Wait for the snapshots to be in Ready state")
        for snap_obj in new_snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snap_obj.reload()
        log.info("Snapshots are Ready")

        # Delete restored PVCs
        for restore_obj in restore_pvcs:
            log.info(f"Deleting restore PVC {restore_obj.name}")
            restore_obj.delete()
            restore_obj.ocp.wait_for_delete(resource_name=restore_obj.name)

        # Create pvc-restore2 from snapshots
        log.info("Restoring snapshots to create new PVCs")
        for snap_obj in new_snap_objs:
            snapshot_restore_factory(
                snapshot_obj=snap_obj,
                volume_mode=snap_obj.parent_volume_mode,
                status=constants.STATUS_BOUND,
                timeout=360,
            )
        log.info(
            "Restored all the snapshots to create PVCs with different access modes"
        )
