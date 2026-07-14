import logging
import pytest
from copy import deepcopy

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources.pod import get_used_space_on_mount_point
from ocs_ci.helpers.helpers import wait_for_resource_state, get_snapshot_content_obj

log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.polarion_id("OCS-2318")
class TestSnapshotAtDifferentPvcUsageLevel(ManageTest):
    """
    Tests to take snapshot when PVC usage is at different levels
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, snapshot_restore_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvc_size = 10
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=[constants.ACCESS_MODE_RWO],
        )

    def test_snapshot_at_different_usage_level(
        self, snapshot_factory, snapshot_restore_factory, pod_factory
    ):
        """
        Test to take multiple snapshots of same PVC when the PVC usage is at
        0%, 20%, 40%, 60%, and 80%, then delete the parent PVC and restore the
        snapshots to create new PVCs. Delete snapshots and attach the restored
        PVCs to pods to verify the data.

        """
        snapshots = []
        usage_percent = [0, 20, 40, 60, 80]
        for usage in usage_percent:
            if usage != 0:
                for pod_obj in self.pods:
                    log.info(f"Running IO on pod {pod_obj.name} to utilize {usage}%")
                    pod_obj.pvc.filename = f"{pod_obj.name}_{usage}"
                    pod_obj.run_io(
                        storage_type="fs",
                        size=f"{int(self.pvc_size/len(usage_percent))}G",
                        runtime=20,
                        fio_filename=pod_obj.pvc.filename,
                    )
                log.info(f"IO started on all pods to utilize {usage}%")

                for pod_obj in self.pods:
                    # Wait for fio to finish
                    pod_obj.get_fio_results()
                    log.info(
                        f"IO to utilize {usage}% finished on pod " f"{pod_obj.name}"
                    )
                    # Calculate md5sum
                    md5_sum = pod.cal_md5sum(pod_obj, pod_obj.pvc.filename)
                    if not getattr(pod_obj.pvc, "md5_sum", None):
                        setattr(pod_obj.pvc, "md5_sum", {})
                    pod_obj.pvc.md5_sum[pod_obj.pvc.filename] = md5_sum

            # Take snapshot of all PVCs
            log.info(f"Creating snapshot of all PVCs at {usage}%")
            for pvc_obj in self.pvcs:
                log.info(f"Creating snapshot of PVC {pvc_obj.name} at {usage}%")
                snap_obj = snapshot_factory(pvc_obj, wait=False)
                # Set a dict containing filename:md5sum for later verification
                setattr(snap_obj, "md5_sum", deepcopy(getattr(pvc_obj, "md5_sum", {})))
                snap_obj.usage_on_mount = get_used_space_on_mount_point(
                    pvc_obj.get_attached_pods()[0]
                )
                snapshots.append(snap_obj)
                log.info(f"Created snapshot of PVC {pvc_obj.name} at {usage}%")
            log.info(f"Created snapshot of all PVCs at {usage}%")
        log.info("Snapshots creation completed.")

        # Verify snapshots are ready
        log.info("Verify snapshots are ready")
        for snapshot in snapshots:
            snapshot.ocp.wait_for_resource(
                condition="true",
                resource_name=snapshot.name,
                column=constants.STATUS_READYTOUSE,
                timeout=90,
            )

        # Delete pods
        log.info("Deleting the pods")
        for pod_obj in self.pods:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        log.info("Deleted all the pods")

        # Delete parent PVCs
        log.info("Deleting parent PVCs")
        for pvc_obj in self.pvcs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            log.info(
                f"Deleted PVC {pvc_obj.name}. Verifying whether PV "
                f"{pv_obj.name} is deleted."
            )
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)
        log.info(
            "Deleted parent PVCs before restoring snapshot. " "PVs are also deleted."
        )

        restore_pvc_objs = []

        # Create PVCs out of the snapshots
        log.info("Creating new PVCs from snapshots")
        for snapshot in snapshots:
            log.info(f"Creating a PVC from snapshot {snapshot.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snapshot,
                size=f"{self.pvc_size}Gi",
                volume_mode=snapshot.parent_volume_mode,
                access_mode=snapshot.parent_access_mode,
                status="",
            )

            log.info(
                f"Created PVC {restore_pvc_obj.name} from snapshot " f"{snapshot.name}"
            )
            restore_pvc_objs.append(restore_pvc_obj)
        log.info("Created new PVCs from all the snapshots")

        # Confirm that the restored PVCs are Bound
        # Increased wait time to 600 seconds as a workaround for BZ 1899968
        # TODO: Revert wait time to 200 seconds once BZ 1899968 is fixed
        log.info("Verify the restored PVCs are Bound")
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=600
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound.")

        snapcontent_objs = []
        # Get VolumeSnapshotContent form VolumeSnapshots and delete
        # VolumeSnapshots
        log.info("Deleting snapshots")
        for snapshot in snapshots:
            snapcontent_objs.append(get_snapshot_content_obj(snap_obj=snapshot))
            snapshot.delete()

        # Verify volume snapshots are deleted
        log.info("Verify snapshots are deleted")
        for snapshot in snapshots:
            snapshot.ocp.wait_for_delete(resource_name=snapshot.name)
        log.info("Verified: Snapshots are deleted")

        # Verify VolumeSnapshotContents are deleted
        for snapcontent_obj in snapcontent_objs:
            snapcontent_obj.ocp.wait_for_delete(
                resource_name=snapcontent_obj.name, timeout=180
            )

        # Attach the restored PVCs to pods
        log.info("Attach the restored PVCs to pods")
        restore_pod_objs = []
        for restore_pvc_obj in restore_pvc_objs:
            interface = (
                constants.CEPHFILESYSTEM
                if (constants.CEPHFS_INTERFACE in restore_pvc_obj.snapshot.parent_sc)
                else constants.CEPHBLOCKPOOL
            )
            restore_pod_obj = pod_factory(
                interface=interface, pvc=restore_pvc_obj, status=""
            )
            log.info(
                f"Attached the PVC {restore_pvc_obj.name} to pod "
                f"{restore_pod_obj.name}"
            )
            restore_pod_objs.append(restore_pod_obj)

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in restore_pod_objs:
            timeout = (
                300
                if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                else 60
            )
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout)
        log.info("Verified: New pods are running")

        # Verify md5sum of files
        log.info("Verifying md5sum of files on all the pods")
        for restore_pod_obj in restore_pod_objs:
            log.info(
                f"Verifying md5sum of these files on pod "
                f"{restore_pod_obj.name}:"
                f"{restore_pod_obj.pvc.snapshot.md5_sum}"
            )
            for (
                file_name,
                actual_md5_sum,
            ) in restore_pod_obj.pvc.snapshot.md5_sum.items():
                file_path = pod.get_file_path(restore_pod_obj, file_name)
                log.info(
                    f"Checking the existence of file {file_name} on pod "
                    f"{restore_pod_obj.name}"
                )
                assert pod.check_file_existence(restore_pod_obj, file_path), (
                    f"File {file_name} does not exist on pod " f"{restore_pod_obj.name}"
                )
                log.info(f"File {file_name} exists on pod {restore_pod_obj.name}")

                # Verify that the md5sum matches
                log.info(
                    f"Verifying md5sum of file {file_name} on pod "
                    f"{restore_pod_obj.name}"
                )
                pod.verify_data_integrity(restore_pod_obj, file_name, actual_md5_sum)
                log.info(
                    f"Verified md5sum of file {file_name} on pod "
                    f"{restore_pod_obj.name}"
                )
            log.info(
                f"Verified md5sum of these files on pod "
                f"{restore_pod_obj.name}:"
                f"{restore_pod_obj.pvc.snapshot.md5_sum}"
            )
        log.info("md5sum verified")

        # Verify usage on mount point
        log.info("Verify usage on new pods")
        for pod_obj in restore_pod_objs:
            usage_on_pod = get_used_space_on_mount_point(pod_obj)
            assert usage_on_pod == pod_obj.pvc.snapshot.usage_on_mount, (
                f"Usage on mount point is not the expected value on pod "
                f"{pod_obj.name}. Usage in percentage {usage_on_pod}. "
                f"Expected usage in percentage "
                f"{pod_obj.pvc.snapshot.usage_on_mount}"
            )
            log.info(
                f"Verified usage on new pod {pod_obj.name}. Usage in "
                f"percentage {usage_on_pod}. Expected usage in percentage "
                f"{pod_obj.pvc.snapshot.usage_on_mount}"
            )
        log.info("Verified usage on new pods")
