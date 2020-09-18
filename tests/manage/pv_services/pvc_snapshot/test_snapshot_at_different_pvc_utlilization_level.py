import logging
import pytest
from copy import deepcopy

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, tier1

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.6')
@pytest.mark.polarion_id('OCS-2318')
class TestSnapshotAtDifferentPvcUsageLevel(ManageTest):
    """
    Tests to take snapshot when PVC usage is at different levels
    """
    @pytest.fixture(autouse=True)
    def setup(self, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvc_size = 10
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=[constants.ACCESS_MODE_RWO]
        )

    def test_snapshot_at_different_usage_level(
        self, pod_factory, teardown_factory, snapshot_restore_factory
    ):
        """
        Test to take multiple snapshots of same PVC when the PVC usage is at
        0%, 20%, 40%, 60%, and 80%, then delete the parent PVC and restore the
        snapshots to create new PVCs

        """
        snapshots = []
        for usage in [0, 20, 40, 60, 80]:
            if usage != 0:
                for pod_obj in self.pods:
                    log.info(
                        f"Running IO on pod {pod_obj.name} to utilize {usage}%"
                    )
                    pod_obj.pvc.filename = f'{pod_obj.name}_{usage}'
                    pod_obj.run_io(
                        storage_type='fs', size='2G', runtime=20,
                        fio_filename=pod_obj.pvc.filename
                    )
                log.info(f"IO started on all pods to utilize {usage}%")

                for pod_obj in self.pods:
                    # Wait for fio to finish
                    pod_obj.get_fio_results()
                    log.info(
                        f"IO to utilize {usage}% finished on pod "
                        f"{pod_obj.name}"
                    )
                    # Calculate md5sum
                    md5_sum = pod.cal_md5sum(pod_obj, pod_obj.pvc.filename)
                    if not getattr(pod_obj.pvc, 'md5_sum', None):
                        setattr(pod_obj.pvc, 'md5_sum', {})
                    pod_obj.pvc.md5_sum[pod_obj.pvc.filename] = md5_sum

            # Take snapshot of all PVCs
            log.info(f"Creating snapshot of all PVCs at {usage}%")
            for pvc_obj in self.pvcs:
                log.info(
                    f"Creating snapshot of PVC {pvc_obj.name} at {usage}%"
                )
                snap_obj = pvc_obj.create_snapshot(wait=True)
                teardown_factory(snap_obj)
                # Set a dict containing filename:md5sum for later verification
                setattr(
                    snap_obj, 'md5_sum',
                    deepcopy(getattr(pvc_obj, 'md5_sum', {}))
                )
                snapshots.append(snap_obj)
                log.info(f"Created snapshot of PVC {pvc_obj.name} at {usage}%")
            log.info(f"Created snapshot of all PVCs at {usage}%")

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
            "Deleted parent PVCs before restoring snapshot. "
            "PVs are also deleted."
        )

        # Create PVCs out of the snapshots and attach to pods
        log.info("Creating new PVCs from snapshots")
        for snapshot in snapshots:
            log.info(f"Creating a PVC from snapshot {snapshot.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snapshot,
                size=f'{self.pvc_size}Gi',
                volume_mode=snapshot.parent_volume_mode,
                access_mode=snapshot.parent_access_mode,
                status=constants.STATUS_BOUND
            )

            log.info(
                f"Created PVC {restore_pvc_obj.name} from snapshot "
                f"{snapshot.name}"
            )

            # Attach the restored PVC to a pod
            interface = constants.CEPHFILESYSTEM if (
                constants.CEPHFS_INTERFACE in snapshot.parent_sc
            ) else constants.CEPHBLOCKPOOL
            restore_pod_obj = pod_factory(
                interface=interface, pvc=restore_pvc_obj,
                status=constants.STATUS_RUNNING
            )
            log.info(
                f"Attached the PVC {restore_pvc_obj.name} to pod "
                f"{restore_pod_obj.name}"
            )

            # Verify md5sum of files on the new pod
            log.info(
                f"Verifying md5sum of these files on pod "
                f"{restore_pod_obj.name}:{snapshot.md5_sum}"
            )
            for file_name, actual_md5_sum in snapshot.md5_sum.items():
                file_path = pod.get_file_path(restore_pod_obj, file_name)
                log.info(
                    f"Checking the existence of file {file_name} on pod "
                    f"{restore_pod_obj.name}"
                )
                assert pod.check_file_existence(restore_pod_obj, file_path), (
                    f"File {file_name} does not exist on pod "
                    f"{restore_pod_obj.name}"
                )
                log.info(
                    f"File {file_name} exists on pod {restore_pod_obj.name}"
                )

                # Verify that the md5sum matches
                log.info(
                    f"Verifying md5sum of file {file_name} on pod "
                    f"{restore_pod_obj.name}"
                )
                pod.verify_data_integrity(
                    restore_pod_obj, file_name, actual_md5_sum
                )
                log.info(
                    f"Verified md5sum of file {file_name} on pod "
                    f"{restore_pod_obj.name}"
                )
            log.info(
                f"Verified md5sum of these files on pod "
                f"{restore_pod_obj.name}:{snapshot.md5_sum}"
            )
