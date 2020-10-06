import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, tier1
from tests.helpers import wait_for_resource_state, create_pods

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.6')
@pytest.mark.polarion_id('OCS-2361')
class TestRbdBlockPvcSnapshot(ManageTest):
    """
    Tests to take snapshots of RBD Block VolumeMode PVCs

    """
    @pytest.fixture(autouse=True)
    def setup(
        self, project_factory, snapshot_restore_factory,
        multi_pvc_factory, pod_factory
    ):
        """
        Create PVCs and pods

        """
        self.pvc_size = 5

        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=self.pvc_size,
            access_modes=[
                f'{constants.ACCESS_MODE_RWX}-Block',
                f'{constants.ACCESS_MODE_RWO}-Block'
            ],
            status=constants.STATUS_BOUND,
            num_of_pvc=2,
            wait_each=False
        )

        self.pod_objs = create_pods(
            self.pvc_objs, pod_factory, constants.CEPHBLOCKPOOL,
            pods_for_rwx=1, status=constants.STATUS_RUNNING
        )

    def md5sum(self, pod_obj):
        """
        Find md5sum

        Args:
            pod_obj (Pod): Instance of class Pod

        Returns:
            str: md5sum value

        """
        file_path = pod_obj.get_storage_path(storage_type='block')
        md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
            command=f"bash -c \"md5sum {file_path}\"", out_yaml_format=False
        )
        md5sum = md5sum_cmd_out.split()[0]
        log.info(f"md5sum of file {file_path} on pod {pod_obj.name}: {md5sum}")
        return md5sum

    def test_rbd_block_pvc_snapshot(
        self, snapshot_factory, snapshot_restore_factory, pod_factory
    ):
        """
        Test to take snapshots of RBD Block VolumeMode PVCs

        """
        # Run IO
        log.info("Find initial md5sum value and run IO on all pods")
        for pod_obj in self.pod_objs:
            # Find initial md5sum
            pod_obj.md5sum_before_io = self.md5sum(pod_obj)
            pod_obj.run_io(
                storage_type='block', size=f'{self.pvc_size - 1}G',
                io_direction='write', runtime=60
            )
        log.info("IO started on all pods")

        # Wait for IO completion
        for pod_obj in self.pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on all pods")

        snap_objs = []

        # Verify md5sum has changed after IO. Create snapshot
        log.info(
            "Verify md5sum has changed after IO and create snapshot from "
            "all PVCs"
        )
        for pod_obj in self.pod_objs:
            md5sum_after_io = self.md5sum(pod_obj)
            assert pod_obj.md5sum_before_io != md5sum_after_io, (
                f"md5sum has not changed after IO on pod {pod_obj.name}"
            )
            log.info(
                f"Creating snapshot of PVC {pod_obj.pvc.name}"
            )
            snap_obj = snapshot_factory(pod_obj.pvc, wait=False)
            snap_obj.md5sum = md5sum_after_io
            snap_objs.append(snap_obj)
        log.info("Snapshots created")

        # Verify snapshots are ready
        log.info("Verify snapshots are ready")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition='true', resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE, timeout=180
            )

        # Delete pods
        log.info("Deleting the pods")
        for pod_obj in self.pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        log.info("Deleted all the pods")

        # Delete parent PVCs to verify snapshot is independent
        log.info("Deleting parent PVCs")
        for pvc_obj in self.pvc_objs:
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

        restore_pvc_objs = []

        # Create PVCs out of the snapshots
        log.info("Creating new PVCs from snapshots")
        for snap_obj in snap_objs:
            log.info(f"Creating a PVC from snapshot {snap_obj.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                size=f'{self.pvc_size}Gi',
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=snap_obj.parent_access_mode,
                status=''
            )

            log.info(
                f"Created PVC {restore_pvc_obj.name} from snapshot "
                f"{snap_obj.name}"
            )
            restore_pvc_obj.md5sum = snap_obj.md5sum
            restore_pvc_objs.append(restore_pvc_obj)
        log.info("Created new PVCs from all the snapshots")

        # Confirm that the restored PVCs are Bound
        log.info("Verify the restored PVCs are Bound")
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound.")

        # Attach the restored PVCs to pods. Attach RWX PVC on two pods
        log.info("Attach the restored PVCs to pods")
        restore_pod_objs = create_pods(
            restore_pvc_objs, pod_factory, constants.CEPHBLOCKPOOL,
            pods_for_rwx=2, status=''
        )

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in restore_pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        log.info("Verified: New pods are running")

        log.info("Verifying md5sum on new pods")
        for pod_obj in restore_pod_objs:
            log.info(f"Verifying md5sum on pod {pod_obj.name}")
            assert pod_obj.pvc.md5sum == self.md5sum(pod_obj), (
                f"md5sum mismatch. Pod {pod_obj}"
            )
            log.info(f"Verified md5sum on pod {pod_obj.name}")
        log.info("Verified md5sum on all pods")

        # Run IO on new pods
        log.info("Starting IO on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.run_io(
                storage_type='block', size='500M', runtime=15
            )

        # Wait for IO completion on new pods
        log.info("Waiting for IO completion on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on new pods.")
