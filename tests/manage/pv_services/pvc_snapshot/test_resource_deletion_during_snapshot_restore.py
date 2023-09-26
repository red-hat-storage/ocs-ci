import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier4c,
    ignore_leftover_label,
    skipif_ocp_version,
    skipif_managed_service,
)
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.helpers import disruption_helpers
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.framework import config

log = logging.getLogger(__name__)


@green_squad
@tier4c
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@skipif_managed_service
@ignore_leftover_label(constants.drain_canary_pod_label)
@pytest.mark.polarion_id("OCS-2369")
class TestResourceDeletionDuringSnapshotRestore(ManageTest):
    """
    Tests to verify PVC snapshot and restore will succeeded if rook-ceph,
    csi pods are re-spun while creating snapshot and while creating restore PVC

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, snapshot_restore_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvc_size = 3
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size, num_of_rbd_pvc=6, num_of_cephfs_pvc=4
        )

    def test_resource_deletion_during_snapshot_restore(
        self, snapshot_factory, snapshot_restore_factory, pod_factory
    ):
        """
        Verify PVC snapshot and restore will succeeded if rook-ceph,
        csi pods are re-spun while creating snapshot and while creating
        restore PVC

        """
        pods_to_delete = [
            "rbdplugin_provisioner",
            "cephfsplugin_provisioner",
            "cephfsplugin",
            "rbdplugin",
        ]
        if not config.DEPLOYMENT["external_mode"]:
            pods_to_delete.extend(["osd", "mgr"])
        executor = ThreadPoolExecutor(max_workers=len(self.pvcs) + len(pods_to_delete))
        disruption_ops = [disruption_helpers.Disruptions() for _ in pods_to_delete]
        file_name = "file_snap"

        # Run IO
        log.info("Running fio on all pods to create a file")
        for pod_obj in self.pods:
            storage_type = (
                "block"
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK)
                else "fs"
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=30,
                fio_filename=file_name,
                end_fsync=1,
            )

        log.info("Wait for IO to complete on pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            log.info(f"Verified IO on pod {pod_obj.name}")
            # Calculate md5sum
            file_name_pod = (
                file_name
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM)
                else pod_obj.get_storage_path(storage_type="block")
            )
            pod_obj.pvc.md5sum = cal_md5sum(
                pod_obj,
                file_name_pod,
                pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
            log.info(f"md5sum obtained from pod {pod_obj.name}")
        log.info("IO is successful on all pods")

        # Select the pods to be deleted
        for disruption, pod_type in zip(disruption_ops, pods_to_delete):
            # Select snapshotter leader if the pod is provisioner pod
            disruption.set_resource(
                resource=pod_type,
                leader_type="snapshotter" if "provisioner" in pod_type else "",
            )

        log.info("Start taking snapshot of all PVCs.")
        for pvc_obj in self.pvcs:
            log.info(f"Taking snapshot of PVC {pvc_obj.name}")
            pvc_obj.snap_proc = executor.submit(snapshot_factory, pvc_obj, wait=False)
        log.info("Started taking snapshot of all PVCs.")

        # Delete the pods 'pods_to_delete'
        log.info(f"Deleting pods {pods_to_delete}")
        for disruption in disruption_ops:
            disruption.delete_proc = executor.submit(disruption.delete_resource)

        # Wait for delete and recovery
        [disruption.delete_proc.result() for disruption in disruption_ops]

        # Get snapshots
        snap_objs = []
        for pvc_obj in self.pvcs:
            snap_obj = pvc_obj.snap_proc.result()
            snap_obj.md5sum = pvc_obj.md5sum
            snap_objs.append(snap_obj)

        # Wait for snapshots to be Ready
        log.info("Waiting for all snapshots to be Ready")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=300,
            )
            log.info(f"Snapshot {snap_obj.name} is Ready")
            snap_obj.reload()
        log.info("All snapshots are Ready")

        # Select the pods to be deleted
        for disruption, pod_type in zip(disruption_ops, pods_to_delete):
            disruption.set_resource(resource=pod_type)

        restore_pvc_objs = []

        # Create PVCs out of the snapshots
        log.info("Start creating new PVCs from snapshots")
        for snap_obj in snap_objs:
            log.info(f"Creating a PVC from snapshot {snap_obj.name}")
            snap_obj.restore_proc = executor.submit(
                snapshot_restore_factory,
                snapshot_obj=snap_obj,
                size=f"{self.pvc_size}Gi",
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=snap_obj.parent_access_mode,
                status="",
            )
        log.info("Started creating new PVCs from snapshots")

        # Delete the pods 'pods_to_delete'
        log.info(f"Deleting pods {pods_to_delete}")
        for disruption in disruption_ops:
            disruption.delete_proc = executor.submit(disruption.delete_resource)

        # Wait for delete and recovery
        [disruption.delete_proc.result() for disruption in disruption_ops]

        # Get restored PVCs
        for snap_obj in snap_objs:
            restore_pvc_obj = snap_obj.restore_proc.result()
            restore_pvc_objs.append(restore_pvc_obj)
            log.info(
                f"Created PVC {restore_pvc_obj.name} from snapshot " f"{snap_obj.name}"
            )
        log.info("Created new PVCs from all the snapshots")

        # Confirm that the restored PVCs are Bound
        log.info("Verifying the restored PVCs are Bound")
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300
            )
            pvc_obj.reload()
            pvc_obj.volume_mode = pvc_obj.data["spec"]["volumeMode"]
        log.info("Verified: Restored PVCs are Bound.")

        restore_pod_objs = []

        # Attach the restored PVCs to pods
        log.info("Attach the restored PVCs to pods")
        for pvc_obj in restore_pvc_objs:
            if pvc_obj.volume_mode == constants.VOLUME_MODE_BLOCK:
                pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            else:
                pod_dict_path = ""
            restore_pod_obj = pod_factory(
                interface=pvc_obj.interface,
                pvc=pvc_obj,
                status="",
                pod_dict_path=pod_dict_path,
                raw_block_pv=pvc_obj.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
            restore_pod_objs.append(restore_pod_obj)
        log.info("Attach the restored PVCs to pods")

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in restore_pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        log.info("Verified: New pods are running")

        # Verify md5sum
        log.info("Verify md5sum")
        for pod_obj in restore_pod_objs:
            file_name_pod = (
                file_name
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM)
                else pod_obj.get_storage_path(storage_type="block")
            )
            verify_data_integrity(
                pod_obj,
                file_name_pod,
                pod_obj.pvc.snapshot.md5sum,
                pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
            log.info(
                f"Verified: md5sum of {file_name_pod} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )
        log.info("Data integrity check passed on all pods")

        # Run IO
        log.info("Running IO on new pods")
        for pod_obj in restore_pod_objs:
            storage_type = (
                "block"
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK)
                else "fs"
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=20,
                fio_filename=file_name,
                end_fsync=1,
            )

        log.info("Wait for IO to complete on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.get_fio_results()
            log.info(f"Verified IO on new pod {pod_obj.name}")
        log.info("IO to completed on new pods")
