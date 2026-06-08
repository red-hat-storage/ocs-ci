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

logger = logging.getLogger(__name__)


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

        logger.test_step(f"Run IO on all {len(self.pods)} pods and calculate md5sum")
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
                direct=int(pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK),
            )

        logger.info("Waiting for IO to complete on all pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            logger.debug(f"IO completed on pod {pod_obj.name}")
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
            logger.debug(f"md5sum obtained from pod {pod_obj.name}")
        logger.info("IO is successful on all pods")

        # Select the pods to be deleted
        for disruption, pod_type in zip(disruption_ops, pods_to_delete):
            # Select snapshotter leader if the pod is provisioner pod
            disruption.set_resource(
                resource=pod_type,
                leader_type="snapshotter" if "provisioner" in pod_type else "",
            )

        logger.test_step("Take snapshots of all PVCs while deleting csi/rook pods")
        for pvc_obj in self.pvcs:
            logger.debug(f"Taking snapshot of PVC {pvc_obj.name}")
            pvc_obj.snap_proc = executor.submit(snapshot_factory, pvc_obj, wait=False)
        logger.info(f"Started taking snapshot of all {len(self.pvcs)} PVCs")

        # Delete the pods 'pods_to_delete'
        logger.info(f"Deleting pods {pods_to_delete}")
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

        logger.test_step("Wait for all snapshots to reach Ready state")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=300,
            )
            logger.debug(f"Snapshot {snap_obj.name} is Ready")
            snap_obj.reload()
        logger.info(f"All {len(snap_objs)} snapshots are Ready")

        # Select the pods to be deleted
        for disruption, pod_type in zip(disruption_ops, pods_to_delete):
            disruption.set_resource(resource=pod_type)

        restore_pvc_objs = []

        logger.test_step("Restore PVCs from snapshots while deleting csi/rook pods")
        for snap_obj in snap_objs:
            logger.debug(f"Creating a PVC from snapshot {snap_obj.name}")
            snap_obj.restore_proc = executor.submit(
                snapshot_restore_factory,
                snapshot_obj=snap_obj,
                size=f"{self.pvc_size}Gi",
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=snap_obj.parent_access_mode,
                status="",
            )
        logger.info("Started creating new PVCs from snapshots")

        # Delete the pods 'pods_to_delete'
        logger.info(f"Deleting pods {pods_to_delete}")
        for disruption in disruption_ops:
            disruption.delete_proc = executor.submit(disruption.delete_resource)

        # Wait for delete and recovery
        [disruption.delete_proc.result() for disruption in disruption_ops]

        # Get restored PVCs
        for snap_obj in snap_objs:
            restore_pvc_obj = snap_obj.restore_proc.result()
            restore_pvc_objs.append(restore_pvc_obj)
            logger.debug(
                f"Created PVC {restore_pvc_obj.name} from snapshot {snap_obj.name}"
            )
        logger.info(f"Created {len(restore_pvc_objs)} new PVCs from snapshots")

        logger.test_step("Verify restored PVCs reach Bound state")
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300
            )
            pvc_obj.reload()
            pvc_obj.volume_mode = pvc_obj.data["spec"]["volumeMode"]
        logger.info("Verified: Restored PVCs are Bound.")

        restore_pod_objs = []

        logger.test_step("Attach restored PVCs to pods and verify Running state")
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
        logger.info(f"Attached {len(restore_pod_objs)} restored PVCs to pods")

        logger.info("Verifying new pods are running")
        for pod_obj in restore_pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        logger.info("Verified: New pods are running")

        logger.test_step("Verify md5sum data integrity on restored pods")
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
            logger.debug(
                f"Verified: md5sum of {file_name_pod} on pod {pod_obj.name} "
                f"matches the original md5sum"
            )
        logger.info("Data integrity check passed on all pods")

        logger.test_step("Run IO on restored pods to verify usability")
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
                direct=int(pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK),
            )

        logger.info("Waiting for IO to complete on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.get_fio_results()
            logger.debug(f"IO completed on new pod {pod_obj.name}")
        logger.info("IO completed on all restored pods")
