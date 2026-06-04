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

logger = logging.getLogger(__name__)


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
        logger.test_step(
            f"Run IO at utilization levels {usage_percent}% and create snapshots at each level"
        )
        for usage in usage_percent:
            if usage != 0:
                for pod_obj in self.pods:
                    logger.debug(
                        f"Running IO on pod {pod_obj.name} to utilize {usage}%"
                    )
                    pod_obj.pvc.filename = f"{pod_obj.name}_{usage}"
                    pod_obj.run_io(
                        storage_type="fs",
                        size=f"{int(self.pvc_size/len(usage_percent))}G",
                        runtime=20,
                        fio_filename=pod_obj.pvc.filename,
                    )

                for pod_obj in self.pods:
                    # Wait for fio to finish
                    pod_obj.get_fio_results()
                    logger.debug(
                        f"IO to utilize {usage}% finished on pod {pod_obj.name}"
                    )
                    # Calculate md5sum
                    md5_sum = pod.cal_md5sum(pod_obj, pod_obj.pvc.filename)
                    if not getattr(pod_obj.pvc, "md5_sum", None):
                        setattr(pod_obj.pvc, "md5_sum", {})
                    pod_obj.pvc.md5_sum[pod_obj.pvc.filename] = md5_sum

            # Take snapshot of all PVCs
            logger.info(f"Creating snapshot of all PVCs at {usage}% utilization")
            for pvc_obj in self.pvcs:
                logger.debug(f"Creating snapshot of PVC {pvc_obj.name} at {usage}%")
                snap_obj = snapshot_factory(pvc_obj, wait=False)
                # Set a dict containing filename:md5sum for later verification
                setattr(snap_obj, "md5_sum", deepcopy(getattr(pvc_obj, "md5_sum", {})))
                snap_obj.usage_on_mount = get_used_space_on_mount_point(
                    pvc_obj.get_attached_pods()[0]
                )
                snapshots.append(snap_obj)
        logger.info(
            f"Created {len(snapshots)} snapshots at different utilization levels"
        )

        logger.test_step(f"Verify all {len(snapshots)} snapshots are ready")
        for snapshot in snapshots:
            snapshot.ocp.wait_for_resource(
                condition="true",
                resource_name=snapshot.name,
                column=constants.STATUS_READYTOUSE,
                timeout=90,
            )

        logger.test_step("Delete pods and parent PVCs to verify snapshot independence")
        for pod_obj in self.pods:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        logger.info("Deleted all the pods")

        for pvc_obj in self.pvcs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            logger.debug(
                f"Deleted PVC {pvc_obj.name}. Verifying PV {pv_obj.name} is deleted."
            )
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)
        logger.info("Deleted parent PVCs and PVs before restoring snapshots")

        restore_pvc_objs = []

        logger.test_step(
            f"Restore {len(snapshots)} PVCs from snapshots and verify Bound state"
        )
        for snapshot in snapshots:
            logger.debug(f"Creating a PVC from snapshot {snapshot.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snapshot,
                size=f"{self.pvc_size}Gi",
                volume_mode=snapshot.parent_volume_mode,
                access_mode=snapshot.parent_access_mode,
                status="",
            )

            logger.debug(
                f"Created PVC {restore_pvc_obj.name} from snapshot {snapshot.name}"
            )
            restore_pvc_objs.append(restore_pvc_obj)
        logger.info(f"Created {len(restore_pvc_objs)} new PVCs from snapshots")

        # Increased wait time to 600 seconds as a workaround for BZ 1899968
        # TODO: Revert wait time to 200 seconds once BZ 1899968 is fixed
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=600
            )
            pvc_obj.reload()
        logger.info("Verified: Restored PVCs are Bound.")

        logger.test_step(
            "Delete snapshots and verify VolumeSnapshotContents are cleaned up"
        )
        snapcontent_objs = []
        for snapshot in snapshots:
            snapcontent_objs.append(get_snapshot_content_obj(snap_obj=snapshot))
            snapshot.delete()

        logger.info("Verifying snapshots are deleted")
        for snapshot in snapshots:
            snapshot.ocp.wait_for_delete(resource_name=snapshot.name)
        logger.info("Verified: Snapshots are deleted")

        # Verify VolumeSnapshotContents are deleted
        for snapcontent_obj in snapcontent_objs:
            snapcontent_obj.ocp.wait_for_delete(
                resource_name=snapcontent_obj.name, timeout=180
            )

        logger.test_step("Attach restored PVCs to pods and verify Running state")
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
            logger.debug(
                f"Attached PVC {restore_pvc_obj.name} to pod {restore_pod_obj.name}"
            )
            restore_pod_objs.append(restore_pod_obj)

        logger.info("Verifying new pods are running")
        for pod_obj in restore_pod_objs:
            timeout = (
                300
                if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                else 60
            )
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout)
        logger.info("Verified: New pods are running")

        logger.test_step("Verify md5sum data integrity on all restored pods")
        for restore_pod_obj in restore_pod_objs:
            logger.debug(
                f"Verifying md5sum of files on pod {restore_pod_obj.name}: "
                f"{list(restore_pod_obj.pvc.snapshot.md5_sum.keys())}"
            )
            for (
                file_name,
                actual_md5_sum,
            ) in restore_pod_obj.pvc.snapshot.md5_sum.items():
                file_path = pod.get_file_path(restore_pod_obj, file_name)
                file_exists = pod.check_file_existence(restore_pod_obj, file_path)
                logger.assertion(
                    f"File {file_name} exists on pod {restore_pod_obj.name}: "
                    f"expected=True, actual={file_exists}"
                )
                assert (
                    file_exists
                ), f"File {file_name} does not exist on pod {restore_pod_obj.name}"

                pod.verify_data_integrity(restore_pod_obj, file_name, actual_md5_sum)
                logger.debug(
                    f"Verified md5sum of file {file_name} on pod {restore_pod_obj.name}"
                )
        logger.info("md5sum verified on all restored pods")

        logger.test_step("Verify usage on mount point matches snapshot usage")
        for pod_obj in restore_pod_objs:
            usage_on_pod = get_used_space_on_mount_point(pod_obj)
            logger.assertion(
                f"Usage on pod {pod_obj.name}: "
                f"expected={pod_obj.pvc.snapshot.usage_on_mount}%, actual={usage_on_pod}%"
            )
            assert usage_on_pod == pod_obj.pvc.snapshot.usage_on_mount, (
                f"Usage on mount point is not the expected value on pod "
                f"{pod_obj.name}. Usage in percentage {usage_on_pod}. "
                f"Expected usage in percentage "
                f"{pod_obj.pvc.snapshot.usage_on_mount}"
            )
            logger.debug(f"Verified usage on pod {pod_obj.name}: {usage_on_pod}%")
        logger.info("Verified usage on all restored pods")
