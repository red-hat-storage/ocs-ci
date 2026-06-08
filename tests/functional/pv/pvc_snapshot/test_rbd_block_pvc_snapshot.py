import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients_push_missing_configs,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.helpers.helpers import wait_for_resource_state, create_pods

logger = logging.getLogger(__name__)


@provider_mode
@green_squad
@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.polarion_id("OCS-2361")
class TestRbdBlockPvcSnapshot(ManageTest):
    """
    Tests to take snapshots of RBD Block VolumeMode PVCs

    """

    @pytest.fixture(autouse=True)
    def setup(
        self, project_factory, snapshot_restore_factory, multi_pvc_factory, pod_factory
    ):
        """
        Create PVCs and pods

        """
        self.pvc_size = 5

        self.pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=self.pvc_size,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            status=constants.STATUS_BOUND,
            num_of_pvc=2,
            wait_each=False,
        )

        self.pod_objs = create_pods(
            self.pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )

    @run_on_all_clients_push_missing_configs
    def test_rbd_block_pvc_snapshot(
        self, snapshot_factory, snapshot_restore_factory, pod_factory, cluster_index
    ):
        """
        Test to take snapshots of RBD Block VolumeMode PVCs

        """
        logger.test_step("Calculate initial md5sum and run IO on all pods")
        for pod_obj in self.pod_objs:
            # Find initial md5sum
            pod_obj.md5sum_before_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )
            pod_obj.run_io(
                storage_type="block",
                size=f"{self.pvc_size - 1}G",
                io_direction="write",
                runtime=60,
                direct=1,
            )
        logger.info("IO started on all pods")

        # Wait for IO completion
        for pod_obj in self.pod_objs:
            pod_obj.get_fio_results()
        logger.info("IO completed on all pods")

        snap_objs = []

        logger.test_step("Verify md5sum changed after IO and create snapshots")
        for pod_obj in self.pod_objs:
            md5sum_after_io = cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                block=True,
            )
            assert (
                pod_obj.md5sum_before_io != md5sum_after_io
            ), f"md5sum has not changed after IO on pod {pod_obj.name}"
            logger.debug(f"Creating snapshot of PVC {pod_obj.pvc.name}")
            snap_obj = snapshot_factory(pod_obj.pvc, wait=False)
            snap_obj.md5sum = md5sum_after_io
            snap_objs.append(snap_obj)
        logger.info(f"Created {len(snap_objs)} snapshots")

        logger.test_step("Verify snapshots are ready")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )

        logger.test_step("Delete pods and parent PVCs to verify snapshot independence")
        for pod_obj in self.pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        logger.info("Deleted all the pods")

        for pvc_obj in self.pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            logger.debug(
                f"Deleted PVC {pvc_obj.name}. Verifying PV {pv_obj.name} is deleted."
            )
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)
        logger.info("Deleted parent PVCs and PVs before restoring snapshots")

        restore_pvc_objs = []

        logger.test_step("Create new PVCs from snapshots and verify Bound state")
        for snap_obj in snap_objs:
            logger.debug(f"Creating a PVC from snapshot {snap_obj.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                size=f"{self.pvc_size}Gi",
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=snap_obj.parent_access_mode,
                status="",
            )

            logger.debug(
                f"Created PVC {restore_pvc_obj.name} from snapshot {snap_obj.name}"
            )
            restore_pvc_obj.md5sum = snap_obj.md5sum
            restore_pvc_objs.append(restore_pvc_obj)
        logger.info(f"Created {len(restore_pvc_objs)} new PVCs from snapshots")
        for pvc_obj in restore_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        logger.info("Verified: Restored PVCs are Bound.")

        logger.test_step("Attach restored PVCs to pods and verify Running state")
        restore_pod_objs = create_pods(
            restore_pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=2,
            status="",
        )

        logger.info("Verifying new pods are running")
        for pod_obj in restore_pod_objs:
            timeout = (
                300
                if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                else 60
            )
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout)
        logger.info("Verified: New pods are running")

        logger.test_step("Verify md5sum data integrity on restored pods")
        for pod_obj in restore_pod_objs:
            logger.debug(f"Verifying md5sum on pod {pod_obj.name}")
            verify_data_integrity(
                pod_obj=pod_obj,
                file_name=pod_obj.get_storage_path(storage_type="block"),
                original_md5sum=pod_obj.pvc.md5sum,
                block=True,
            )
            logger.debug(f"Verified md5sum on pod {pod_obj.name}")
        logger.info("Verified md5sum on all restored pods")

        logger.test_step("Run IO on restored pods to verify usability")
        for pod_obj in restore_pod_objs:
            pod_obj.run_io(storage_type="block", size="500M", runtime=15, direct=1)

        # Wait for IO completion on new pods
        logger.info("Waiting for IO completion on new pods")
        for pod_obj in restore_pod_objs:
            pod_obj.get_fio_results()
        logger.info("IO completed on new pods.")
