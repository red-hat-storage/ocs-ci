import logging
import pytest
from itertools import cycle

from ocs_ci.ocs import constants, node
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients_push_missing_configs,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    polarion_id,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


@provider_mode
@green_squad
@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@polarion_id("OCS-2410")
class TestSnapshotRestoreWithDifferentAccessMode(ManageTest):
    """
    Tests to verify PVC snapshot restore with access mode different than
    parent PVC

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, snapshot_restore_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(pvc_size=3, pods_for_rwx=1)

    @run_on_all_clients_push_missing_configs
    def test_snapshot_restore_with_different_access_mode(
        self, pod_factory, snapshot_factory, snapshot_restore_factory, cluster_index
    ):
        """
        Restore snapshot with an access mode different than parent PVC

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
        logger.test_step("Run IO on all pods and calculate md5sum")

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
        logger.info(f"Started IO on all {len(self.pods)} pods")

        # Wait for IO to finish
        logger.info(f"Waiting for IO to finish on {len(self.pods)} pods")
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

        # Create snapshots
        logger.test_step(
            f"Create snapshots of {len(self.pvcs)} PVCs and wait for Ready state"
        )
        snap_objs = []
        for pvc_obj in self.pvcs:
            logger.debug(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snap_obj.interface = pvc_obj.interface
            snap_objs.append(snap_obj)
            logger.debug(f"Created snapshot of PVC {pvc_obj.name}")

        logger.info(f"Created {len(snap_objs)} snapshots. Waiting for Ready state")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snap_obj.reload()
        logger.info("Snapshots are Ready")

        # Restore snapshots
        logger.test_step(
            "Restore snapshots to create new PVCs with different access modes"
        )
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
                    f"{access_mode} from snapshot {snap_obj.name}. "
                    f"Parent PVC accessMode: {snap_obj.parent_access_mode}"
                )
                restore_pvcs.append(restore_obj)
        logger.info(
            "Restored all the snapshots to create PVCs with different access modes"
        )

        logger.test_step("Verify restored PVCs are Bound and volume modes match")
        for pvc_obj in restore_pvcs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=480
            )
            pvc_obj.reload()
        logger.info("Verified: Restored PVCs are Bound")

        # Verify restored PVC volume mode
        for pvc_obj in restore_pvcs:
            logger.assertion(
                f"PVC {pvc_obj.name} volume mode: expected='{pvc_obj.snapshot.parent_volume_mode}', "
                f"actual='{pvc_obj.data['spec']['volumeMode']}'"
            )
            assert (
                pvc_obj.data["spec"]["volumeMode"]
                == pvc_obj.snapshot.parent_volume_mode
            ), f"Volume mode mismatch in PVC {pvc_obj.name}"

        # Get worker node names and create an iterator
        nodes_iter = cycle(node.get_worker_nodes())

        # Attach the restored PVCs to pods
        logger.test_step(f"Attach {len(restore_pvcs)} restored PVCs to pods")
        restore_pod_objs = []
        for pvc_obj in restore_pvcs:
            if pvc_obj.data["spec"]["volumeMode"] == "Block":
                pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            else:
                pod_dict_path = ""
            # Create 2 pods if access mode is RWX, else 1
            for _ in range(
                int(pvc_obj.get_pvc_access_mode != constants.ACCESS_MODE_RWX), 2
            ):
                pvc_read_only_mode = None
                if pvc_obj.get_pvc_access_mode == constants.ACCESS_MODE_ROX:
                    pvc_read_only_mode = True
                restore_pod_obj = pod_factory(
                    interface=pvc_obj.interface,
                    pvc=pvc_obj,
                    status="",
                    node_name=next(nodes_iter),
                    pod_dict_path=pod_dict_path,
                    raw_block_pv=pvc_obj.data["spec"]["volumeMode"] == "Block",
                    pvc_read_only_mode=pvc_read_only_mode,
                )
                logger.debug(
                    f"Attaching the PVC {pvc_obj.name} to pod "
                    f"{restore_pod_obj.name}"
                )
                restore_pod_objs.append(restore_pod_obj)

        # Verify the new pods are running
        logger.test_step("Verify restored pods reach Running state")
        for pod_obj in restore_pod_objs:
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        logger.info("Verified: New pods are running")

        # Verify md5sum
        logger.test_step("Verify data integrity via md5sum on restored pods")
        for pod_obj in restore_pod_objs:
            if pod_obj.pvc.get_pvc_access_mode != constants.ACCESS_MODE_ROX:
                file_name_pod = (
                    file_name
                    if (
                        pod_obj.pvc.data["spec"]["volumeMode"]
                        == constants.VOLUME_MODE_FILESYSTEM
                    )
                    else pod_obj.get_storage_path(storage_type="block")
                )
                pod.verify_data_integrity(
                    pod_obj,
                    file_name_pod,
                    pod_obj.pvc.md5sum,
                    pod_obj.pvc.data["spec"]["volumeMode"]
                    == constants.VOLUME_MODE_BLOCK,
                )
                logger.debug(
                    f"Verified: md5sum of {file_name_pod} on pod {pod_obj.name} "
                    "matches the original md5sum"
                )
        logger.info("Data integrity check passed on all pods")
