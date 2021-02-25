import logging
import pytest
from itertools import cycle

from ocs_ci.ocs import constants, node
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    polarion_id,
    skipif_ocp_version,
)
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@tier2
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@polarion_id("")
class TestSnapshotRestoreWithDifferentVolumeMode(ManageTest):
    """
    Tests to verify RBD PVC snapshot restore with volume mode different than
    parent PVC

    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        project_factory,
        snapshot_restore_factory,
        create_pvcs_and_pods,
        pod_factory,
    ):
        """
        Create RBD PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=3,
            pods_for_rwx=1,
            access_modes_rbd=[
                "ReadWriteOnce",
                "ReadWriteMany-Block",
                "ReadWriteOnce-Block",
            ],
            num_of_rbd_pvc=3,
            num_of_cephfs_pvc=0,
        )

    def test_snapshot_restore_with_different_access_mode(
        self, pod_factory, snapshot_factory, snapshot_restore_factory
    ):
        """
        Restore snapshot with a volume mode different than parent PVC

        """
        file_name = "fio_test"
        switch_vol_mode = (
            lambda vol_mode: constants.VOLUME_MODE_BLOCK
            if vol_mode == constants.VOLUME_MODE_FILESYSTEM
            else constants.VOLUME_MODE_BLOCK
        )

        # Identify mount path to use the same path on pods using RBD Block volume mode PVC
        for pod_obj in self.pods:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM:
                mount_path = pod_obj.get_storage_path(storage_type="fs")

        # Mount volume
        for pod_obj in self.pods:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK:
                pod_obj.mount_device(mount_path=mount_path)

        # Start IO
        log.info("Starting IO on all pods")
        for pod_obj in self.pods:
            pod_obj.run_io(
                storage_type="fs",
                size="1G",
                runtime=20,
                fio_filename=file_name,
                end_fsync=1,
                path=mount_path,
            )
            log.info(f"IO started on pod {pod_obj.name}")
        log.info("Started IO on all pods")

        # Wait for IO to finish
        log.info("Wait for IO to finish on pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            log.info(f"IO finished on pod {pod_obj.name}")
            # Calculate md5sum to compare after restoring
            md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
                command=f'bash -c "md5sum {mount_path}/{file_name}"',
                out_yaml_format=False,
            )
            md5sum = md5sum_cmd_out.split()[0]
            log.info(f"md5sum of file {file_name} on pod {pod_obj.name}: {md5sum}")
            pod_obj.pvc.md5sum = md5sum
        log.info("IO finished on all pods")

        # Create snapshots
        log.info("Creating snapshot of the PVCs")
        snap_objs = []
        for pvc_obj in self.pvcs:
            log.info(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snap_obj.interface = constants.CEPHBLOCKPOOL
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

        # Access modes list
        access_modes_dict = {
            constants.VOLUME_MODE_FILESYSTEM: [
                constants.ACCESS_MODE_RWO,
                constants.ACCESS_MODE_ROX,
            ],
            constants.VOLUME_MODE_BLOCK: [
                constants.ACCESS_MODE_RWX,
                constants.ACCESS_MODE_RWO,
                constants.ACCESS_MODE_ROX,
            ],
        }

        # Restore snapshots
        log.info("Restoring snapshots to create new PVCs")
        restore_pvcs = []
        for snap_obj in snap_objs:
            volume_mode = switch_vol_mode(snap_obj.parent_volume_mode)
            access_modes = access_modes_dict[volume_mode]
            for access_mode in access_modes:
                restore_obj = snapshot_restore_factory(
                    snapshot_obj=snap_obj,
                    volume_mode=volume_mode,
                    access_mode=access_mode,
                    status="",
                )
                restore_obj.interface = snap_obj.interface
                restore_obj.md5sum = snap_obj.md5sum
                restore_obj.volume_mode = volume_mode
                restore_obj.access_mode_used = access_mode
                log.info(
                    f"Created PVC {restore_obj.name} with volumeMode {volume_mode} and accessMode "
                    f"{access_mode} from snapshot {snap_obj.name}. "
                    f"Parent PVC accessMode: {snap_obj.parent_access_mode}"
                )
                restore_pvcs.append(restore_obj)
        log.info(
            "Restored all the snapshots to create PVCs with different volume modes"
        )

        log.info("Verifying restored PVCs are Bound")
        for pvc_obj in restore_pvcs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=200
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound")

        # Verify restored PVC volume mode and access mode"
        for pvc_obj in restore_pvcs:
            exp_vol_mode = switch_vol_mode(pvc_obj.snapshot.parent_volume_mode)
            assert (
                pvc_obj.data["spec"]["volumeMode"] == exp_vol_mode
            ), f"Volume mode mismatch in PVC {pvc_obj.name}."
            assert pvc_obj.access_mode_used == pvc_obj.get_pvc_access_mode, (
                f"Access mode mismatch in PVC {pvc_obj.name}. Expected {pvc_obj.access_mode_used}. "
                f"Present access mode is {pvc_obj.get_pvc_access_mode}"
            )

        # Get worker node names and create an iterator
        nodes_iter = cycle(node.get_worker_nodes())

        # Attach the restored PVCs to pods
        log.info("Attach the restored PVCs to pods")
        restore_pod_objs = []
        for pvc_obj in restore_pvcs:
            # Create 2 pods if access mode is RWX or ROX, else 1
            for _ in range(
                int(
                    pvc_obj.get_pvc_access_mode
                    not in [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_ROX]
                ),
                2,
            ):
                restore_pod_obj = pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    pvc=pvc_obj,
                    status="",
                    node_name=next(nodes_iter),
                    raw_block_pv=pvc_obj.data["spec"]["volumeMode"] == "Block",
                )
                log.info(
                    f"Attached the restored PVC {pvc_obj.name} to pod {restore_pod_obj.name}"
                )
                restore_pod_objs.append(restore_pod_obj)

        # Verify pods are running
        for pod_obj in restore_pod_objs:
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)

        # Mount volume
        for pod_obj in restore_pod_objs:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK:
                pod_obj.mount_device(mount_path=mount_path, do_format=False)

        # Verify md5sum
        for pod_obj in restore_pod_objs:
            # Obtain md5sum
            md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
                command=f'bash -c "md5sum {mount_path}/{file_name}"',
                out_yaml_format=False,
            )
            md5sum = md5sum_cmd_out.split()[0]
            log.info(f"md5sum of file {file_name} on pod {pod_obj.name}: {md5sum}")
            assert md5sum == pod_obj.pvc.md5sum, "md5sum mismatch."
            log.info(
                f"Verified: md5sum of {file_name} on pod {pod_obj.name} "
                "matches the original md5sum"
            )
            # Flag to set if new file is created from a pod so that IO need not be run
            # on another pod which use the same RWX PVC
            pod_obj.pvc.new_io_done = False
        log.info("Data integrity check passed on all pods - stage 1")

        file_name_new = "fio_file_2"

        # Start IO
        log.info("Starting IO on all pods")
        for pod_obj in restore_pod_objs:
            pod_obj.io_done = False
            if not (
                pod_obj.pvc.new_io_done
                or (pod_obj.pvc.get_pvc_access_mode == constants.ACCESS_MODE_ROX)
            ):
                pod_obj.run_io(
                    storage_type="fs",
                    size="1G",
                    runtime=20,
                    fio_filename=file_name_new,
                    end_fsync=1,
                    path=mount_path,
                )
                log.info(f"IO started on pod {pod_obj.name}")
                pod_obj.io_done = True
            pod_obj.pvc.new_io_done = True
        log.info("Started IO on all pods")

        # Wait for IO to finish
        log.info("Wait for IO to finish on pods")
        for pod_obj in restore_pod_objs:
            if pod_obj.io_done:
                pod_obj.get_fio_results()
                log.info(f"IO finished on pod {pod_obj.name}")
                # Calculate md5sum to compare after restoring
                md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
                    command=f'bash -c "md5sum {mount_path}/{file_name_new}"',
                    out_yaml_format=False,
                )
                md5sum = md5sum_cmd_out.split()[0]
                log.info(
                    f"md5sum of file {file_name_new} on pod {pod_obj.name}: {md5sum}"
                )
                pod_obj.pvc.md5sum_new = md5sum
        log.info("IO finished on all pods")

        # Create snapshots
        log.info("Creating snapshot of the PVCs")
        snap_objs_new = []
        for pvc_obj in restore_pvcs:
            log.info(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.md5sum = pvc_obj.md5sum
            snap_obj.md5sum_new = pvc_obj.md5sum_new
            snap_obj.interface = constants.CEPHBLOCKPOOL
            snap_objs_new.append(snap_obj)
            log.info(f"Created snapshot of PVC {pvc_obj.name}")

        log.info("Snapshots are created. Wait for the snapshots to be in Ready state")
        for snap_obj in snap_objs_new:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=180,
            )
            snap_obj.reload()
        log.info("New snapshots are Ready")

        # Restore snapshots
        log.info("Restoring new snapshots to create new PVCs")
        restore_pvcs_new = []
        for snap_obj in snap_objs_new:
            volume_mode = switch_vol_mode(snap_obj.parent_volume_mode)
            access_modes = access_modes_dict[volume_mode]
            for access_mode in access_modes:
                restore_obj = snapshot_restore_factory(
                    snapshot_obj=snap_obj,
                    volume_mode=volume_mode,
                    access_mode=access_mode,
                    status="",
                )
                restore_obj.interface = snap_obj.interface
                restore_obj.md5sum = snap_obj.md5sum
                restore_obj.md5sum_new = snap_obj.md5sum_new
                restore_obj.volume_mode = volume_mode
                restore_obj.access_mode_used = access_mode
                log.info(
                    f"Created PVC {restore_obj.name} with volumeMode {volume_mode} and accessMode "
                    f"{access_mode} from snapshot {snap_obj.name}. "
                    f"Parent PVC accessMode: {snap_obj.parent_access_mode}"
                )
                restore_pvcs_new.append(restore_obj)
        log.info(
            "Restored all the new snapshots to create PVCs with different volume modes"
        )

        log.info("Verifying restored PVCs are Bound")
        for pvc_obj in restore_pvcs_new:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=200
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound")

        # Verify restored PVC volume mode and access mode"
        for pvc_obj in restore_pvcs_new:
            exp_vol_mode = switch_vol_mode(pvc_obj.snapshot.parent_volume_mode)
            assert (
                pvc_obj.data["spec"]["volumeMode"] == exp_vol_mode
            ), f"Volume mode mismatch in PVC {pvc_obj.name}."
            assert pvc_obj.access_mode_used == pvc_obj.get_pvc_access_mode, (
                f"Access mode mismatch in PVC {pvc_obj.name}. Expected {pvc_obj.access_mode_used}. "
                f"Present access mode is {pvc_obj.get_pvc_access_mode}"
            )

        # Attach the restored PVCs to pods
        log.info("Attach the rewly restored PVCs to pods")
        restore_pod_objs_new = []
        for pvc_obj in restore_pvcs_new:
            # Create 2 pods if access mode is RWX or ROX, else 1
            for _ in range(
                int(
                    pvc_obj.get_pvc_access_mode
                    not in [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_ROX]
                ),
                2,
            ):
                restore_pod_obj = pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    pvc=pvc_obj,
                    status="",
                    node_name=next(nodes_iter),
                    raw_block_pv=pvc_obj.data["spec"]["volumeMode"] == "Block",
                )
                log.info(
                    f"Attached the restored PVC {pvc_obj.name} to pod {restore_pod_obj.name}"
                )
                restore_pod_objs_new.append(restore_pod_obj)

        # Verify pods are running
        for pod_obj in restore_pod_objs_new:
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)

        # Mount volume
        for pod_obj in restore_pod_objs_new:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK:
                pod_obj.mount_device(mount_path=mount_path, do_format=False)

        # Verify md5sum
        for pod_obj in restore_pod_objs_new:
            ls_out = pod_obj.exec_cmd_on_pod(
                command=f"ls {mount_path}/", out_yaml_format=False
            )
            log.info(
                f"ls output from pod {pod_obj.name} where {pod_obj.pvc.volume_mode} "
                f"volume mode PVC {pod_obj.pvc.name} is used.\n{ls_out}"
            )
            # Verify md5sum of both the files
            for filename in file_name, file_name_new:
                md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
                    command=f"md5sum {mount_path}/{filename}", out_yaml_format=False
                )
                md5sum = md5sum_cmd_out.split()[0]
                log.info(f"md5sum of file {filename} on pod {pod_obj.name}: {md5sum}")
                exp_md5sum = (
                    pod_obj.pvc.md5sum
                    if filename == file_name
                    else pod_obj.pvc.md5sum_new
                )
                assert (
                    md5sum == exp_md5sum
                ), f"md5sum mismatch of file {filename} on pod {pod_obj.name}. Expected {exp_md5sum}. Actual {md5sum}"
                log.info(
                    f"Verified: md5sum of {filename} on pod {pod_obj.name} "
                    "matches the original md5sum"
                )
        log.info("Data integrity check passed on all pods - stage 2")
