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
from ocs_ci.ocs.resources import pod
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
        create_pvcs_and_pods,
        snapshot_restore_factory,
        pod_factory,
    ):
        """
        Create RBD PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=5,
            pods_for_rwx=1,
            access_modes_rbd=[
                "ReadWriteOnce",
                "ReadWriteMany-Block",
                "ReadWriteOnce-Block",
            ],
            num_of_rbd_pvc=3,
            num_of_cephfs_pvc=0,
        )
        # self.pvcs = multi_pvc_factory(
        #     interface=constants.CEPHBLOCKPOOL,
        #     size=5,
        #     #access_modes=["ReadWriteOnce", "ReadWriteMany-Block", "ReadWriteOnce-Block"],
        #     access_modes=["ReadWriteMany-Block"],
        #     status=constants.STATUS_BOUND,
        #     #num_of_pvc=3,
        #     num_of_pvc=1,
        # )
        # # Set volume mode on PVC objects
        # for pvc_obj in self.pvcs:
        #     pvc_info = pvc_obj.get()
        #     setattr(pvc_obj, "volume_mode", pvc_info["spec"]["volumeMode"])
        #
        #
        #
        # self.pods = []
        # for pvc_obj in self.pvcs:
        #     pod_obj = pod_factory(interface=constants.CEPHBLOCKPOOL,
        #                             pvc=pvc_obj,
        #                             raw_block_pv=pvc_obj.volume_mode == constants.VOLUME_MODE_BLOCK,
        #                           pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML
        #                             )
        #     self.pods.append(pod_obj)

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

        # Mount
        for pod_obj in self.pods:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK:
                # pod_obj.exec_cmd_on_pod(command="yum -y update", out_yaml_format=False)
                pod_obj.exec_cmd_on_pod(
                    command="apt-get -y install e2fsprogs", out_yaml_format=False
                )
                device_path = pod_obj.get_storage_path(storage_type="block")
                pod_obj.exec_cmd_on_pod(
                    command=f"mkfs.ext4 {device_path}", out_yaml_format=False
                )
                pod_obj.exec_cmd_on_pod(
                    command=f"mount -t ext4 {device_path} /var/lib/www/html",
                    out_yaml_format=False,
                )

        # Start IO
        log.info("Starting IO on all pods")
        for pod_obj in self.pods:
            pod_obj.run_io(
                storage_type="fs",
                size="1G",
                runtime=20,
                fio_filename=file_name,
                end_fsync=1,
                path="/var/lib/www/html",
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
                command=f'bash -c "md5sum /var/lib/www/html/{file_name}"',
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
            constants.VOLUME_MODE_FILESYSTEM: [constants.ACCESS_MODE_RWO],
            constants.VOLUME_MODE_BLOCK: [
                constants.ACCESS_MODE_RWX,
                constants.ACCESS_MODE_RWO,
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

        # Verify restored PVC volume mode"
        for pvc_obj in restore_pvcs:
            exp_vol_mode = switch_vol_mode(pvc_obj.snapshot.parent_volume_mode)
            assert (
                pvc_obj.data["spec"]["volumeMode"] == exp_vol_mode
            ), f"Volume mode mismatch in PVC {pvc_obj.name}"

        # Get worker node names and create an iterator
        nodes_iter = cycle(node.get_worker_nodes())

        # Attach the restored PVCs to pods
        log.info("Attach the restored PVCs to pods")
        restore_pod_objs = []
        for pvc_obj in restore_pvcs:
            # Create 2 pods if access mode is RWX, else 1
            for _ in range(
                int(pvc_obj.get_pvc_access_mode != constants.ACCESS_MODE_RWX), 2
            ):
                restore_pod_obj = dc_pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    pvc=pvc_obj,
                    node_name=next(nodes_iter),
                    raw_block_pv=pvc_obj.data["spec"]["volumeMode"] == "Block",
                    sa_obj=self.sa_obj,
                )

                log.info(
                    f"Attached the PVC {pvc_obj.name} to pod " f"{restore_pod_obj.name}"
                )
                restore_pod_objs.append(restore_pod_obj)

        # Mount
        for pod_obj in restore_pod_objs:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK:
                # pod_obj.exec_cmd_on_pod(command="yum -y update", out_yaml_format=False)
                pod_obj.exec_cmd_on_pod(
                    command="yum -y install e2fsprogs", out_yaml_format=False
                )
                device_path = pod_obj.get_storage_path(storage_type="block")
                pod_obj.exec_cmd_on_pod(
                    command=f"mount -t ext4 {device_path} /mnt", out_yaml_format=False
                )

        # Verify md5sum
        for pod_obj in restore_pod_objs:
            # Obtain md5sum
            md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
                command=f'bash -c "md5sum /mnt/{file_name}"', out_yaml_format=False
            )
            md5sum = md5sum_cmd_out.split()[0]
            log.info(f"md5sum of file {file_name} on pod {pod_obj.name}: {md5sum}")
            assert md5sum == pod_obj.pvc.md5sum, "md5sum mismatch."
            log.info(
                f"Verified: md5sum of {file_name} on pod {pod_obj.name} "
                "matches the original md5sum"
            )
            pod_obj.pvc.new_io_done = False
        log.info("Data integrity check passed on all pods- stage 1")

        file_name_new = "fio_file_2"
        # Start IO
        log.info("Starting IO on all pods")
        for pod_obj in restore_pod_objs:
            pod_obj.io_done = False
            if not pod_obj.pvc.new_io_done:
                pod_obj.run_io(
                    storage_type="fs",
                    size="1G",
                    runtime=20,
                    fio_filename=file_name_new,
                    end_fsync=1,
                    path="/mnt",
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
                    command=f'bash -c "md5sum /mnt/{file_name_new}"',
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
                log.info(
                    f"Created PVC {restore_obj.name} with volumeMode {volume_mode} and accessMode "
                    f"{access_mode} from snapshot {snap_obj.name}. "
                    f"Parent PVC accessMode: {snap_obj.parent_access_mode}"
                )
                restore_pvcs_new.append(restore_obj)
        log.info(
            "Restored all the snapshots to create PVCs with different volume modes"
        )

        log.info("Verifying restored PVCs are Bound")
        for pvc_obj in restore_pvcs_new:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=200
            )
            pvc_obj.reload()
        log.info("Verified: Restored PVCs are Bound")

        # Verify restored PVC volume mode"
        for pvc_obj in restore_pvcs:
            exp_vol_mode = switch_vol_mode(pvc_obj.snapshot.parent_volume_mode)
            assert (
                pvc_obj.data["spec"]["volumeMode"] == exp_vol_mode
            ), f"Volume mode mismatch in PVC {pvc_obj.name}"

        # Get worker node names and create an iterator
        nodes_iter = cycle(node.get_worker_nodes())

        # Attach the restored PVCs to pods
        log.info("Attach the restored PVCs to pods")
        restore_pod_objs_new = []
        for pvc_obj in restore_pvcs_new:
            # Create 2 pods if access mode is RWX, else 1
            for _ in range(
                int(pvc_obj.get_pvc_access_mode != constants.ACCESS_MODE_RWX), 2
            ):
                restore_pod_obj = dc_pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    pvc=pvc_obj,
                    node_name=next(nodes_iter),
                    raw_block_pv=pvc_obj.data["spec"]["volumeMode"] == "Block",
                    sa_obj=self.sa_obj,
                )

                log.info(
                    f"Attached the PVC {pvc_obj.name} to pod " f"{restore_pod_obj.name}"
                )
                restore_pod_objs_new.append(restore_pod_obj)

        # Mount
        for pod_obj in restore_pod_objs_new:
            if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK:
                # pod_obj.exec_cmd_on_pod(command="yum -y update", out_yaml_format=False)
                pod_obj.exec_cmd_on_pod(
                    command="yum -y install e2fsprogs", out_yaml_format=False
                )
                device_path = pod_obj.get_storage_path(storage_type="block")
                # Check ext4 signature or do pod_obj.exec_cmd_on_pod(command=f"mkfs.ext4 {device_path}")
                pod_obj.exec_cmd_on_pod(
                    command=f"mount -t ext4 {device_path} /mnt", out_yaml_format=False
                )

        # Verify md5sum
        for pod_obj in restore_pod_objs_new:
            ls_out = pod_obj.exec_cmd_on_pod(
                command=f'bash -c "ls /mnt"', out_yaml_format=False
            )
            log.info(
                f"ls out from pod {pod_obj.name} where {pod_obj.pvc.volume_mode} volume mode PVC {pod_obj.pvc.name} is used.\n{ls_out}"
            )
            # Obtain md5sum
            md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
                command=f'bash -c "md5sum /mnt/{file_name}"', out_yaml_format=False
            )
            md5sum = md5sum_cmd_out.split()[0]
            log.info(f"md5sum of file {file_name} on pod {pod_obj.name}: {md5sum}")
            assert md5sum == pod_obj.pvc.md5sum, "md5sum mismatch."
            log.info(
                f"Verified: md5sum of {file_name} on pod {pod_obj.name} "
                "matches the original md5sum"
            )
            md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
                command=f'bash -c "md5sum /mnt/{file_name_new}"', out_yaml_format=False
            )
            md5sum = md5sum_cmd_out.split()[0]
            log.info(f"md5sum of file {file_name_new} on pod {pod_obj.name}: {md5sum}")
            assert md5sum == pod_obj.pvc.md5sum_new, "md5sum mismatch on second file."
            log.info(
                f"Verified: md5sum of {file_name_new} on pod {pod_obj.name} "
                "matches the original md5sum"
            )
        log.info("Data integrity check passed on all pods- stage 2")
