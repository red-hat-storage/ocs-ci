import logging
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io, verifyvolume
from ocs_ci.helpers.helpers import create_pvc
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import delete_pvcs

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-6322")
class TestVmHotPlugUnplug(E2ETest):
    """
    Test case for VM hot plugging and unplugging of PVC disks.
    This test ensures that PVC disks can be hotplugged into a running VM
    and that data written to the disk is persisted after reboot.
    """

    def test_vm_hot_plugging_unplugging(
        self,
        # setup_cnv,
        project_factory,
        multi_cnv_workload,
    ):
        """
        Test the hot plugging and unplugging of a PVC into/from a VM.

        The test involves:
        1. Hotplugging a disk into a running VM based on PVC.
        2. Verifying the disk is attached to the VM.
        3. Writing data to the disk and rebooting the VM to test persistence.
        4. Hotplugging another disk without the --persist flag and verifying it is detached correctly.
        """

        proj_obj = project_factory()
        file_paths = ["/file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        log.info(f"Total VMs to process: {len(vm_list)}")

        for index, vm_obj in enumerate(vm_list):
            before_disks = vm_obj.run_ssh_cmd(
                command="lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(f"Disks before hotplug:\n{before_disks}")

            # Step 2: Create a PVC and hotplug it to the VM with persist flag
            pvc_obj = create_pvc(
                sc_name=vm_obj.sc_name,
                namespace=vm_obj.namespace,
                size="20Gi",
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
            )
            log.info(f"PVC {pvc_obj.name} created successfully")

            # Attach the PVC to the VM (with persist flag enabled)
            vm_obj.addvolume(volume_name=pvc_obj.name, verify=True)
            log.info(f"Hotplugged PVC {pvc_obj.name} to VM {vm_obj.name}")
            time.sleep(30)
            # Step 3: Verify the disk is attached
            after_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks after hotplug:\n{after_disks}")
            assert (
                set(after_disks) - set(before_disks)
            ) != set(), f"Failed to plug disk {pvc_obj.name} to VM {vm_obj.name}"

            # Step 4: Perform I/O on the attached disk to ensure it's working
            log.info(f"Running I/O operation on VM {vm_obj.name}")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            # Step 5: Reboot the VM and verify the data is persistent
            log.info(f"Rebooting VM {vm_obj.name}")
            vm_obj.restart(wait=True, verify=True)
            log.info(f"Reboot Success for VM: {vm_obj.name}")

            # Verify that the disk is still attached after reboot
            assert verifyvolume(
                vm_obj.name, volume_name=pvc_obj.name, namespace=vm_obj.namespace
            ), f"Unable to find volume {pvc_obj.name} mounted on VM: {vm_obj.name}"

            # Verify that the data on the disk persisted
            # after reboot (using MD5 checksum)
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"MD5 mismatch after reboot for VM {vm_obj.name}"

            # Step 6: Hotplug another disk to the VM without persist flag
            pvc_obj_wout = create_pvc(
                sc_name=vm_obj.sc_name,
                namespace=vm_obj.namespace,
                size="20Gi",
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
            )
            log.info(f"PVC {pvc_obj_wout.name} created successfully")

            # Attach the new PVC to the VM (without persist flag)
            vm_obj.addvolume(volume_name=pvc_obj_wout.name, persist=False)
            log.info(
                f"Hotplugged PVC {pvc_obj_wout.name} to VM {vm_obj.name} without persist"
            )
            time.sleep(30)
            # Step 7: Verify the new disk was successfully hotplugged
            after_disks_wout_add = vm_obj.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(
                f"Disks after hotplug of {pvc_obj_wout.name}:\n{after_disks_wout_add}"
            )

            # Step 8: Perform I/O on the new disk
            log.info(f"Running I/O operation on VM {vm_obj.name}")
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])

            # Step 9: Unplug the newly hotplugged disk
            vm_obj.removevolume(volume_name=pvc_obj_wout.name, verify=True)

            # Step 10: Verify the disk was successfully detached
            after_hotplug_rm_disk_wout = vm_obj.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(
                f"Disks after unplugging {pvc_obj_wout.name}:\n{after_hotplug_rm_disk_wout}"
            )

            # Ensure the hotplugged disk was removed successfully (check for no change)
            assert set(after_disks) == set(
                after_hotplug_rm_disk_wout
            ), f"Failed to unplug disk {pvc_obj_wout.name} from VM {vm_obj.name}"
            delete_pvcs(pvc_objs=[pvc_obj_wout, pvc_obj])
