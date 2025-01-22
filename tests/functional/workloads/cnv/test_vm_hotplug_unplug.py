import logging
import time
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io, verifyvolume
from ocs_ci.helpers.helpers import create_pvc
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-6322")
class TestVmHotPlugUnplug(E2ETest):
    """
    Test case for VM hot plugging and unplugging of PVC disks.
    """

    def test_vm_hot_plugging_unplugging(
        self,
        # setup_cnv,
        project_factory,
        multi_cnv_workload,
    ):
        """
        Verify that hotplugging and hot unplugging of a PVC to/from a VM works

            Steps:
            1. Hotplug disk to the running VM based on PVC.
            2. Verify the disk is attached to VM
            3. Add data to disk
                a. Identify newly attached disk.
                b. Create file or do dd on new disk
            4. Reboot the VM,
            5. After reboot check if disk is still attached.
            6. Make sure newly added data on new vm disk is intact
            7. Unplug(Dettach) the disk from vm
            8. Verify disk is successfully detached using console or cli
            9. login into VM and confirm disk is no longer listed.
            10 Repeat the above tests for DVT based VM
        """

        # Create project and get VM details
        proj_obj = project_factory()
        file_paths = ["/file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        log.info(f"Total VMs to process: {len(vm_list)}")
        for index, vm_obj in enumerate(vm_list):
            pvc_obj = create_pvc(
                sc_name=vm_obj.sc_name,
                namespace=vm_obj.namespace,
                size="20Gi",
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
            )
            log.info(f"PVC {pvc_obj.name} created successfully")
            before_disks = vm_obj.run_ssh_cmd(
                command="lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(f"Disks before hotplug:\n{before_disks}")
            vm_obj.addvolume(volume_name=pvc_obj.name, verify=True)
            log.info(f"Hotplugged PVC {pvc_obj.name} to VM {vm_obj.name}")
            time.sleep(30)
            after_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks after hotplug:\n{after_disks}")
            log.info(f"Running I/O operation on VM {vm_obj.name}")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            log.info(f"Rebooting VM {vm_obj.name}")
            vm_obj.restart(wait=True, verify=True)
            log.info(f"Reboot Success for VM: {vm_obj.name}")

            # Verify that the disk is still attached
            assert verifyvolume(
                vm_obj.name, volume_name=pvc_obj.name, namespace=vm_obj.namespace
            ), f"Unable to found volume {pvc_obj.name} mounted on VM: {vm_obj.name}"

            # Verify data persistence by checking MD5 checksum
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"MD5 mismatch after reboot for VM {vm_obj.name}"

            # Unplug the disk
            vm_obj.removevolume(volume_name=pvc_obj.name, verify=True)

            # Verify the disk is detached
            after_hotplug_rm_disks = vm_obj.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(f"Disks after unplugging:\n{after_hotplug_rm_disks}")

            # Ensure the hotplugged disk was removed successfully
            assert set(after_hotplug_rm_disks) == set(
                before_disks
            ), f"Failed to unplug disk from VM {vm_obj.name}"
