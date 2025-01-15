import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io, verifyvolume
from ocs_ci.helpers.helpers import create_pvc
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-")
class TestVmHotPlugUnplug(E2ETest):
    """
    Test case for VM hot plugging and unplugging of PVC disks.
    """

    def test_vm_hot_plugging_unplugging(
        self, project_factory, multi_cnv_workload, setup_cnv
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
            # Create a new PVC for hotplug
            pvc_obj = create_pvc(
                sc_name=constants.DEFAULT_CNV_CEPH_RBD_SC,
                namespace=vm_obj.namespace,
                size="20Gi",
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
            )
            log.info(f"PVC {pvc_obj.name} created successfully")

            # List disks before attaching the new volume
            before_disks = vm_obj.run_ssh_cmd(
                command="lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(f"Disks before hotplug:\n{before_disks}")

            # Hotplug the PVC to the VM
            vm_obj.addvolume(volume_name=pvc_obj.name)
            verifyvolume(vm_obj, volume_name=pvc_obj.name)
            log.info(f"Hotplugged PVC {pvc_obj.name} to VM {vm_obj.name}")

            # List disks after attaching the new volume
            after_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks after hotplug:\n{after_disks}")

            # Identify the newly attached disk
            new_disks = set(after_disks) - set(before_disks)
            log.info(f"Newly attached disks: {set(new_disks)}")

            # Perform I/O operation on the new disk
            log.info(
                f"Running I/O operation on the newly attached disk in VM {vm_obj.name}"
            )
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            # Reboot the VM
            log.info(f"Rebooting VM {vm_obj.name}")
            vm_obj.restart(wait=True, verify=True)

            # Verify that the disk is still attached
            verifyvolume(vm_obj, volume_name=pvc_obj.name)

            # Verify data persistence by checking MD5 checksum
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"MD5 mismatch after reboot for VM {vm_obj.name}"

            # Unplug the disk
            vm_obj.removevolume(volume_name=pvc_obj.name)

            # Verify the disk is detached
            after_hotplug_rm_disks = vm_obj.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(f"Disks after unplugging:\n{after_hotplug_rm_disks}")

            # Ensure the hotplugged disk was removed successfully
            assert set(after_hotplug_rm_disks) == set(
                before_disks
            ), f"Failed to unplug disk from VM {vm_obj.name}"

            # Confirm disk removal
            if not verifyvolume(vm_obj, volume_name=pvc_obj.name):
                log.info(
                    f"Volume {pvc_obj.name} unplugged successfully from VM {vm_obj.name}"
                )
                run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
            else:
                pytest.fail(
                    f"Volume {pvc_obj.name} is still attached to VM {vm_obj.name}"
                )
