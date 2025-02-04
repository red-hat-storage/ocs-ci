import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import (
    cal_md5sum_vm,
    run_dd_io,
    verifyvolume,
    verify_hotplug,
)
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-")
class TestVmHotPlugUnplugSnapClone(E2ETest):
    """
    Test case for snapshot and clones with hotplug/unplug
    """

    def test_vm_hotpl_snap_clone(
        self,
        # setup_cnv,
        project_factory,
        cnv_workload,
        pvc_clone_factory,
        pvc_factory,
    ):
        """
        A running DVT based VM and a PVC based VM
        Steps:
        1. Hotplug disk to the running VM based on PVC.
        2. Verify the disk is attached to VM
        3. Add data to disk
        4. Reboot the VM, verify disk is still attached, check data integrity
        5. Create clones of hotplugged PVCs
        6. Attach clones to opposite VMs and verify disk operation
        7. Unplug the disks and verify detachment
        """
        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]

        # Create a PVC-based VM (VM1)
        vm_obj_pvc = cnv_workload(
            namespace=proj_obj.namespace, volume_interface=constants.VM_VOLUME_PVC
        )

        # Create the PVC for VM1
        pvc_obj = pvc_factory(
            project=proj_obj,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        log.info(f"PVC {pvc_obj.name} created successfully")

        # Create a DVT-based VM (VM2)
        vm_obj_dvt = cnv_workload(
            namespace=proj_obj.namespace, volume_interface=constants.VM_VOLUME_DVT
        )
        dvt_obj = pvc_factory(
            project=proj_obj,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        log.info(f"PVC {dvt_obj.name} created successfully")

        # List of VM-PVC pairs for hotplug testing
        vms_pvc = [(vm_obj_pvc, pvc_obj), (vm_obj_dvt, dvt_obj)]
        before_disks_hotplug = []

        # Hotplug disks and perform I/O operations
        for i, (vm_obj, pvc) in enumerate(vms_pvc):
            # Verify disks before hotplugging
            before_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks before hotplug on VM {vm_obj.name}:\n{before_disks}")
            before_disks_hotplug.append(before_disks)

            # Hotplug the PVC volume to the VM
            log.info(f"Hotplugging PVC {pvc.name} to VM {vm_obj.name}")
            vm_obj.addvolume(volume_name=pvc.name, verify=True)

            # Wait for the disk to be hotplugged successfully
            sample = TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=vm_obj,
                hot_pl_disk_raw=before_disks,
            )
            sample.wait_for_func_value(value=True)
            log.info(f"Hotplugged PVC {pvc.name} to VM {vm_obj.name}")

            # Run I/O operation
            log.info(f"Running I/O operation on VM {vm_obj.name}")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            # Reboot the VM
            log.info(f"Rebooting VM {vm_obj.name}")
            vm_obj.restart(wait=True, verify=True)
            log.info(f"Reboot Success for VM: {vm_obj.name}")

            # Verify disk is still attached after reboot
            assert verifyvolume(
                vm_obj.name, volume_name=pvc.name, namespace=vm_obj.namespace
            ), f"Unable to find volume {pvc.name} mounted on VM: {vm_obj.name}"

            # Verify data persistence by checking MD5 checksum
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"MD5 mismatch after reboot for VM {vm_obj.name}"

        # Create PVC clones and attach them to opposite VMs
        log.info("Creating clones of PVCs")
        clone_obj_pvc = pvc_clone_factory(pvc_obj)
        clone_obj_dvt = pvc_clone_factory(dvt_obj)
        log.info(f"Clones of PVCs {pvc_obj.name} and {dvt_obj.name} created!")

        # Attach clones to the opposite VMs
        log.info(f"Attaching clone of {dvt_obj.name} to VM {vm_obj_pvc.name}")
        before_disks_pvc = vm_obj_pvc.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
        log.info(
            f"Disks before clone hotplug on VM {vm_obj_pvc.name}:\n{before_disks_pvc}"
        )
        vm_obj_pvc.addvolume(volume_name=clone_obj_dvt.name, persist=False)
        sample = TimeoutSampler(
            timeout=600,
            sleep=5,
            func=verify_hotplug,
            vm_obj=vm_obj_pvc,
            hot_pl_disk_raw=before_disks_pvc,
        )
        sample.wait_for_func_value(value=True)

        log.info(f"Attaching clone of {pvc_obj.name} to VM {vm_obj_dvt.name}")
        before_disks_dvt = vm_obj_dvt.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
        log.info(
            f"Disks before clone hotplug on VM {vm_obj_dvt.name}:\n{before_disks_dvt}"
        )
        vm_obj_dvt.addvolume(volume_name=clone_obj_pvc.name, persist=False)
        sample = TimeoutSampler(
            timeout=600,
            sleep=5,
            func=verify_hotplug,
            vm_obj=vm_obj_dvt,
            hot_pl_disk_raw=before_disks_dvt,
        )
        sample.wait_for_func_value(value=True)

        # Run I/O on the cloned disks
        log.info(f"Running I/O operation on cloned PVC {clone_obj_pvc.name}")
        run_dd_io(vm_obj=vm_obj_pvc, file_path=file_paths[1])
        log.info(f"Running I/O operation on cloned DVT {clone_obj_dvt.name}")
        run_dd_io(vm_obj=vm_obj_dvt, file_path=file_paths[1])

        # Unplug cloned disks and verify detachment
        log.info(f"Unplugging clone of {dvt_obj.name} from VM {vm_obj_pvc.name}")
        before_disks_pvc_rm = vm_obj_pvc.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
        log.info(
            f"Disks before unplugging clone from VM {vm_obj_pvc.name}:\n{before_disks_pvc_rm}"
        )
        vm_obj_pvc.removevolume(volume_name=clone_obj_dvt.name, verify=True)
        sample = TimeoutSampler(
            timeout=600,
            sleep=5,
            func=verify_hotplug,
            vm_obj=vm_obj_pvc,
            hot_pl_disk_raw=before_disks_pvc_rm,
        )
        sample.wait_for_func_value(value=True)

        log.info(f"Unplugging clone of {pvc_obj.name} from VM {vm_obj_dvt.name}")
        before_disks_dvt_rm = vm_obj_dvt.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
        log.info(
            f"Disks before unplugging clone from VM {vm_obj_dvt.name}:\n{before_disks_dvt_rm}"
        )
        vm_obj_dvt.removevolume(volume_name=clone_obj_pvc.name, verify=True)
        sample = TimeoutSampler(
            timeout=600,
            sleep=5,
            func=verify_hotplug,
            vm_obj=vm_obj_dvt,
            hot_pl_disk_raw=before_disks_dvt_rm,
        )
        sample.wait_for_func_value(value=True)

        # Stop the VMs after the test
        log.info(f"Stopping VM {vm_obj_pvc.name}")
        vm_obj_pvc.stop()

        log.info(f"Stopping VM {vm_obj_dvt.name}")
        vm_obj_dvt.stop()
