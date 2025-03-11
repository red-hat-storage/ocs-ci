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

        # Create VMs
        vm_obj_pvc = cnv_workload(
            namespace=proj_obj.namespace, volume_interface=constants.VM_VOLUME_PVC
        )
        vm_obj_dvt = cnv_workload(
            namespace=proj_obj.namespace, volume_interface=constants.VM_VOLUME_DVT
        )

        dvt_obj = create_pvc(
            sc_name=vm_obj_dvt.sc_name,
            namespace=vm_obj_dvt.namespace,
            size="20Gi",
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        pvc_obj = create_pvc(
            sc_name=vm_obj_pvc.sc_name,
            namespace=vm_obj_pvc.namespace,
            size="20Gi",
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )

        log.info(f"PVC {dvt_obj.name} and {pvc_obj.name} created successfully")

        # List of VM-Object and PVC pairs for hotplug testing
        vms_pvc = [(vm_obj_pvc, pvc_obj), (vm_obj_dvt, dvt_obj)]
        before_disks_hotpl = []
        for vm_obj, pvc in vms_pvc:
            i = 0
            # Verify disks before hotplugging
            before_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks before hotplug on VM {vm_obj.name}:\n{before_disks}")
            before_disks_hotpl[i] = before_disks

            # Hotplug the PVC volume
            vm_obj.addvolume(volume_name=pvc.name, verify=True)
            log.info(f"Hotplugged PVC {pvc.name} to VM {vm_obj.name}")
            time.sleep(30)

            # Verify disks after hotplugging
            after_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks after hotplug on VM {vm_obj.name}:\n{after_disks}")

            # Run I/O operation on the new disk
            log.info(f"Running I/O operation on VM {vm_obj.name}")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            # Reboot the VM and verify disk persistence
            log.info(f"Rebooting VM {vm_obj.name}")
            vm_obj.restart(wait=True, verify=True)
            log.info(f"Reboot Success for VM: {vm_obj.name}")

            # Verify disk is still attached
            assert verifyvolume(
                vm_obj.name, volume_name=pvc.name, namespace=vm_obj.namespace
            ), f"Unable to find volume {pvc.name} mounted on VM: {vm_obj.name}"

            # Verify data persistence by checking MD5 checksum
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"MD5 mismatch after reboot for VM {vm_obj.name}"

        # Create PVC clones
        clone_obj_pvc = pvc_clone_factory(pvc_obj)
        clone_obj_dvt = pvc_clone_factory(dvt_obj)
        log.info(f"Clones of PVCs {pvc_obj.name} and {dvt_obj.name} created!")

        # Attach clones to the opposite VMs

        # Verify doesn't work as temp hotplug is
        # not visible inside VM yaml
        vm_obj_pvc.addvolume(volume_name=clone_obj_dvt.name, persist=False)
        vm_obj_dvt.addvolume(volume_name=clone_obj_pvc.name, persist=False)

        # Run I/O on the cloned disks
        run_dd_io(vm_obj=vm_obj_pvc, file_path=file_paths[0])
        run_dd_io(vm_obj=vm_obj_dvt, file_path=file_paths[0])

        # Unplug the cloned disks and verify detachment
        vm_obj_pvc.removevolume(volume_name=clone_obj_dvt.name, verify=True)
        vm_obj_dvt.removevolume(volume_name=clone_obj_pvc.name, verify=True)

        # Verify disk detachment for both VMs
        after_disks_hotpl_pvc = vm_obj_pvc.run_ssh_cmd(
            "lsblk -o NAME,SIZE,MOUNTPOINT -P"
        )
        after_disks_hotpl_dvt = vm_obj_dvt.run_ssh_cmd(
            "lsblk -o NAME,SIZE,MOUNTPOINT -P"
        )

        log.info(
            f"Disks after removing clone hotplug from PVC VM:\n{after_disks_hotpl_pvc}"
        )
        log.info(
            f"Disks after removing clone hotplug from DVT VM:\n{after_disks_hotpl_dvt}"
        )

        # Verify that the disks are detached properly
        assert (
            after_disks_hotpl_pvc == before_disks_hotpl[0]
        ), f"Hotplug removal failed on PVC VM {vm_obj_pvc} after clone removal"
        assert (
            after_disks_hotpl_dvt == before_disks_hotpl[1]
        ), f"Hotplug removal failed on DVT VM {vm_obj_dvt} after clone removal"
