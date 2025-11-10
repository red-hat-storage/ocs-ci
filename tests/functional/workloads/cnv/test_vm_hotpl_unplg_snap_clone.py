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
@pytest.mark.polarion_id("OCS-7299")
class TestVmHotPlugUnplugSnapClone(E2ETest):
    """
    Test case for snapshot and clones
    with hotplug/unplug
    """

    def hotplug_and_run_io(
        self, vm_obj, pvc, file_paths, before_disks, cross_pvc=False
    ):
        """
        Hotplugs a PVC to a VM and runs I/O operations.

        This function handles the hotplugging of a Persistent Volume Claim (PVC) to a virtual machine (VM)
        and performs I/O operations on the newly attached disk.

        Args:
            vm_obj (cnv_workload): The virtual machine object to which the PVC will be hotplugged.
            pvc (pvc_object): The Persistent Volume Claim object to be hotplugged.
            file_paths (list): A list of file paths for I/O operations.
            before_disks (str): The output of 'lsblk -o NAME,SIZE,MOUNTPOINT -P' before hotplugging.
            cross_pvc (bool, optional): If True, indicates that the I/O operation is for a pvc of pvc
                                        based vm to dvt based VM and vice a versa. Defaults to False.

        Returns:
            str: The MD5 checksum of the source file after I/O operation if cross_pvc is False.

        Raises:
            Exception: If there is an error during hotplugging or I/O operation.
        """
        # Hotplug the PVC volume to the VM
        log.info(f"Hotplugging PVC {pvc.name} to VM {vm_obj.name}")
        vm_obj.addvolume(volume_name=pvc.name)
        # Wait for the disk to be hotplugged successfully
        sample = TimeoutSampler(
            timeout=600,
            sleep=5,
            func=verify_hotplug,
            vm_obj=vm_obj,
            disks_before_hotplug=before_disks,
        )
        sample.wait_for_func_value(value=True)
        log.info(f"Hotplugged PVC {pvc.name} to VM {vm_obj.name}")

        if not cross_pvc:
            # Run I/O operation
            log.info(f"Running I/O operation on VM {vm_obj.name}")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            return source_csum
        else:
            log.info(f"Running I/O operation {pvc.name}")
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1], verify=True)

    def unplug_disks_and_verify(self, vm_obj, pvc):
        """
        Removes a PVC from the specified VM and verifies its detachment.

        Args:
            vm_obj (CnvWorkload): The VM object from which to remove the PVC.
            pvc (Pvc): The PVC object to be removed from the VM.

        Returns:
            None
        """
        vm_obj.removevolume(volume_name=pvc.name, persist=True, verify=True)

    def test_vm_hotpl_unplg_snap_clone(
        self,
        setup_cnv,
        storageclass_factory,
        project_factory,
        cnv_workload,
        pvc_factory,
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

        # Create an encryption enabled storageclass for RBD
        sc_obj_def = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            new_rbd_pool=True,
            mapOptions="krbd:rxbounce",
            mounter="rbd",
        )

        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]

        # Create a PVC-based VM (VM1)
        vm_obj_pvc = cnv_workload(
            storageclass=sc_obj_def.name,
            namespace=proj_obj.namespace,
            volume_interface=constants.VM_VOLUME_PVC,
        )
        pvc_name = (f"pvc-hotplug-vm1-{vm_obj_pvc.name}")[:35]
        # Create the PVC for VM1
        pvc_obj = pvc_factory(
            project=proj_obj,
            storageclass=sc_obj_def,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
            pvc_name=pvc_name,
        )
        log.info(f"PVC {pvc_obj.name} created successfully")

        # Create a DVT-based VM (VM2)
        vm_obj_dvt = cnv_workload(
            storageclass=sc_obj_def.name,
            namespace=proj_obj.namespace,
            volume_interface=constants.VM_VOLUME_DVT,
        )
        pvc_name = (f"pvc-hotplug-vm2-{vm_obj_dvt.name}")[:35]
        # Create the PVC for VM2
        dvt_obj = pvc_factory(
            project=proj_obj,
            storageclass=sc_obj_def,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
            pvc_name=pvc_name,
        )
        log.info(f"PVC {dvt_obj.name} created successfully")

        # List of VM-PVC pairs for hotplug testing
        vms_pvc = [(vm_obj_pvc, pvc_obj), (vm_obj_dvt, dvt_obj)]

        # Hotplug disks and perform I/O operations
        for i, (vm_obj, pvc) in enumerate(vms_pvc):
            try:
                # Verify disks before hotplugging
                disks_before_hotplug = vm_obj.run_ssh_cmd(
                    "lsblk -o NAME,SIZE,MOUNTPOINT -P"
                )
                log.info(
                    f"Disks before hotplug on VM {vm_obj.name}:\n{disks_before_hotplug}"
                )

                source_csum = self.hotplug_and_run_io(
                    vm_obj, pvc, file_paths, disks_before_hotplug
                )

                # Reboot the VM
                log.info(f"Rebooting VM {vm_obj.name}")
                vm_obj.restart()
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
            except Exception as e:
                log.error(
                    f"An error occurred during hotplugging and I/O operations on VM {vm_obj.name}: {str(e)}"
                )
                raise

        # Create PVC clones and attach them to opposite VMs
        try:
            clone_obj_pvc = pvc_clone_factory(
                pvc_obj, clone_name=f"clone-{pvc_obj.name}"
            )
            clone_obj_dvt = pvc_clone_factory(
                dvt_obj, clone_name=f"clone-{dvt_obj.name}"
            )
            log.info(
                f"Clones of PVCs {pvc_obj.name}:{clone_obj_pvc.name} and "
                f"{dvt_obj.name}:{clone_obj_dvt.name} created!"
            )

            # Attach clones to the opposite VMs
            log.info(f"Attaching clone of {dvt_obj.name} to VM {vm_obj_pvc.name}")
            before_disks_pvc = vm_obj_pvc.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(
                f"Disks before clone hotplug on VM {vm_obj_pvc.name}:\n{before_disks_pvc}"
            )

            self.hotplug_and_run_io(
                vm_obj_pvc, clone_obj_dvt, file_paths, before_disks_pvc, cross_pvc=True
            )

            log.info(f"Attaching clone of {pvc_obj.name} to VM {vm_obj_dvt.name}")
            before_disks_dvt = vm_obj_dvt.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            log.info(
                f"Disks before clone hotplug on VM {vm_obj_dvt.name}:\n{before_disks_dvt}"
            )

            self.hotplug_and_run_io(
                vm_obj_dvt, clone_obj_pvc, file_paths, before_disks_dvt, cross_pvc=True
            )

        except Exception as e:
            log.error(f"An error occurred during PVC cloning and hotplugging: {str(e)}")
            raise

        try:
            # Unplug cloned disks and verify detachment
            log.info(f"Unplugging clone of {dvt_obj.name} from VM {vm_obj_pvc.name}")
            self.unplug_disks_and_verify(vm_obj_pvc, clone_obj_dvt)

            log.info(f"Unplugging clone of {pvc_obj.name} from VM {vm_obj_dvt.name}")
            self.unplug_disks_and_verify(vm_obj_dvt, clone_obj_pvc)

            # Unplug normal disks and verify detachment
            for i, (vm_obj, pvc) in enumerate(vms_pvc):
                self.unplug_disks_and_verify(vm_obj, pvc)
        except Exception as e:
            log.error(f"An error occurred during PVC Unplugging: {str(e)}")
            raise
