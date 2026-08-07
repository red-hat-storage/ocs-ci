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

logger = logging.getLogger(__name__)


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
        logger.info(f"Hotplugging PVC '{pvc.name}' to VM '{vm_obj.name}'")
        vm_obj.addvolume(volume_name=pvc.name)
        sample = TimeoutSampler(
            timeout=600,
            sleep=5,
            func=verify_hotplug,
            vm_obj=vm_obj,
            disks_before_hotplug=before_disks,
        )
        sample.wait_for_func_value(value=True)
        logger.info(f"PVC '{pvc.name}' hotplugged successfully to VM '{vm_obj.name}'")

        if not cross_pvc:
            logger.info(f"Running I/O on VM '{vm_obj.name}'")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            return source_csum
        else:
            logger.info(
                f"Running cross-PVC I/O on VM '{vm_obj.name}' with PVC '{pvc.name}'"
            )
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
        logger.info(f"Unplugging PVC '{pvc.name}' from VM '{vm_obj.name}'")
        vm_obj.removevolume(volume_name=pvc.name, persist=True, verify=True)
        logger.info(f"PVC '{pvc.name}' unplugged and verified for VM '{vm_obj.name}'")

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

        logger.test_step("Create StorageClass, VMs, and hotplug PVCs")
        sc_obj_def = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            new_rbd_pool=True,
            mapOptions="krbd:rxbounce",
            mounter="rbd",
        )

        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]

        vm_obj_pvc = cnv_workload(
            storageclass=sc_obj_def.name,
            namespace=proj_obj.namespace,
            volume_interface=constants.VM_VOLUME_PVC,
        )
        pvc_name = (f"pvc-hotplug-vm1-{vm_obj_pvc.name}")[:35]
        pvc_obj = pvc_factory(
            project=proj_obj,
            storageclass=sc_obj_def,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
            pvc_name=pvc_name,
        )
        logger.info(
            f"PVC-based VM '{vm_obj_pvc.name}' and PVC '{pvc_obj.name}' created"
        )

        vm_obj_dvt = cnv_workload(
            storageclass=sc_obj_def.name,
            namespace=proj_obj.namespace,
            volume_interface=constants.VM_VOLUME_DVT,
        )
        pvc_name = (f"pvc-hotplug-vm2-{vm_obj_dvt.name}")[:35]
        dvt_obj = pvc_factory(
            project=proj_obj,
            storageclass=sc_obj_def,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
            pvc_name=pvc_name,
        )
        logger.info(
            f"DVT-based VM '{vm_obj_dvt.name}' and PVC '{dvt_obj.name}' created"
        )

        vms_pvc = [(vm_obj_pvc, pvc_obj), (vm_obj_dvt, dvt_obj)]

        logger.test_step("Hotplug disks, run I/O, reboot, and verify persistence")
        for i, (vm_obj, pvc) in enumerate(vms_pvc):
            try:
                disks_before_hotplug = vm_obj.run_ssh_cmd(
                    "lsblk -o NAME,SIZE,MOUNTPOINT -P"
                )
                logger.debug(
                    f"Disks before hotplug on VM '{vm_obj.name}':\n{disks_before_hotplug}"
                )

                source_csum = self.hotplug_and_run_io(
                    vm_obj, pvc, file_paths, disks_before_hotplug
                )

                logger.info(f"Rebooting VM '{vm_obj.name}'")
                vm_obj.restart()
                logger.info(f"VM '{vm_obj.name}' rebooted successfully")

                volume_attached = verifyvolume(
                    vm_obj.name, volume_name=pvc.name, namespace=vm_obj.namespace
                )
                logger.assertion(
                    f"Volume attached after reboot: vm='{vm_obj.name}', "
                    f"volume='{pvc.name}', expected=True, actual={volume_attached}"
                )
                assert (
                    volume_attached
                ), f"Volume '{pvc.name}' not found on VM '{vm_obj.name}' after reboot"

                new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
                logger.assertion(
                    f"Data persistence after reboot: vm='{vm_obj.name}', "
                    f"expected='{source_csum}', actual='{new_csum}', "
                    f"match={source_csum == new_csum}"
                )
                assert (
                    source_csum == new_csum
                ), f"MD5 mismatch after reboot for VM '{vm_obj.name}'"
            except Exception as e:
                logger.exception(
                    f"Hotplug and I/O operations failed on VM '{vm_obj.name}': {e}"
                )
                raise

        logger.test_step("Create PVC clones and attach to opposite VMs")
        try:
            clone_obj_pvc = pvc_clone_factory(
                pvc_obj, clone_name=f"clone-{pvc_obj.name}"
            )
            clone_obj_dvt = pvc_clone_factory(
                dvt_obj, clone_name=f"clone-{dvt_obj.name}"
            )
            logger.info(
                f"Created clones: '{pvc_obj.name}' -> '{clone_obj_pvc.name}', "
                f"'{dvt_obj.name}' -> '{clone_obj_dvt.name}'"
            )

            logger.info(
                f"Attaching clone '{clone_obj_dvt.name}' to VM '{vm_obj_pvc.name}'"
            )
            before_disks_pvc = vm_obj_pvc.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )

            self.hotplug_and_run_io(
                vm_obj_pvc, clone_obj_dvt, file_paths, before_disks_pvc, cross_pvc=True
            )

            logger.info(
                f"Attaching clone '{clone_obj_pvc.name}' to VM '{vm_obj_dvt.name}'"
            )
            before_disks_dvt = vm_obj_dvt.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )

            self.hotplug_and_run_io(
                vm_obj_dvt, clone_obj_pvc, file_paths, before_disks_dvt, cross_pvc=True
            )

        except Exception as e:
            logger.exception(f"PVC cloning and cross-VM hotplug failed: {e}")
            raise

        logger.test_step("Unplug all hotplugged disks and verify detachment")
        try:
            self.unplug_disks_and_verify(vm_obj_pvc, clone_obj_dvt)
            self.unplug_disks_and_verify(vm_obj_dvt, clone_obj_pvc)

            for i, (vm_obj, pvc) in enumerate(vms_pvc):
                self.unplug_disks_and_verify(vm_obj, pvc)
            logger.info("All hotplugged disks unplugged and verified")
        except Exception as e:
            logger.exception(f"PVC unplug failed: {e}")
            raise
