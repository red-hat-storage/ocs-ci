import logging
import re

import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import (
    cal_md5sum_vm,
    run_dd_io,
    verifyvolume,
    verify_hotplug,
    add_fs_mount_hotplug,
)
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-")
class TestVmHotPlugUnplugSnapClone(E2ETest):
    """
    Test case for snapshot and clones
    with hotplug/unplug
    """

    def test_vm_hotpl_snap_clone(
        self,
        # setup_cnv,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        project_factory,
        cnv_workload,
        pvc_factory,
        pvc_clone_factory,
    ):
        """
        A running DVT-based VM and a PVC-based VM
        Steps:
        1. Hotplug disk to the running VM based on PVC.
        2. Verify the disk is attached to VM.
        3. Add data to disk.
        4. Reboot the VM, verify disk is still attached, check data integrity.
        5. Create clones of hotplugged PVCs.
        6. Attach clones to opposite VMs and verify disk operation.
        7. Unplug the disks and verify detachment.
        """
        self.vm_mt_path = {}
        log.info("Setting up csi-kms-connection-details configmap")
        kms = pv_encryption_kms_setup_factory(kv_version="v2")
        log.info("csi-kms-connection-details setup successful")

        sc_obj_def = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=kms.kmsid,
            new_rbd_pool=True,
            mapOptions="krbd:rxbounce",
            mounter="rbd",
        )

        proj_obj = project_factory()
        kms.vault_path_token = kms.generate_vault_token()
        kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)
        pvk_obj = PVKeyrotation(sc_obj_def)
        pvk_obj.annotate_storageclass_key_rotation(schedule="*/3 * * * *")

        file_paths = ["/source_file.txt", "/new_file.txt"]

        vm_obj_pvc = cnv_workload(
            storageclass=sc_obj_def.name,
            namespace=proj_obj.namespace,
            volume_interface=constants.VM_VOLUME_PVC,
        )

        pvc_obj = pvc_factory(
            project=proj_obj,
            storageclass=sc_obj_def,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        log.info(f"PVC {pvc_obj.name} created successfully")

        vm_obj_dvt = cnv_workload(
            storageclass=sc_obj_def.name,
            namespace=proj_obj.namespace,
            volume_interface=constants.VM_VOLUME_DVT,
        )

        dvt_obj = pvc_factory(
            project=proj_obj,
            storageclass=sc_obj_def,
            size=20,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        log.info(f"PVC {dvt_obj.name} created successfully")

        vms_pvc = [(vm_obj_pvc, pvc_obj), (vm_obj_dvt, dvt_obj)]
        source_csum = {}

        # Hotplug disks, write data and verify integrity
        for vm_obj, pvc in vms_pvc:
            before_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks before hotplug on VM {vm_obj.name}:\n{before_disks}")

            log.info(f"Hotplugging PVC {pvc.name} to VM {vm_obj.name}")
            vm_obj.addvolume(volume_name=pvc.name)

            TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=vm_obj,
                disks_before_hotplug=before_disks,
            ).wait_for_func_value(value=True)
            log.info(f"Hotplugged PVC {pvc.name} to VM {vm_obj.name}")

            after_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks after hotplug on VM {vm_obj.name}:\n{after_disks}")

            added_disks = set(re.findall(r'NAME="([^"]+)"', after_disks)) - set(
                re.findall(r'NAME="([^"]+)"', before_disks)
            )

            if added_disks:
                added_disks_str = "".join(added_disks)

                log.info(f"Added disks: {added_disks_str}")
                self.vm_mt_path[pvc.name] = add_fs_mount_hotplug(
                    vm_obj=vm_obj, hotpl_new_disk=added_disks_str
                )
                log.info(
                    f"Mounted new disk(s) {added_disks_str} at {self.vm_mt_path[pvc.name]}"
                )

            log.info(f"Running I/O operation on VM {vm_obj.name} and PVC {pvc.name}")
            mount_path = self.vm_mt_path.get(pvc.name)
            if not mount_path:
                log.error(f"Mount path not found for {pvc.name}")
                assert False, f"Mount path not found for {pvc.name}"

            source_file_path = mount_path + file_paths[0]
            source_csum[pvc.name] = run_dd_io(
                vm_obj=vm_obj, file_path=source_file_path, verify=True
            )

            log.info(f"Rebooting VM {vm_obj.name}")
            vm_obj.restart()
            log.info(f"Reboot Success for VM: {vm_obj.name}")

            assert verifyvolume(
                vm_obj.name, volume_name=pvc.name, namespace=vm_obj.namespace
            ), f"Unable to find volume {pvc.name} mounted on VM: {vm_obj.name}"

            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=source_file_path)
            assert (
                source_csum[pvc.name] == new_csum
            ), f"MD5 mismatch after reboot for VM {vm_obj.name}"

        clone_obj_pvc = pvc_clone_factory(pvc_obj)
        clone_obj_dvt = pvc_clone_factory(dvt_obj)
        log.info(
            f"Clones of PVCs {pvc_obj.name}:{clone_obj_pvc.name} and "
            f"{dvt_obj.name}:{clone_obj_dvt.name} created!"
        )

        clone_map = {
            clone_obj_dvt: (vm_obj_pvc, dvt_obj),
            clone_obj_pvc: (vm_obj_dvt, pvc_obj),
        }

        for clone_obj, (target_vm, orig_pvc) in clone_map.items():
            log.info(f"Attaching clone {clone_obj.name} to VM {target_vm.name}")
            before_disks = target_vm.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            target_vm.addvolume(volume_name=clone_obj.name, persist=False, verify=False)

            TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=target_vm,
                disks_before_hotplug=before_disks,
            ).wait_for_func_value(value=True)

            after_disks = target_vm.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks after plugging to VM {target_vm.name}:\n{after_disks}")

            # File is not present at the mount path after clone and
            # attach (sba -> mount point empty)

            assert source_csum[orig_pvc.name] == cal_md5sum_vm(
                vm_obj=target_vm,
                file_path=self.vm_mt_path[orig_pvc.name] + file_paths[0],
            ), f"MD5 mismatch on cloned volume {clone_obj.name} on VM {target_vm.name}"

        log.info(f"Running I/O operation {clone_obj_pvc.name}")
        run_dd_io(vm_obj=vm_obj_pvc, file_path=file_paths[1])

        log.info(f"Running I/O operation {clone_obj_dvt.name}")
        run_dd_io(vm_obj=vm_obj_dvt, file_path=file_paths[1])

        log.info(f"Unplugging clone of {dvt_obj.name} from VM {vm_obj_pvc.name}")
        before_disks_pvc_rm = vm_obj_pvc.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
        log.info(
            f"Disks before unplugging from VM {vm_obj_pvc.name}:\n{before_disks_pvc_rm}"
        )
        vm_obj_pvc.removevolume(volume_name=clone_obj_dvt.name, verify=False)

        TimeoutSampler(
            timeout=600,
            sleep=5,
            func=verify_hotplug,
            vm_obj=vm_obj_pvc,
            disks_before_hotplug=before_disks_pvc_rm,
        ).wait_for_func_value(value=True)

        log.info(f"Unplugging clone of {pvc_obj.name} from VM {vm_obj_dvt.name}")
        before_disks_dvt_rm = vm_obj_dvt.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
        log.info(
            f"Disks before unplugging from VM {vm_obj_dvt.name}:\n{before_disks_dvt_rm}"
        )
        vm_obj_dvt.removevolume(volume_name=clone_obj_pvc.name, verify=False)

        TimeoutSampler(
            timeout=600,
            sleep=5,
            func=verify_hotplug,
            vm_obj=vm_obj_dvt,
            disks_before_hotplug=before_disks_dvt_rm,
        ).wait_for_func_value(value=True)

        log.info(f"Stopping VM {vm_obj_pvc.name}")
        vm_obj_pvc.stop()

        log.info(f"Stopping VM {vm_obj_dvt.name}")
        vm_obj_dvt.stop()
