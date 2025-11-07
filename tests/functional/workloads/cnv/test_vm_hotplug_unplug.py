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
@pytest.mark.polarion_id("OCS-6322")
class TestVmHotPlugUnplug(E2ETest):
    """
    Test case for VM hot plugging and unplugging of PVC disks.
    This test ensures that PVC disks can be hotplugged into a running VM
    and that data written to the disk is persisted after reboot.
    """

    def test_vm_hot_plugging_unplugging(
        self,
        setup_cnv,
        project_factory,
        multi_cnv_workload,
        pvc_factory,
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
        vm_objs_def, vm_objs_aggr, sc_objs_def, sc_objs_aggr = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        log.info(f"Total VMs to process: {len(vm_list)}")

        for vm_obj in vm_list:
            sc_obj = sc_objs_def if vm_obj in vm_objs_def else sc_objs_aggr
            before_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            log.info(f"Disks before hotplug:\n{before_disks}")
            pvc_name = (f"pvc-hotplug-1-{vm_obj.name}")[:35]
            pvc_obj = pvc_factory(
                project=proj_obj,
                storageclass=sc_obj,
                size=20,
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
                pvc_name=pvc_name,
            )
            log.info(f"PVC {pvc_obj.name} created successfully")

            vm_obj.addvolume(volume_name=pvc_obj.name)
            log.info(f"Hotplugged PVC {pvc_obj.name} to VM {vm_obj.name}")

            sample = TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=vm_obj,
                disks_before_hotplug=before_disks,
            )
            sample.wait_for_func_value(value=True)

            log.info(f"Running I/O operation on VM {vm_obj.name}")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            log.info(f"Rebooting VM {vm_obj.name}")
            vm_obj.restart(wait=True, verify=True)
            log.info(f"Reboot Success for VM: {vm_obj.name}")

            assert verifyvolume(
                vm_obj.name, volume_name=pvc_obj.name, namespace=vm_obj.namespace
            ), f"Unable to find volume {pvc_obj.name} mounted on VM: {vm_obj.name}"

            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"MD5 mismatch after reboot for VM {vm_obj.name}"
            pvc_name = (f"pvc-hotplug-1-{vm_obj.name}")[:35]
            pvc_obj_wout = pvc_factory(
                project=proj_obj,
                storageclass=sc_obj,
                size=20,
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
                pvc_name=pvc_name,
            )
            log.info(f"PVC {pvc_obj_wout.name} created successfully")
            before_disks_wout = vm_obj.run_ssh_cmd(
                "lsblk -o NAME,SIZE," "MOUNTPOINT -P"
            )
            log.info(f"Disks before hotplug (without persist):\n{before_disks_wout}")

            vm_obj.addvolume(volume_name=pvc_obj_wout.name, persist=False, verify=False)

            sample = TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=vm_obj,
                disks_before_hotplug=before_disks_wout,
            )
            sample.wait_for_func_value(value=True)

            log.info(f"Running I/O operation on VM {vm_obj.name}")
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])

            before_disks_wout_rm = vm_obj.run_ssh_cmd(
                "lsblk -o NAME,SIZE," "MOUNTPOINT -P"
            )
            vm_obj.removevolume(volume_name=pvc_obj_wout.name)

            sample = TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=vm_obj,
                disks_before_hotplug=before_disks_wout_rm,
            )
            sample.wait_for_func_value(value=True)
            vm_obj.stop()
