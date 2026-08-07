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

        logger.test_step("Create project and deploy CNV workload VMs")
        proj_obj = project_factory()
        file_paths = ["/file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, sc_objs_def, sc_objs_aggr = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        logger.info(f"Created {len(vm_list)} VMs for hotplug testing")

        logger.test_step(
            "Hotplug persistent PVC, run I/O, reboot, and verify data persistence per VM"
        )
        for vm_obj in vm_list:
            sc_obj = sc_objs_def if vm_obj in vm_objs_def else sc_objs_aggr
            before_disks = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            logger.debug(f"Disks before hotplug on VM '{vm_obj.name}':\n{before_disks}")
            pvc_name = (f"pvc-hotplug-1-{vm_obj.name}")[:35]
            pvc_obj = pvc_factory(
                project=proj_obj,
                storageclass=sc_obj,
                size=20,
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
                pvc_name=pvc_name,
            )

            logger.info(f"Hotplugging PVC '{pvc_obj.name}' to VM '{vm_obj.name}'")
            vm_obj.addvolume(volume_name=pvc_obj.name)

            sample = TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=vm_obj,
                disks_before_hotplug=before_disks,
            )
            sample.wait_for_func_value(value=True)
            logger.info(
                f"PVC '{pvc_obj.name}' hotplugged successfully to VM '{vm_obj.name}'"
            )

            logger.info(f"Running I/O on VM '{vm_obj.name}'")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            logger.info(f"Rebooting VM '{vm_obj.name}'")
            vm_obj.restart(wait=True, verify=True)
            logger.info(f"VM '{vm_obj.name}' rebooted successfully")

            volume_attached = verifyvolume(
                vm_obj.name, volume_name=pvc_obj.name, namespace=vm_obj.namespace
            )
            logger.assertion(
                f"Volume attached after reboot: vm='{vm_obj.name}', "
                f"volume='{pvc_obj.name}', expected=True, actual={volume_attached}"
            )
            assert (
                volume_attached
            ), f"Volume '{pvc_obj.name}' not found on VM '{vm_obj.name}' after reboot"

            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            logger.assertion(
                f"Data persistence after reboot: vm='{vm_obj.name}', "
                f"expected='{source_csum}', actual='{new_csum}', "
                f"match={source_csum == new_csum}"
            )
            assert (
                source_csum == new_csum
            ), f"MD5 mismatch after reboot for VM '{vm_obj.name}'"

            pvc_name = (f"pvc-hotplug-2-{vm_obj.name}")[:35]
            pvc_obj_wout = pvc_factory(
                project=proj_obj,
                storageclass=sc_obj,
                size=20,
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
                pvc_name=pvc_name,
            )

            before_disks_wout = vm_obj.run_ssh_cmd("lsblk -o NAME,SIZE,MOUNTPOINT -P")
            logger.debug(
                f"Disks before non-persistent hotplug on VM '{vm_obj.name}':\n{before_disks_wout}"
            )

            logger.info(
                f"Hotplugging PVC '{pvc_obj_wout.name}' to VM '{vm_obj.name}' without persist"
            )
            vm_obj.addvolume(volume_name=pvc_obj_wout.name, persist=False, verify=False)

            sample = TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=vm_obj,
                disks_before_hotplug=before_disks_wout,
            )
            sample.wait_for_func_value(value=True)

            logger.info(f"Running I/O on non-persistent disk of VM '{vm_obj.name}'")
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])

            before_disks_wout_rm = vm_obj.run_ssh_cmd(
                "lsblk -o NAME,SIZE,MOUNTPOINT -P"
            )
            logger.info(f"Unplugging PVC '{pvc_obj_wout.name}' from VM '{vm_obj.name}'")
            vm_obj.removevolume(volume_name=pvc_obj_wout.name)

            sample = TimeoutSampler(
                timeout=600,
                sleep=5,
                func=verify_hotplug,
                vm_obj=vm_obj,
                disks_before_hotplug=before_disks_wout_rm,
            )
            sample.wait_for_func_value(value=True)
            logger.info(
                f"PVC '{pvc_obj_wout.name}' unplugged and verified for VM '{vm_obj.name}'"
            )

            logger.info(f"Stopping VM '{vm_obj.name}'")
            vm_obj.stop()
        logger.info("Hotplug/unplug testing completed for all VMs")
