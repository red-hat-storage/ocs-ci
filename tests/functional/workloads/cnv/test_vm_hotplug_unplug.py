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
    Tests for VM hot plugging and unplugging
    Test Steps:
        Pre-requisite before hot plugging:
        - a running DVT based VM and a PVC based VM
        - From virtctl, we will need to add --persist flag to add the hot plugged disk
        to the VM as a permanently mounted virtual disk.

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

    def test_vm_hot_plugging_unplugging(
        self, project_factory, multi_cnv_workload, cnv_workload
    ):
        """
        Test for disk Hot Plugging and Hot Unplugging for DVT and PVC VMs

        """
        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        log.info(f"Total VMs to process: {len(vm_list)}")
        for index, vm_obj in enumerate(vm_list):
            log.info(
                f"Starting I/O operation on VM {vm_obj.name} using "
                f"{file_paths[0]}..."
            )
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[0])
            # creating the hotplug
            pvc_obj = create_pvc(
                sc_name=constants.DEFAULT_CNV_CEPH_RBD_SC,
                namespace=vm_obj.namespace,
                size="20Gi",
                access_mode=constants.ACCESS_MODE_RWX,
                volume_mode=constants.VOLUME_MODE_BLOCK,
            )
            # adding the hotplug with taking care of persist flag
            vm_obj.addvolume(volume_name=pvc_obj.name)
            # identify the newly attached pvc
            verifyvolume(vm_obj, volume_name=pvc_obj.name)
            # adding data onto the disk attached
            pvc_obj.run_ssh_cmd(
                command="dd if=/dev/zero of=/dd_file.txt bs=1024 count=102400"
            )
            # rebooting the vm
            vm_obj.restart_vm()
            # verify the disk attached
            verifyvolume(vm_obj, volume_name=pvc_obj.name)
            # checking if the data is persistent or not
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and cloned {vm_obj.name} VMs"
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
            vm_obj.removevolume(volume_name=pvc_obj.name)
            if not vm_obj.verify_volume(volume_name=pvc_obj.name):
                log.info("Volume Unplug Successful")
