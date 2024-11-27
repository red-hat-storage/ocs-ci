import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@pytest.mark.polarion_id("OCS-5241")
class TestVmSnapshotClone(E2ETest):
    """
    Tests related VM snapshots and clones
    """

    def test_vm_clone(self, cnv_workload, clone_vm_workload, setup_cnv):
        """
        This test performs the VM cloning and IO on different volume interfaces

        Steps:
        1) Create a VM using PVC/DV/DVT
        2) Write IOs on source VMs
        3) Clone the source VMs and validate it
        4) Validate data in cloned VM is same a source VM
        5) Write new data on source VMs
        6) Stop the VM
        7) Delete the Cloned and source VMs

        """
        file_paths = ["original_file.txt, new_file.txt"]
        volume_interface = [
            constants.VM_VOLUME_PVC,
            constants.VM_VOLUME_DV,
            constants.VM_VOLUME_DVT,
        ]
        for index, vl_if in enumerate(volume_interface):
            vm_obj = cnv_workload(
                volume_interface=vl_if, source_url=constants.CNV_FEDORA_SOURCE
            )[index]
            original_csum = run_dd_io(
                vm_obj=vm_obj, file_path=file_paths[0], verify=True
            )
            vm_obj.stop()
            clone_obj = clone_vm_workload(
                vm_obj=vm_obj,
                volume_interface=vl_if,
                namespace=(
                    vm_obj.namespace if vl_if == constants.VM_VOLUME_PVC else None
                ),
            )[index]
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
            new_csum = cal_md5sum_vm(vm_obj=clone_obj, file_path=file_paths[0])
            assert (
                original_csum == new_csum
            ), f"Failed: MD5 comparison between original {vm_obj.name} and cloned {clone_obj.name} VMs"
            clone_obj.stop()
