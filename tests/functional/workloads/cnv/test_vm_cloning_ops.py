import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
class TestVmSnapshotClone(E2ETest):
    """
    Tests related VM snapshots and clones
    """

    @workloads
    @pytest.mark.polarion_id("OCS-6288")
    def test_vm_clone(self, cnv_workload, clone_vm_workload, setup_cnv):
        """
        This test performs the VM cloning and IOs created using different volume interfaces(PVC/DV/DVT)

        Test steps:
        1. Create a clone of a VM PVC by following the documented procedure from ODF official docs.
            1.1 Create clone of the pvc associated with VM.
            1.2 Cloned pvc successfully created and listed
        2. Verify the cloned PVc is created.
        3. create vm using cloned pvc.
        4. Verify that the data on VM backed by cloned pvc is same as that in the original VM.
        5. Add additional data to the cloned VM.
        6. Delete the clone by following the documented procedure from ODF official docs
         6.1 Delete clone of the pvc associated with VM.
         6.2 cloned pvc successfully deleted
        7. Repeat the above procedure for all the VMs in the system
        8. Delete all the clones created as part of this test
        """

        file_paths = ["/source_file.txt", "/new_file.txt"]
        # TODO: Add multi_cnv fixture to configure VMs based on specifications
        volume_interface = [
            constants.VM_VOLUME_PVC,
            constants.VM_VOLUME_DV,
            constants.VM_VOLUME_DVT,
        ]
        for index, vl_if in enumerate(volume_interface):
            vm_obj = cnv_workload(
                volume_interface=vl_if, source_url=constants.CNV_FEDORA_SOURCE
            )[index]
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            vm_obj.stop()
            clone_obj = clone_vm_workload(
                vm_obj=vm_obj,
                volume_interface=vl_if,
                namespace=(
                    vm_obj.namespace if vl_if == constants.VM_VOLUME_PVC else None
                ),
            )[index]
            new_csum = cal_md5sum_vm(vm_obj=clone_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and cloned {clone_obj.name} VMs"
            run_dd_io(vm_obj=clone_obj, file_path=file_paths[1])
            clone_obj.stop()
