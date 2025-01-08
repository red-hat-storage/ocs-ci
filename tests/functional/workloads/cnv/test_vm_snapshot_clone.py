import logging
from ocs_ci.framework.pytest_customization.marks import workloads, magenta_squad
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@magenta_squad
@workloads
class TestVMSnapshotClone(E2ETest):
    """
    Tests for VM PVC Expansion
    """

    def test_vm_snapshot_clone(
        self,
        cnv_workload,
        snapshot_factory,
        snapshot_restore_factory,
        pvc_clone_factory,
    ):
        """
        Creates a snapshot of a deployed VM, restores the snapshot, and then clones the restored PVC.

        Test Steps:
        1. Create a VM with PVC
        2. Write data to the VM and stop it
        3. Take a snapshot of the VM's PVC
        4. Restore the snapshot to a new PVC
        5. Clone the restored snapshot PVC
        6. Create a new VM with the cloned PVC
        7. Verify data integrity in the cloned VM 
        8. Verify that the data persisted after cloning
        """

        # create a VM
        vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
        )[-1]

        # Write data to the VM
        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
        vm_obj.stop()

        # Take a snapshot of the VM's PVC
        pvc_obj = vm_obj.get_vm_pvc_obj()
        snap_obj = snapshot_factory(pvc_obj)

        # Restore the snapshot to a new PVC
        res_snap_obj = snapshot_restore_factory(
            snapshot_obj=snap_obj,
            storageclass=vm_obj.sc_name,
            size=vm_obj.pvc_size,
            volume_mode=snap_obj.parent_volume_mode,
            access_mode=vm_obj.pvc_access_mode,
            status=constants.STATUS_BOUND,
            timeout=300,
        )
        # Clone the restored snapshot PVC to create a new PVC
        cloned_pvc_obj = pvc_clone_factory(
            pvc_obj=res_snap_obj, clone_name=f"{res_snap_obj.name}-clone"
        )
        # Create a new VM with the cloned PVC
        res_vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
            pvc_obj=cloned_pvc_obj,
            namespace=vm_obj.namespace,
        )[1]
        # Verify data integrity in the cloned VM
        run_dd_io(vm_obj=res_vm_obj, file_path=file_paths[1], verify=True)
        # Check the MD5 checksum to verify that data persisted after cloning
        res_csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
        assert source_csum == res_csum, (
            f"Failed: MD5 comparison between source {vm_obj.name} and cloned "
            f"{res_vm_obj.name} VMs"
        )
        res_vm_obj.stop()
