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
    def test_vm_clone(
        self, project_factory, multi_cnv_workload, clone_vm_workload, setup_cnv
    ):
        """
        This test performs the VM cloning and IOs created using different
        volume interfaces(PVC/DVT)

        Test steps:
        1. Create a clone of a VM PVC by following the documented procedure
        from ODF official docs.
            1.1 Create clone of the pvc associated with VM.
            1.2 Cloned pvc successfully created and listed
        2. Verify the cloned PVC is created.
        3. Create a VM using cloned PVC.
        4. Verify that the data on VM backed by cloned PVC is the
        same as that in the original VM.
        5. Add additional data to the cloned VM.
        6. Delete the clone by following the documented procedure from
        ODF official docs
         6.1 Delete the clone of the PVC associated with VM.
         6.2 Cloned PVC successfully deleted
        7. Repeat the above procedure for all the VMs
        8. Delete all the clones created as part of this test
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
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            log.info(f"Source checksum for {vm_obj.name}: {source_csum}")
            log.info(f"Stopping VM {vm_obj.name}...")
            vm_obj.stop()
            log.info(f"Cloning VM {vm_obj.name}...")
            clone_obj = clone_vm_workload(
                vm_obj=vm_obj,
                volume_interface=vm_obj.volume_interface,
                namespace=vm_obj.namespace,
            )
            log.info(
                f"Clone created successfully for VM {vm_obj.name}: " f"{clone_obj.name}"
            )
            new_csum = cal_md5sum_vm(vm_obj=clone_obj, file_path=file_paths[0])
            assert source_csum == new_csum, (
                f"Failed: MD5 comparison between source {vm_obj.name} "
                f"and cloned {clone_obj.name} VMs"
            )
            run_dd_io(vm_obj=clone_obj, file_path=file_paths[1])

    @workloads
    @pytest.mark.polarion_id("OCS-6299")
    def test_vm_snapshot_ops(
        self, cnv_workload, snapshot_factory, snapshot_restore_factory, setup_cnv
    ):
        """
        This test performs the VM PVC snapshot operations

        Test steps:
        1. Create VMs, add data(e.g., files) to all the VMs
        2. Create a snapshot for a VM backed pvc
        3. Restore the snapshot (to same access mode of the parent PVC and storage_class) by following the
        documented procedure from ODF official docs
        4. Create new vm using restored pvc Verify existing data of the VM are not changed.
        5. Add further data(e.g., new file) to the VM
        6. Repeat the above procedure for all the VMs in the system
        7. Delete all the VMs created as part of this test
        """
        file_paths = ["/file.txt", "/new_file.txt"]
        # TODO: Add multi_cnv fixture to configure VMs based on specifications
        vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
        )[0]
        # Writing IO on source VM
        source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
        # Stopping VM before taking snapshot of the VM PVC
        vm_obj.stop()
        # Taking Snapshot of PVC
        pvc_obj = vm_obj.get_vm_pvc_obj()
        snap_obj = snapshot_factory(pvc_obj)
        # Restore the snapshot
        res_snap_obj = snapshot_restore_factory(
            snapshot_obj=snap_obj,
            storageclass=vm_obj.sc_name,
            size=vm_obj.pvc_size,
            volume_mode=snap_obj.parent_volume_mode,
            access_mode=vm_obj.pvc_access_mode,
            status=constants.STATUS_BOUND,
            timeout=300,
        )
        # Create new VM using the restored PVC
        res_vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
            existing_pvc_obj=res_snap_obj,
            namespace=vm_obj.namespace,
        )[1]
        # Write new file to VM
        run_dd_io(vm_obj=res_vm_obj, file_path=file_paths[1], verify=True)
        # Validate data integrity of file written before taking snapshot
        res_csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
        assert (
            source_csum == res_csum
        ), f"Failed: MD5 comparison between source {vm_obj.name} and cloned {res_vm_obj.name} VMs"
        res_vm_obj.stop()

    @workloads
    @pytest.mark.polarion_id("OCS-6288")
    def test_vm_snap_of_clone(
        self,
        setup_cnv,
        project_factory,
        multi_cnv_workload,
        clone_vm_workload,
        snapshot_factory,
        snapshot_restore_factory,
        cnv_workload,
    ):
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
        6. Create snapshot of cloned pvc
        7. Vertify snapshot of cloned pvc created successfully
        8. Validate the content of snapshot by restoring it to new pvc
        9. Create VM using restored pvc
        9. Check data conisistency on the new VM
        10. Delete the clone and restored pvc by following the documented procedure from ODF official docs
          1. Delete clone and restored pvc of the pvc associated with VM.
          2. cloned pvc and restored pvc successfully deleted
        11. Repeat the above procedure for all the VMs in the system
        12. Delete all the clones and restored pvc created as part of this test
        """

        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )

        vm_list = vm_objs_def + vm_objs_aggr

        for index, vm_obj in enumerate(vm_list):
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            vm_obj.stop()
            clone_obj = clone_vm_workload(
                vm_obj=vm_obj,
                volume_interface=vm_obj.volume_interface,
                namespace=(
                    vm_obj.namespace
                    if vm_obj.volume_interface == constants.VM_VOLUME_PVC
                    else None
                ),
            )[index]
            new_csum = cal_md5sum_vm(vm_obj=clone_obj, file_path=file_paths[0])
            assert (
                source_csum == new_csum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and cloned {clone_obj.name} VMs"
            run_dd_io(vm_obj=clone_obj, file_path=file_paths[1])

            cloned_pvc_obj = clone_obj.get_vm_pvc_obj()
            # Stopping VM before taking snapshot of the VM PVC
            clone_obj.stop()
            # Taking Snapshot of cloned PVC
            snap_obj = snapshot_factory(cloned_pvc_obj)

            # Restore the snapshot
            res_snap_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                storageclass=vm_obj.sc_name,
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=vm_obj.pvc_access_mode,
                status=constants.STATUS_BOUND,
                timeout=300,
            )

            # Create new VM using the restored PVC
            res_vm_obj = cnv_workload(
                source_url=constants.CNV_FEDORA_SOURCE,
                storageclass=vm_obj.sc_name,
                existing_pvc_obj=res_snap_obj,
                namespace=vm_obj.namespace,
            )[-1]

            restore_csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
            assert (
                source_csum == restore_csum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and restored {res_vm_obj.name} VMs"
            run_dd_io(vm_obj=res_vm_obj, file_path=file_paths[1])

            res_vm_obj.stop()
