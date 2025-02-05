import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io, expand_pvc_and_verify
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
    @pytest.mark.parametrize(
        argnames=["pvc_expand_before_snapshot", "pvc_expand_after_restore"],
        argvalues=[
            pytest.param(
                False,
                False,
                marks=pytest.mark.polarion_id(
                    "OCS-6299"
                ),  # Polarion ID for no PVC expansion
            ),
            pytest.param(
                True,
                False,
                marks=[
                    pytest.mark.polarion_id(
                        "OCS-6305"
                    ),  # Polarion ID for expansion before snapshot
                    pytest.mark.jira("CNV-55558", run=False),
                ],
            ),
            pytest.param(
                False,
                True,
                marks=[
                    pytest.mark.polarion_id(
                        "OCS-6305"
                    ),  # Polarion ID for expansion after restore
                    pytest.mark.jira("CNV-55558", run=False),
                ],
            ),
        ],
    )
    def test_vm_snapshot_ops(
        self,
        setup_cnv,
        project_factory,
        pvc_expand_before_snapshot,
        pvc_expand_after_restore,
        multi_cnv_workload,
        snapshot_factory,
        snapshot_restore_factory,
        cnv_workload,
    ):
        """
        This test performs the VM PVC snapshot operations

        Test steps:
        1. Create VMs, add data(e.g., files) to all the VMs
        2. Create a snapshot for a VM backed pvc
            a. Expand PVC if `pvc_expand_before_snapshot` is True.
            b. Verify the availability of expanded portion for IOs
        3. Restore the snapshot (to same access mode of the parent PVC and storage_class) by following the
        documented procedure from ODF official docs
        4. Create new vm using restored pvc Verify existing data of the VM are not changed.
             a. Expand PVC if `pvc_expand_after_restore` is True
             b. Verify the availability of expanded portion for IOs
        5. Add further data(e.g., new file) to the VM
        6. Repeat the above procedure for all the VMs in the system
        7. Stop all the VMs created as part of this test.
        """
        proj_obj = project_factory()
        file_paths = ["/file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        failed_vms = []
        for vm_obj in vm_list:
            # Expand PVC if `pvc_expand_before_snapshot` is True
            pvc_obj = vm_obj.get_vm_pvc_obj()
            new_size = 50
            if pvc_expand_before_snapshot:
                if not expand_pvc_and_verify(
                    vm_obj, new_size, failed_vms=failed_vms, vm_objs_def=vm_objs_def
                ):
                    continue
            # Writing IO on source VM
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            # Stopping VM before taking snapshot of the VM PVC
            vm_obj.stop()

            # Taking Snapshot of PVC
            snap_obj = snapshot_factory(pvc_obj)

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
                volume_interface=constants.VM_VOLUME_PVC,
                storageclass=vm_obj.sc_name,
                source_url=constants.CNV_FEDORA_SOURCE,
                existing_pvc_obj=res_snap_obj,
                namespace=vm_obj.namespace,
            )

            # Expand PVC if `pvc_expand_after_restore` is True
            if pvc_expand_after_restore:
                if not expand_pvc_and_verify(
                    vm_obj, new_size, failed_vms=failed_vms, vm_objs_def=vm_objs_def
                ):
                    continue

            # Validate data integrity of file written before taking snapshot
            res_csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
            assert (
                source_csum == res_csum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and restored {res_vm_obj.name} VMs"

            # Write new file to VM
            run_dd_io(vm_obj=res_vm_obj, file_path=file_paths[1], verify=True)
            res_vm_obj.stop()
        if failed_vms:
            assert False, f"Test case failed for VMs: {', '.join(failed_vms)}"
