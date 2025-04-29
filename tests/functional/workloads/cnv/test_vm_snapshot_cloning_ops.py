import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io, expand_pvc_and_verify
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocp_resources.virtual_machine_restore import VirtualMachineRestore
from ocp_resources.virtual_machine_snapshot import VirtualMachineSnapshot

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
        admin_client,
    ):
        """
        This test performs the VM PVC snapshot operations

        Test steps:
        1. Create VMs, add data(e.g., files) to all the VMs
        2. Create a snapshot of VM
            a. Expand PVC if `pvc_expand_before_snapshot` is True.
            b. Verify the availability of expanded portion for IOs
        3. Restore the snapshot into same VM and Verify data avaialble before snapshot of the VM are not changed
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
            new_size = 50
            if pvc_expand_before_snapshot:
                try:
                    expand_pvc_and_verify(vm_obj, new_size)
                except ValueError as e:
                    log.error(
                        f"Error for VM {vm_obj.name}: {e}. Continuing with the next VM."
                    )
                    failed_vms.append(
                        f"{vm_obj.name} (Config: {vm_obj.pvc_access_mode}-{vm_obj.volume_interface}, "
                        f"Storage Compression: {'default' if vm_obj in vm_objs_def else 'aggressive'})"
                    )
                    continue

            # Writing IO on source VM
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            snapshot_name = f"snapshot-{vm_obj.name}"
            # Explicitly create the VirtualMachineSnapshot instance
            with VirtualMachineSnapshot(
                name=snapshot_name,
                namespace=vm_obj.namespace,
                vm_name=vm_obj.name,
                client=admin_client,
                teardown=False,
            ) as vm_snapshot:
                vm_snapshot.wait_snapshot_done()

            # Write file after snapshot
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])

            # Stopping VM before restoring
            vm_obj.stop()

            # Explicitly create the VirtualMachineRestore instance
            restore_snapshot_name = create_unique_resource_name(
                vm_snapshot.name, "restore"
            )
            try:
                with VirtualMachineRestore(
                    name=restore_snapshot_name,
                    namespace=vm_obj.namespace,
                    vm_name=vm_obj.name,
                    snapshot_name=vm_snapshot.name,
                    client=admin_client,
                    teardown=False,
                ) as vm_restore:
                    vm_restore.wait_restore_done()  # Wait for restore completion
                    vm_obj.start()
                    vm_obj.wait_for_ssh_connectivity(timeout=1200)
            finally:
                vm_snapshot.delete()

            # Verify file written after snapshot is not present.
            command = f"test -f {file_paths[1]} && echo 'File exists' || echo 'File not found'"
            output = vm_obj.run_ssh_cmd(
                command=command,
            )

            # Check if file is not present
            if "File exists" in output:
                raise FileExistsError(
                    (
                        f"ERROR: File '{file_paths[1]}' still exists after snapshot restore!"
                    )
                )
            log.info(f"File '{file_paths[1]}' is NOT present (expected).")

            # Expand PVC if `pvc_expand_after_restore` is True
            if pvc_expand_after_restore:
                try:
                    if vm_obj.volume_interface == "DVT":
                        vm_obj.pvc_name = (
                            vm_obj.get()
                            .get("spec")
                            .get("template")
                            .get("spec")
                            .get("volumes")[0]
                            .get("dataVolume")
                            .get("name")
                        )
                    else:
                        vm_obj.pvc_name = (
                            vm_obj.get()
                            .get("spec")
                            .get("template")
                            .get("spec")
                            .get("volumes")[0]
                            .get("persistentVolumeClaim")
                            .get("claimName")
                        )
                    expand_pvc_and_verify(vm_obj, new_size)
                except ValueError as e:
                    log.error(
                        f"Error for VM {vm_obj.name}: {e}. Continuing with the next VM."
                    )
                    failed_vms.append(
                        f"{vm_obj.name} (Config: {vm_obj.pvc_access_mode}-{vm_obj.volume_interface}, "
                        f"Storage Compression: {'default' if vm_obj in vm_objs_def else 'aggressive'})"
                    )
                    continue

            # Validate data integrity of file written before taking snapshot
            res_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csum == res_csum
            ), f"Failed: MD5 comparison between source {vm_obj.name} and restored {vm_restore.name} VMs"

            # Write new file to VM
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1], verify=True)

        if failed_vms:
            assert False, f"Test case failed for VMs: {', '.join(failed_vms)}"

# TODO: Add multi_cnv fixture to configure VMs based on specifications
        vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            source_url=constants.CNV_FEDORA_SOURCE,
        )
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
        )
        # Write new file to VM
        run_dd_io(vm_obj=res_vm_obj, file_path=file_paths[1], verify=True)
        # Validate data integrity of file written before taking snapshot
        res_csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
        assert (
            source_csum == res_csum
        ), f"Failed: MD5 comparison between source {vm_obj.name} and cloned {res_vm_obj.name} VMs"
        res_vm_obj.stop()

    @workloads
    @pytest.mark.polarion_id("OCS-6321")
    def test_vm_snapshot_pvc_clone(
        self,
        setup_cnv,
        project_factory,
        multi_cnv_workload,
        snapshot_factory,
        snapshot_restore_factory,
        cnv_workload,
        clone_vm_workload,
    ):
        """
        This test checks the clone of restored snapshot PVC created successfully
        without data loss or corruption.

        Test steps:
        1. Create a VM with PVC/DVT
        2. Add data to the VM and shut it down
        3. Take a snapshot of the VM’s PVC
        4. Deploy a new VM using the restored snapshot PVC
        5. Clone the VM workload to create a new PVC from the restored snapshot PVC
        6. Check data integrity in the cloned VM
        7. Verify that the data persisted after cloning
        8. Adding data on cloned VM
        """
        proj_obj = project_factory()
        file_paths = ["/file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        log.info(f"Total VMs to process: {len(vm_list)}")
        for index, vm_obj in enumerate(vm_list):
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            vm_obj.stop()

            # Take snapshot of the VM's PVC
            pvc_obj = vm_obj.get_vm_pvc_obj()
            snap_obj = snapshot_factory(pvc_obj)

            # Restore the snapshot to a new PVC
            res_snap_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                storageclass=vm_obj.sc_name,
                size=vm_obj.pvc_size,
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=vm_obj.pvc_access_mode,
                timeout=300,
            )

            # Create a new VM from the restored PVC
            res_vm_obj = cnv_workload(
                source_url=constants.CNV_FEDORA_SOURCE,
                existing_pvc_obj=res_snap_obj,
                namespace=vm_obj.namespace,
                storageclass=vm_obj.sc_name,
            )

            res_vm_obj.stop()

            # Clone the restored VM
            res_vm_obj_clone = clone_vm_workload(res_vm_obj, namespace=vm_obj.namespace)

            # Validate data integrity in the cloned VM
            res_csum = cal_md5sum_vm(vm_obj=res_vm_obj_clone, file_path=file_paths[0])
            assert source_csum == res_csum, (
                f"Failed: MD5 comparison between source {vm_obj.name} and cloned "
                f"{res_vm_obj.name} VMs"
            )

            # Writing new data on cloned VM
            run_dd_io(vm_obj=res_vm_obj_clone, file_path=file_paths[1])
