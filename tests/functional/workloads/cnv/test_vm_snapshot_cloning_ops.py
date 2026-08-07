import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads, tier2
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import (
    cal_md5sum_vm,
    run_dd_io,
    expand_pvc_and_verify,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from ocs_ci.framework import config, config_safe_thread_pool_task


logger = logging.getLogger(__name__)


@magenta_squad
class TestVmSnapshotClone(E2ETest):
    """
    Tests related VM snapshots and clones
    """

    @workloads
    @pytest.mark.parametrize(
        argnames=["pvc_expand_before_clone", "pvc_expand_after_clone"],
        argvalues=[
            pytest.param(
                False,
                False,
                marks=pytest.mark.polarion_id(
                    "OCS-6288"
                ),  # Polarion ID for no PVC expansion
            ),
            pytest.param(
                True,
                False,
                marks=[
                    pytest.mark.polarion_id(
                        "OCS-6326"
                    ),  # Polarion ID for expansion before clone
                    pytest.mark.jira(
                        "CNV-55558", run=False
                    ),  # Skip if JIRA issue is open
                ],
            ),
            pytest.param(
                False,
                True,
                marks=[
                    pytest.mark.polarion_id(
                        "OCS-6326"
                    ),  # Polarion ID for expansion after clone
                    pytest.mark.jira(
                        "CNV-55558", run=False
                    ),  # Skip if JIRA issue is open
                ],
            ),
        ],
    )
    def test_vm_clone_with_expansion(
        self,
        setup_cnv,
        project_factory,
        pvc_expand_before_clone,
        pvc_expand_after_clone,
        multi_cnv_workload,
        admin_client,
        vm_clone_fixture,
    ):
        """
        This test performs the VM cloning and IOs created using different
        volume interfaces(PVC/DVT)

        Test steps:
        1. Create a clone of a VM PVC by following the documented procedure
        from ODF official docs.
            1.1 Expand PVC if `pvc_expand_before_clone` is True.
            1.2 Verify the availability of expanded portion for IOs.
        2. Verify the cloned PVC is created.
        3. Create a VM using cloned PVC.
        4. Verify that the data on VM backed by cloned PVC is the
        same as that in the original VM.
            4.1 Expand PVC if `pvc_expand_after_restore` is True
            4.2 Verify the availability of expanded portion for IOs
        5. Add additional data to the cloned VM.
        6. Delete the clone by following the documented procedure from
        ODF official docs
         6.1 Delete the clone of the PVC associated with VM.
         6.2 Cloned PVC successfully deleted
        7. Repeat the above procedure for all the VMs
        8. Delete all the clones created as part of this test
        """

        logger.test_step("Create project and deploy VMs for clone testing")
        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        logger.info(f"Created {len(vm_list)} VMs for clone testing")

        logger.test_step(
            "Clone each VM, verify data integrity, and write additional data"
        )
        failed_vms = []
        for vm_obj in vm_list:
            if pvc_expand_before_clone:
                new_size = 50
                try:
                    expand_pvc_and_verify(vm_obj, new_size)
                except ValueError:
                    logger.exception(f"PVC expansion failed for VM '{vm_obj.name}'")
                    failed_vms.append(vm_obj.name)
                    continue
            logger.info(f"Running I/O on VM '{vm_obj.name}' at '{file_paths[0]}'")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            logger.debug(f"Source checksum for VM '{vm_obj.name}': {source_csum}")

            logger.info(f"Cloning VM '{vm_obj.name}'")
            cloned_vm = vm_clone_fixture(vm_obj, admin_client)

            new_csum = cal_md5sum_vm(vm_obj=cloned_vm, file_path=file_paths[0])
            logger.assertion(
                f"Clone data integrity: source='{vm_obj.name}', clone='{cloned_vm.name}', "
                f"expected='{source_csum}', actual='{new_csum}', "
                f"match={source_csum == new_csum}"
            )
            assert source_csum == new_csum, (
                f"MD5 mismatch between source VM '{vm_obj.name}' "
                f"and cloned VM '{cloned_vm.name}'"
            )

            # Expand PVC if `pvc_expand_after_clone` is True
            if pvc_expand_after_clone:
                new_size = 50
                try:
                    expand_pvc_and_verify(cloned_vm, new_size)
                except ValueError:
                    logger.exception(
                        f"PVC expansion failed for cloned VM '{cloned_vm.name}'"
                    )
                    failed_vms.append(cloned_vm.name)
                    continue
            run_dd_io(vm_obj=cloned_vm, file_path=file_paths[1])
            logger.info(
                f"Data written to '{file_paths[1]}' on cloned VM '{cloned_vm.name}'"
            )
        if failed_vms:
            assert False, f"Test case failed for VMs: {', '.join(failed_vms)}"

    @tier2
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
        vm_snapshot_restore_fixture,
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
        logger.test_step("Create project and deploy VMs for snapshot testing")
        proj_obj = project_factory()
        file_paths = ["/file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        logger.info(f"Created {len(vm_list)} VMs for snapshot testing")

        logger.test_step("Snapshot, restore, and verify data integrity per VM")
        failed_vms = []
        for vm_obj in vm_list:
            new_size = 50
            if pvc_expand_before_snapshot:
                try:
                    expand_pvc_and_verify(vm_obj, new_size)
                except ValueError:
                    logger.exception(
                        f"PVC expansion before snapshot failed for VM '{vm_obj.name}'"
                    )
                    failed_vms.append(
                        f"{vm_obj.name} (Config: {vm_obj.pvc_access_mode}-{vm_obj.volume_interface}, "
                        f"Storage Compression: {'default' if vm_obj in vm_objs_def else 'aggressive'})"
                    )
                    continue

            logger.info(f"Writing I/O on source VM '{vm_obj.name}'")
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)

            logger.info(f"Creating snapshot and restoring VM '{vm_obj.name}'")
            restored_vm = vm_snapshot_restore_fixture(vm_obj, admin_client)

            command = f"test -f {file_paths[1]} && echo 'File exists' || echo 'File not found'"
            output = vm_obj.run_ssh_cmd(
                command=command,
            )

            if "File exists" in output:
                raise FileExistsError(
                    f"File '{file_paths[1]}' still exists on VM '{vm_obj.name}' after snapshot restore"
                )
            logger.info(
                f"Verified file '{file_paths[1]}' absent after snapshot restore on VM '{vm_obj.name}'"
            )

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
                except ValueError:
                    logger.exception(
                        f"PVC expansion after restore failed for VM '{vm_obj.name}'"
                    )
                    failed_vms.append(
                        f"{vm_obj.name} (Config: {vm_obj.pvc_access_mode}-{vm_obj.volume_interface}, "
                        f"Storage Compression: {'default' if vm_obj in vm_objs_def else 'aggressive'})"
                    )
                    continue

            res_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            logger.assertion(
                f"Snapshot restore data integrity: vm='{vm_obj.name}', "
                f"expected='{source_csum}', actual='{res_csum}', "
                f"match={source_csum == res_csum}"
            )
            assert (
                source_csum == res_csum
            ), f"MD5 mismatch between source VM '{vm_obj.name}' and restored VM '{restored_vm.name}'"

            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1], verify=True)

        if failed_vms:
            assert False, f"Test case failed for VMs: {', '.join(failed_vms)}"

    def process_vm_with_snapshot_clone(
        self,
        vm_obj,
        file_paths,
        admin_client,
        vm_clone_fixture,
        vm_snapshot_restore_fixture,
        flag,
    ):
        """
        Process operations on a given VM including cloning, snapshot restore of cloned VM, and data checksum.

        Args:
            vm_obj (object): The virtual machine object to operate on.
            file_paths (list): List of file paths to be handled or verified during the operation.
            admin_client (object): The admin client instance.
            vm_clone_fixture (fixture): Pytest fixture used to clone the VM.
            vm_snapshot_restore_fixture (fixture): Pytest fixture used to create and restore VM snapshots.
            flag(bool): if True - function will create clone VM of restored VM
                        if False - function will create restore VM from cloned VM.

        """
        try:
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            logger.debug(f"Source checksum for VM '{vm_obj.name}': {source_csum}")

            if not flag:
                logger.info(f"Creating clone of VM '{vm_obj.name}'")
                cloned_vm = vm_clone_fixture(vm_obj, admin_client)
                run_dd_io(vm_obj=cloned_vm, file_path=file_paths[1])

                logger.info(f"Creating snapshot of cloned VM '{cloned_vm.name}'")
                restored_vm = vm_snapshot_restore_fixture(cloned_vm, admin_client)
                restore_csum = cal_md5sum_vm(
                    vm_obj=restored_vm, file_path=file_paths[0]
                )
                logger.assertion(
                    f"Snapshot of clone data integrity: source='{vm_obj.name}', "
                    f"restored='{restored_vm.name}', expected='{source_csum}', "
                    f"actual='{restore_csum}', match={source_csum == restore_csum}"
                )
                assert source_csum == restore_csum, (
                    f"MD5 mismatch between source VM '{vm_obj.name}' "
                    f"and restored VM '{restored_vm.name}'"
                )
                run_dd_io(vm_obj=restored_vm, file_path=file_paths[1])
                logger.info(f"VM '{vm_obj.name}' clone-snapshot processing completed")
            else:
                logger.info(f"Creating snapshot and restoring VM '{vm_obj.name}'")
                restored_vm = vm_snapshot_restore_fixture(vm_obj, admin_client)
                restore_csum = cal_md5sum_vm(
                    vm_obj=restored_vm, file_path=file_paths[0]
                )
                logger.assertion(
                    f"Restore data integrity: source='{vm_obj.name}', "
                    f"restored='{restored_vm.name}', expected='{source_csum}', "
                    f"actual='{restore_csum}', match={source_csum == restore_csum}"
                )
                assert source_csum == restore_csum, (
                    f"MD5 mismatch between source VM '{vm_obj.name}' "
                    f"and restored VM '{restored_vm.name}'"
                )

                logger.info(f"Creating clone of restored VM '{restored_vm.name}'")
                cloned_vm = vm_clone_fixture(restored_vm, admin_client)
                run_dd_io(vm_obj=restored_vm, file_path=file_paths[1])
                clone_csum = cal_md5sum_vm(vm_obj=cloned_vm, file_path=file_paths[0])
                logger.assertion(
                    f"Clone of restore data integrity: source='{vm_obj.name}', "
                    f"clone='{cloned_vm.name}', expected='{source_csum}', "
                    f"actual='{clone_csum}', match={source_csum == clone_csum}"
                )
                assert source_csum == clone_csum, (
                    f"MD5 mismatch between source VM '{vm_obj.name}' "
                    f"and clone VM '{cloned_vm.name}'"
                )

        except Exception:
            logger.exception(f"VM processing failed for '{vm_obj.name}'")
            raise

    def run_parallel_vm_clone_restore(
        self,
        vm_list,
        file_paths,
        admin_client,
        vm_clone_fixture,
        vm_snapshot_restore_fixture,
        flag=None,
    ):
        """
        Process operations on VMs in parallel including cloning, snapshot restore of cloned VM, and data checksum.

        Args:
            vm_obj (object): The virtual machine object to operate on.
            file_paths (list): List of file paths to be handled or verified during the operation.
            admin_client (object): The admin client instance.
            vm_clone_fixture (fixture): Pytest fixture used to clone the VM.
            vm_snapshot_restore_fixture (fixture): Pytest fixture used to create and restore VM snapshots.

        """

        MAX_WORKERS = 10
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    config_safe_thread_pool_task,
                    config.default_cluster_index,
                    self.process_vm_with_snapshot_clone,
                    vm_obj,
                    file_paths,
                    admin_client,
                    vm_clone_fixture,
                    vm_snapshot_restore_fixture,
                    flag,
                ): vm_obj.name
                for vm_obj in vm_list
            }
            for future in as_completed(futures):
                vm_name = futures[future]
                try:
                    future.result()
                except Exception:
                    logger.exception(f"Parallel VM processing failed for '{vm_name}'")
                    raise

    @workloads
    @pytest.mark.polarion_id("OCS-6325")
    def test_vm_snap_of_clone(
        self,
        setup_cnv,
        project_factory,
        multi_cnv_workload,
        vm_clone_fixture,
        vm_snapshot_restore_fixture,
        admin_client,
    ):
        """
        This test performs the VM cloning and IOs created using different volume interfaces(PVC/DV/DVT)

        Test steps:
        1. Create a clone of a VM by following the documented procedure from CNV official docs.
        2. Add additional data to the cloned VM.
        3. Create snapshot of cloned VM
        4. Vertify snapshot of cloned VM created successfully.
        5. Check data conisistency on the Restored VM
        6. Repeat the above procedure for all the VMs in the system
        7. Delete all the clones and restored VM created as part of this test
        """

        logger.test_step("Create project and deploy VMs")
        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        logger.info(f"Created {len(vm_list)} VMs for snapshot-of-clone testing")

        logger.test_step(
            "Clone VMs, snapshot clones, and verify data integrity in parallel"
        )
        self.run_parallel_vm_clone_restore(
            vm_list,
            file_paths,
            admin_client,
            vm_clone_fixture,
            vm_snapshot_restore_fixture,
        )

    @pytest.mark.polarion_id("OCS-6321")
    def test_clone_of_restored_vm(
        self,
        setup_cnv,
        project_factory,
        multi_cnv_workload,
        vm_clone_fixture,
        vm_snapshot_restore_fixture,
        admin_client,
    ):
        """
        This test performs the VM cloning and IOs created using different volume interfaces(PVC/DV/DVT)

        Test steps:
        1. Deploy multiple VMs with associated PVCs.
        2. Write and verify initial data on each VM.
        3. Create and wait for VM snapshots to complete.
        4. Add additional data to the VM after snapshot.
        5. Stop and restore each VM from its snapshot.
        6. Clone the restored VM and verify the data integrity.
        7. Write additional data to the cloned VM.
        """

        logger.test_step("Create project and deploy VMs")
        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        logger.info(f"Created {len(vm_list)} VMs for clone-of-restored testing")

        logger.test_step(
            "Restore VMs from snapshots, clone restored VMs, and verify data integrity in parallel"
        )
        self.run_parallel_vm_clone_restore(
            vm_list,
            file_paths,
            admin_client,
            vm_clone_fixture,
            vm_snapshot_restore_fixture,
            flag=1,
        )
