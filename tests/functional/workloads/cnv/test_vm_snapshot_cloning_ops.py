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
        clone_vm_workload,
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

        proj_obj = project_factory()
        file_paths = ["/source_file.txt", "/new_file.txt"]
        vm_objs_def, vm_objs_aggr, _, _ = multi_cnv_workload(
            namespace=proj_obj.namespace
        )
        vm_list = vm_objs_def + vm_objs_aggr
        log.info(f"Total VMs to process: {len(vm_list)}")
        failed_vms = []
        for vm_obj in vm_list:
            # Expand PVC if `pvc_expand_before_snapshot` is True
            pvc_obj = vm_obj.get_vm_pvc_obj()
            if pvc_expand_before_clone:
                new_size = 50
                try:
                    pvc_obj.resize_pvc(new_size=new_size, verify=True)
                    pvc_obj = vm_obj.get_vm_pvc_obj()

                    # Get rootdisk name
                    disk = (
                        vm_obj.vmi_obj.get()
                        .get("status")
                        .get("volumeStatus")[1]["target"]
                    )
                    devicename = f"/dev/{disk}"

                    result = vm_obj.run_ssh_cmd(
                        command=f"lsblk -d -n -o SIZE {devicename}"
                    ).strip()
                    if result == f"{new_size}G":
                        log.info("expanded PVC size is showing on vm")
                    else:
                        raise ValueError(
                            "Expanded PVC size before clone is not showing on VM. "
                            "Please verify the disk rescan and filesystem resize."
                        )
                except ValueError as e:
                    log.error(
                        f"Error for VM {vm_obj}: {e}. Continuing with the next VM."
                    )
                    failed_vms.append(vm_obj.name)
                    continue
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
            # Expand PVC if `pvc_expand_after_restore` is True
            if pvc_expand_after_clone:
                new_size = 50
                try:
                    clone_pvc_obj = clone_obj.get_vm_pvc_obj()
                    clone_pvc_obj.resize_pvc(new_size=new_size, verify=True)
                    assert (
                        clone_pvc_obj.get_vm_pvc_obj().size == new_size
                    ), f"Failed: VM PVC Expansion on cloned VM {clone_obj.name} "

                    # Get rootdisk name
                    disk = (
                        vm_obj.vmi_obj.get()
                        .get("status")
                        .get("volumeStatus")[1]["target"]
                    )
                    devicename = f"/dev/{disk}"

                    result = vm_obj.run_ssh_cmd(
                        command=f"lsblk -d -n -o SIZE {devicename}"
                    ).strip()
                    if result == f"{new_size}G":
                        log.info("expanded PVC size is showing on vm")
                    else:
                        raise ValueError(
                            "Expanded PVC size after clone is not showing on VM. "
                            "Please verify the disk rescan and filesystem resize."
                        )
                except ValueError as e:
                    log.error(
                        f"Error for VM {vm_obj}: {e}. Continuing with the next VM."
                    )
                    failed_vms.append(vm_obj.name)
                    continue
            run_dd_io(vm_obj=clone_obj, file_path=file_paths[1])

        if failed_vms:
            assert False, f"Test case failed for VMs: {', '.join(failed_vms)}"

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
