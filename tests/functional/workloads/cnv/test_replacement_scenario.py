import logging
import pytest
import random

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework.pytest_customization.marks import (
    workloads,
    magenta_squad,
    skipif_external_mode,
)
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs import osd_operations
from ocs_ci.framework import config
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification


logger = logging.getLogger(__name__)


@magenta_squad
@workloads
@skipif_external_mode
class TestCnvDeviceReplace(E2ETest):
    """
    Test case for Device replacement

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, multi_cnv_workload):
        """
        Setting up VMs for tests

        """

        # Create a project
        proj_obj = project_factory()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_def_compr,
            self.sc_obj_aggressive,
        ) = multi_cnv_workload(namespace=proj_obj.namespace)
        logger.info("All vms created successfully")

    def test_vms_with_device_replacement(
        self,
        setup_cnv,
        setup,
        nodes,
        vm_clone_fixture,
        vm_snapshot_restore_fixture,
        admin_client,
    ):
        """
        This test performs the behaviour of VMs and data integrity after Node or Device of cluster

        1. Keep IO operations going on in the VMs. Make sure some snapshot and clones of the VMs present
        2. Keep vms in different states(power on, paused, stoped).
        3. Initiate device Replace  scenarios by following the official procedure for device replacement.
        4. Check VM State Post-Replacement:
            After the device is replaced, verify the state of the VMs that were on the old node:
            Check if running VMs are still running.
            Check if paused VMs remain paused.
            Check if stopped VMs remain stopped.
            Check if all the snapshots and clones preserved their states and data integrity
        5.Verify Cluster Stability:
            Ensure the cluster is stable after the replacement:
            All critical pods are running as expected.
        6. Check for data Integrity
        """

        all_vms = self.vm_objs_def + self.vm_objs_aggr

        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csums = {}
        for vm_obj in all_vms:
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            source_csums[vm_obj.name] = source_csum

        # Choose VMs randomaly
        vm_for_clone, vm_for_stop, vm_for_snap = random.sample(all_vms, 3)

        # Create Clone of VM
        logger.info(f"Cloning VM {vm_for_clone.name}...")
        cloned_vm = vm_clone_fixture(vm_for_clone, admin_client)
        csum = cal_md5sum_vm(vm_obj=cloned_vm, file_path=file_paths[0])
        source_csums[cloned_vm.name] = csum
        all_vms.append(cloned_vm)

        # Create a snapshot
        logger.info(f"Snapshot and restore VM {vm_for_snap.name}...")
        restored_vm = vm_snapshot_restore_fixture(vm_for_snap, admin_client)
        csum = cal_md5sum_vm(vm_obj=restored_vm, file_path=file_paths[0])
        source_csums[vm_for_snap.name] = csum

        # Keep vms in different states (power on, paused, stoped)
        vm_for_stop.stop()
        vm_for_snap.pause()

        logger.info("Start Replacing the device")
        osd_operations.osd_device_replacement(nodes)

        logger.info("Verify osd encryption")
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        # Check VMs status
        assert (
            vm_for_stop.printableStatus() == constants.CNV_VM_STOPPED
        ), "VM did not stop with preserved state after device replacement."
        logger.info("After device replacement, stopped VM preserved state.")

        assert (
            vm_for_snap.printableStatus() == constants.VM_PAUSED
        ), "VM did not pause with preserved state after device replacement."
        logger.info("After device replacement, paused VM preserved state.")

        logger.info("Starting vms")
        vm_for_stop.start()
        vm_for_clone.start()
        vm_for_snap.unpause()

        for vm_obj in all_vms:
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1], verify=True)

        # Perform post device replacement data integrity check
        for vm_obj in all_vms:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert source_csums[vm_obj.name] == new_csum, (
                f"ERROR: Failed data integrity before replacing device and after replacing the device "
                f"for VM '{vm_obj.name}'."
            )
            vm_obj.stop()
