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
from ocs_ci.framework.testlib import ignore_leftovers


logger = logging.getLogger(__name__)


@magenta_squad
@workloads
@skipif_external_mode
@ignore_leftovers
class TestCnvDeviceReplace(E2ETest):
    """
    Test case for Device replacement

    """

    @pytest.fixture(autouse=True)
    def setup(self, request, project_factory, multi_cnv_workload):
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

        # Register the teardown
        request.addfinalizer(self.teardown)

    def teardown(self):
        """
        Teardown operations for the test case.
        """
        logger.info("Performing teardown operations...")

        # Start the stopped VM if it is in stopped state
        if self.vm_for_stop.printableStatus() == constants.CNV_VM_STOPPED:
            self.vm_for_stop.start()
            logger.info(f"VM {self.vm_for_stop.name} started.")

        # Unpause the paused VM if it is in paused state
        if self.vm_for_snap.printableStatus() == constants.VM_PAUSED:
            self.vm_for_snap.unpause()
            logger.info(f"VM {self.vm_for_snap.name} unpaused.")

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
        Tests VM behavior and data integrity after device replacement in a cluster.

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
        # Initialize checksums
        source_csums = {
            vm.name: run_dd_io(vm, file_paths[0], verify=True) for vm in all_vms
        }

        # Randomly select VMs for operations
        self.vm_for_clone, self.vm_for_stop, self.vm_for_snap = random.sample(
            all_vms, 3
        )

        # Create clone and snapshot, update checksums
        for vm in [self.vm_for_clone, self.vm_for_snap]:
            vm_obj = (
                vm_clone_fixture(vm, admin_client)
                if vm == self.vm_for_clone
                else vm_snapshot_restore_fixture(vm, admin_client)
            )

            # Use cal_md5sum_vm here
            source_csums[vm_obj.name] = cal_md5sum_vm(vm_obj, file_paths[0])
            if vm == self.vm_for_clone:
                all_vms.append(vm_obj)

        # Keep vms in different states (power on, paused, stoped)
        self.vm_for_stop.stop()
        self.vm_for_snap.pause()

        # Perform device replacement
        osd_operations.osd_device_replacement(nodes)

        logger.info("Verify osd encryption")
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        # Check VMs status post-replacement
        assert (
            self.vm_for_stop.printableStatus() == constants.CNV_VM_STOPPED
        ), "Stopped VM state not preserved."
        logger.info("After device replacement, stopped VM preserved state.")

        assert (
            self.vm_for_snap.printableStatus() == constants.VM_PAUSED
        ), "Paused VM state not preserved."
        logger.info("After device replacement, paused VM preserved state.")

        logger.info("Starting vms")
        self.vm_for_stop.start()
        self.vm_for_snap.unpause()

        # Combined IO operations and data integrity check
        for vm_obj in all_vms:
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1], verify=True)
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert (
                source_csums[vm_obj.name] == new_csum
            ), f"Data integrity failed for VM '{vm_obj.name}'."
