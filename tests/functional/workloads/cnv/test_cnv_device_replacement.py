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
        self.vm_for_stop = None
        self.vm_for_snap = None

        logger.test_step("Create project and deploy CNV workload VMs")
        proj_obj = project_factory()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_def_compr,
            self.sc_obj_aggressive,
        ) = multi_cnv_workload(namespace=proj_obj.namespace)
        all_vms = self.vm_objs_def + self.vm_objs_aggr
        logger.info(f"Created {len(all_vms)} VMs successfully")

        # Register the teardown
        request.addfinalizer(self.teardown)

    def teardown(self):
        """
        Teardown operations for the test case.
        """
        logger.info("Performing teardown operations")

        # Start the stopped VM if it is in stopped state
        if (
            self.vm_for_stop
            and self.vm_for_stop.printableStatus() == constants.CNV_VM_STOPPED
        ):
            self.vm_for_stop.start()
            logger.info(f"VM '{self.vm_for_stop.name}' started")

        # Unpause the paused VM if it is in paused state
        if (
            self.vm_for_snap
            and self.vm_for_snap.printableStatus() == constants.VM_PAUSED
        ):
            self.vm_for_snap.unpause()
            logger.info(f"VM '{self.vm_for_snap.name}' unpaused")

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

        logger.test_step("Write initial data and create clone/snapshot VMs")
        all_vms = self.vm_objs_def + self.vm_objs_aggr
        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csums = {
            vm.name: run_dd_io(vm, file_paths[0], verify=True) for vm in all_vms
        }

        self.vm_for_clone, self.vm_for_stop, self.vm_for_snap = random.sample(
            all_vms, 3
        )
        logger.info(
            f"Selected VMs: clone='{self.vm_for_clone.name}', "
            f"stop='{self.vm_for_stop.name}', snapshot='{self.vm_for_snap.name}'"
        )

        for vm in [self.vm_for_clone, self.vm_for_snap]:
            op = "clone" if vm == self.vm_for_clone else "snapshot"
            logger.info(f"Creating {op} of VM '{vm.name}'")
            vm_obj = (
                vm_clone_fixture(vm, admin_client)
                if vm == self.vm_for_clone
                else vm_snapshot_restore_fixture(vm, admin_client)
            )

            source_csums[vm_obj.name] = cal_md5sum_vm(vm_obj, file_paths[0])
            if vm == self.vm_for_clone:
                all_vms.append(vm_obj)

        logger.test_step("Set VMs to different states and perform device replacement")
        logger.info(
            f"Stopping VM '{self.vm_for_stop.name}' and pausing VM '{self.vm_for_snap.name}'"
        )
        self.vm_for_stop.stop()
        self.vm_for_snap.pause()

        logger.info("Performing OSD device replacement")
        osd_operations.osd_device_replacement(nodes)

        logger.info("Verifying OSD encryption")
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        logger.test_step("Verify VM state preservation after device replacement")
        stop_status = self.vm_for_stop.printableStatus()
        logger.assertion(
            f"Stopped VM state: vm='{self.vm_for_stop.name}', "
            f"expected='{constants.CNV_VM_STOPPED}', actual='{stop_status}', "
            f"match={stop_status == constants.CNV_VM_STOPPED}"
        )
        assert (
            stop_status == constants.CNV_VM_STOPPED
        ), f"VM '{self.vm_for_stop.name}' stopped state not preserved after device replacement"

        pause_status = self.vm_for_snap.printableStatus()
        logger.assertion(
            f"Paused VM state: vm='{self.vm_for_snap.name}', "
            f"expected='{constants.VM_PAUSED}', actual='{pause_status}', "
            f"match={pause_status == constants.VM_PAUSED}"
        )
        assert (
            pause_status == constants.VM_PAUSED
        ), f"VM '{self.vm_for_snap.name}' paused state not preserved after device replacement"

        logger.info(
            f"Starting VM '{self.vm_for_stop.name}' and unpausing VM '{self.vm_for_snap.name}'"
        )
        self.vm_for_stop.start()
        self.vm_for_snap.unpause()

        logger.test_step("Verify data integrity and run I/O on all VMs")
        for vm_obj in all_vms:
            run_dd_io(vm_obj=vm_obj, file_path=file_paths[1], verify=True)
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            logger.assertion(
                f"Data integrity: vm='{vm_obj.name}', "
                f"expected='{source_csums[vm_obj.name]}', actual='{new_csum}', "
                f"match={source_csums[vm_obj.name] == new_csum}"
            )
            assert (
                source_csums[vm_obj.name] == new_csum
            ), f"MD5 mismatch for VM '{vm_obj.name}' after device replacement"
        logger.info("Data integrity verified and I/O completed on all VMs")
