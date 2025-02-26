import logging
import pytest
import random

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework.pytest_customization.marks import workloads, magenta_squad
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs import constants
from ocs_ci.ocs import osd_operations
from ocs_ci.framework import config
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification

logger = logging.getLogger(__name__)


@magenta_squad
@workloads
class TestCnvDeviceReplace(E2ETest):
    """
    Device replacement

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

    def test_vms_with_osd_device_replacement(
        self,
        setup_cnv,
        setup,
        clone_vm_workload,
        snapshot_factory,
        snapshot_restore_factory,
        cnv_workload,
        nodes,
    ):
        """ """
        all_vms = self.vm_objs_def + self.vm_objs_aggr

        file_paths = ["/source_file.txt", "/new_file.txt"]
        source_csums = {}
        for vm_obj in all_vms:
            source_csum = run_dd_io(vm_obj=vm_obj, file_path=file_paths[0], verify=True)
            source_csums[vm_obj.name] = source_csum

        # Choose VMs randomaly
        vm_for_clone, vm_for_stop, vm_for_snap = random.sample(all_vms, 3)

        # Create VM using cloned pvc of source VM PVC
        vm_for_clone.stop()
        clone_obj = clone_vm_workload(
            vm_obj=vm_for_clone,
            volume_interface=vm_for_clone.volume_interface,
            namespace=vm_for_clone.namespace,
        )
        all_vms.append(clone_obj)
        csum = cal_md5sum_vm(vm_obj=clone_obj, file_path=file_paths[0])
        source_csums[clone_obj.name] = csum

        # Create a snapshot
        # Taking Snapshot of PVC
        pvc_obj = vm_for_snap.get_vm_pvc_obj()
        snap_obj = snapshot_factory(pvc_obj)

        # Restore the snapshot
        res_snap_obj = snapshot_restore_factory(
            snapshot_obj=snap_obj,
            storageclass=vm_for_snap.sc_name,
            volume_mode=snap_obj.parent_volume_mode,
            access_mode=vm_for_snap.pvc_access_mode,
            status=constants.STATUS_BOUND,
            timeout=300,
        )

        # Create new VM using the restored PVC
        res_vm_obj = cnv_workload(
            source_url=constants.CNV_FEDORA_SOURCE,
            storageclass=vm_for_snap.sc_name,
            existing_pvc_obj=res_snap_obj,
            namespace=vm_obj.namespace,
        )
        all_vms.append(res_vm_obj)
        csum = cal_md5sum_vm(vm_obj=res_vm_obj, file_path=file_paths[0])
        source_csums[res_vm_obj.name] = csum

        # Keep vms in different states (power on, paused, stoped)
        vm_for_stop.stop()
        vm_for_snap.pause()

        # Replace device
        osd_operations.osd_device_replacement(nodes)

        # Verify osd encryption
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

        # Perform post device replacement data integrity check
        for vm_obj in all_vms:
            new_csum = cal_md5sum_vm(vm_obj=vm_obj, file_path=file_paths[0])
            assert source_csums[vm_obj.name] == new_csum, (
                f"ERROR: Failed data integrity before replacing device and after replacing the device "
                f"for VM '{vm_obj.name}'."
            )
            vm_obj.stop()
