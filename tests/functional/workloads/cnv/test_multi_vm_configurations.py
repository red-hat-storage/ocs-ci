import logging
import pytest

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework.pytest_customization.marks import workloads, magenta_squad
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@magenta_squad
class TestCNVVM(E2ETest):
    """
    Includes tests related to CNV+ODF workloads.

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, multi_cnv_workload):
        """
        Setting up VMs for tests

        """

        logger.test_step("Create project and deploy encrypted CNV workload VMs")
        proj_obj = project_factory()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_def_compr,
            self.sc_obj_aggressive,
        ) = multi_cnv_workload(namespace=proj_obj.namespace, encrypted=True)
        logger.info(
            f"Created {len(self.vm_objs_def + self.vm_objs_aggr)} VMs with encryption enabled"
        )

    def verify_keyrotation(self, vm_objs, sc_obj):
        """
        Verify the keyrotation is succeed.

        Args:
            vm_objs (obj): virtual machine Object
            sc_obj (obj): storage class object

        """
        for vm in vm_objs:
            if vm.volume_interface == constants.VM_VOLUME_PVC:
                logger.info(f"Verifying key rotation for VM '{vm.name}'")
                pvk_obj = PVKeyrotation(sc_obj)
                volume_name = vm.pvc_obj.get().get("spec", {}).get("volumeName")
                volume_handle = None
                for line in run_oc_command(
                    f"describe pv {volume_name}", namespace=vm.namespace
                ):
                    if "VolumeHandle:" in line:
                        volume_handle = line.split()[1]
                        break
                if not volume_handle:
                    logger.error(
                        f"Cannot get volume handle for PV '{volume_name}' "
                        f"(VM: '{vm.name}')"
                    )
                    raise Exception("Cannot get volume handle")
                logger.assertion(
                    f"Key rotation: vm='{vm.name}', pvc='{vm.pvc_obj.name}', "
                    f"volume_handle='{volume_handle}'"
                )
                assert pvk_obj.wait_till_keyrotation(
                    volume_handle
                ), f"Failed to rotate key for PVC '{vm.pvc_obj.name}'"
                logger.info(f"Key rotation succeeded for VM '{vm.name}'")

    @workloads
    @pytest.mark.polarion_id("OCS-6298")
    def test_cnv_vms(self, setup_cnv, setup):
        """
        Tests to verify configuration for non-GS like environment

        Steps:
        1) Create VMs using fixture multi_cnv_workload
        2) Validate data integrity using md5sum.
            a. create file locally and take md5sum
            b. copy same file to vm and take md5sum
            c. Validate both are same or not
        3) Validate pvc level key rotation
        4) Stop the VM
        5) Delete the VM (as part of factory teardown)

        """
        all_vm_list = self.vm_objs_def + self.vm_objs_aggr

        logger.test_step("Validate data integrity using md5sum")
        file_name = "/tmp/dd_file"
        vm_filepath = "/home/admin/dd_file1_copy"

        logger.info("Creating 1GB test file locally")
        cmd = f"dd if=/dev/zero of={file_name} bs=1M count=1024"
        exec_cmd(cmd)

        cmd = f"md5sum {file_name}"
        result = exec_cmd(cmd)
        md5sum_on_local = result.stdout.decode().strip().split()[0]
        if not md5sum_on_local:
            raise ValueError(
                "MD5 checksum could not be calculated. Ensure the file exists and is accessible."
            )
        logger.info(f"Local file MD5 checksum: {md5sum_on_local}")

        logger.info(f"Copying test file to {len(all_vm_list)} VMs via SCP")
        for vm_obj in all_vm_list:
            vm_obj.scp_to_vm(
                local_path=file_name,
                vm_dest_path=vm_filepath,
            )

        for vm_obj in all_vm_list:
            md5sum_on_vm = cal_md5sum_vm(vm_obj, vm_filepath, username=None)
            logger.assertion(
                f"MD5 checksum: vm='{vm_obj.name}', "
                f"expected='{md5sum_on_local}', actual='{md5sum_on_vm}', "
                f"match={md5sum_on_vm == md5sum_on_local}"
            )
            assert (
                md5sum_on_vm == md5sum_on_local
            ), f"MD5 checksum mismatch after copying file to VM '{vm_obj.name}'"
        logger.info("Data integrity verified for all VMs")

        logger.test_step(
            "Verify PV key rotation for VMs with default and aggressive compression"
        )
        self.verify_keyrotation(self.vm_objs_def, self.sc_obj_def_compr)
        self.verify_keyrotation(self.vm_objs_aggr, self.sc_obj_aggressive)
        logger.info("PV key rotation verified for all VMs")
