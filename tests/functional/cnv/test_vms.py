import logging
import pytest

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


class TestCNVVM(E2ETest):
    """
    Includes tests related to CNV+ODF workloads.

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
        ) = multi_cnv_workload(namespace=proj_obj.namespace)

        logger.info("All vms created successfully")

    @magenta_squad
    @workloads
    def test_cnv_vms(self, setup):
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

        # To Do
        # 1. if os is windows then check rxbounce enabled in sc yaml

        all_vm_list = self.vm_objs_def + self.vm_objs_aggr

        # 2. Validate data integrity using md5sum.
        file_name = "/tmp/dd_file"
        vm_filepath = "/home/admin/dd_file1_copy"

        # Create file locally
        cmd = f"dd if=/dev/zero of={file_name} bs=1M count=1024"
        run_cmd(cmd)

        # Calculate the MD5 checksum
        if file_name:
            cmd = f"md5sum {file_name}"
            md5sum_on_local = run_cmd(cmd)
            if md5sum_on_local:
                logger.info(f"MD5 checksum of the file: {md5sum_on_local}")
            else:
                raise ValueError(
                    "MD5 checksum could not be calculated. Ensure the file exists and is accessible."
                )
        else:
            raise ValueError(
                "File name is not provided. Please specify a valid file name."
            )

        # Copy local file to all vms
        for vm_obj in all_vm_list:
            vm_obj.scp_to_vm(
                local_path=file_name,
                vm_dest_path=vm_filepath,
            )

        # Take md5sum of copied file and compare with md5sum taken locally
        for vm_obj in all_vm_list:
            md5sum_on_vm = cal_md5sum_vm(vm_obj, vm_filepath, username=None)
            assert (
                md5sum_on_vm == md5sum_on_local
            ), f"md5sum has changed after copying file on {vm_obj.name}"

        # 3.Verify PV Keyrotation.
        volume_interface = [
            constants.VM_VOLUME_PVC,
            constants.VM_VOLUME_DVT,
        ]

        for vl_if in volume_interface:
            if vl_if == constants.VM_VOLUME_PVC:
                for vm_group in [self.vm_objs_def, self.vm_objs_aggr]:
                    for vm in vm_group:
                        pvk_obj = PVKeyrotation(vm.pvc_obj.storageclass)
                        if vm_group is self.vm_objs_def:
                            volume_handle = vm.pvc_obj.get_pv_volume_handle_name
                        else:
                            volume_name = (
                                vm.pvc_obj.get().get("spec", {}).get("volumeName")
                            )
                            volume_handle = next(
                                (
                                    line.split()[1]
                                    for line in run_oc_command(
                                        f"describe pv {volume_name}",
                                        namespace=vm.namespace,
                                    )
                                    if "VolumeHandle:" in line
                                ),
                                None,
                            )
                            if volume_handle is None:
                                logger.error(
                                    f"Cannot get volume handle for PV {volume_name}"
                                )
                                raise Exception("Cannot get volume handle")

                        assert pvk_obj.wait_till_keyrotation(
                            volume_handle
                        ), f"Failed to rotate Key for the PVC {vm.pvc_obj.name}"

        # 4.Stop all VMs
        for vm_obj in all_vm_list:
            vm_obj.stop()
