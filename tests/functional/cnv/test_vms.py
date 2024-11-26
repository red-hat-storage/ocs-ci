import logging
import pytest
import subprocess
import os
import hashlib
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.ocs.cluster import (
    validate_compression,
)
from ocs_ci.ocs.exceptions import (
    PoolNotCompressedAsExpected,
)

logger = logging.getLogger(__name__)


class TestCNVVM(E2ETest):
    """
    Includes tests related to CNV workloads on MDR environment.

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, multi_cnv_workload):

        # Create a project
        proj_obj = project_factory()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            self.sc_obj_aggressive,
            self.sc_obj_def_compr,
        ) = multi_cnv_workload(namespace=proj_obj.namespace)

        logger.info("All vms created successfully")

    def create_file_with_dd(self, file_name, size_in_mb):
        """
        Creates files on local machine of specified size
        Args:
            file_name : File name to be create
            size_in_mb: Size of file

        Returns:

            file_path(str) : Path of the created file
        """
        try:
            # Define the file path
            file_path = str(os.path.abspath(file_name))

            # Command to create a file with the specified size using dd
            subprocess.run(
                [
                    "dd",
                    "if=/dev/zero",
                    f"of={file_path}",
                    "bs=1M",
                    f"count={size_in_mb}",
                ],
                check=True,
                text=True,
            )
            logger.info(
                f"File '{file_name}' of size {size_in_mb}MB created successfully."
            )
            return file_path

        except subprocess.CalledProcessError as e:
            logger.info(f"Failed to create the file: {e}")
            return None

    def get_md5sum(self, file_path):
        """
        Calculates the md5sum of the file
        Args:
            file_path (str): The name of the file for which md5sum to be calculated

        Returns:
            str: The md5sum of the file
        """
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        except subprocess.CalledProcessError as e:
            print(f"Error occurred while calculating MD5 checksum: {e}")
            return None

    @magenta_squad
    def test_cnv_vms(self, setup):
        """
        Tests to verify configuration for non-GS like environment

        """

        # 1. if os os windows then check rxbounce enabled in sc yaml
        # 2. Validate data integrity using md5sum.

        local_file_name = "dd_file1"
        vm_filepath = "/home/admin/dd_file1_copy"

        # Create file locally
        file_path = self.create_file_with_dd(local_file_name, 2048)

        # Calculate the MD5 checksum
        if file_path:
            md5sum_on_local = self.get_md5sum(file_path)
            if md5sum_on_local:
                print(f"MD5 checksum of the file: {md5sum_on_local}")

        # Copy local file to all vms
        for vm_obj in self.vm_objs_aggr:
            vm_obj.scp_to_vm(
                local_path=file_path,
                vm_username=None,
                identity_file=None,
                vm_dest_path=vm_filepath,
                recursive=False,
            )
        for vm_obj in self.vm_objs_def:
            vm_obj.scp_to_vm(
                local_path=file_path,
                vm_username=None,
                identity_file=None,
                vm_dest_path=vm_filepath,
                recursive=False,
            )

        # Take md5sum of copied file and compare with md5sum taken locally
        for vm_obj in self.vm_objs_aggr:
            md5sum_on_vm = cal_md5sum_vm(vm_obj, vm_filepath, username=None)
            assert (
                md5sum_on_vm == md5sum_on_local
            ), f"md5sum has not changed after copying file on {vm_obj.name}"

        for vm_obj in self.vm_objs_def:
            md5sum_on_vm = cal_md5sum_vm(vm_obj, vm_filepath, username=None)
            assert (
                md5sum_on_vm == md5sum_on_local
            ), f"md5sum has not changed after copying file on {vm_obj.name}"

        # 4.Write random data on vm and validate compression
        # Validate that data is compressed
        for vm_obj in self.vm_objs_aggr:
            logger.info(f"All Attributes {dir(vm_obj)}")
            compression_result = validate_compression(
                self.sc_obj_aggressive.interface_name
            )
            if compression_result is False:
                raise PoolNotCompressedAsExpected(
                    f"Pool {self.sc_obj_aggressive.interface_name} compression did not reach expected value"
                )

        # 5.Verify PV Keyrotation.
        for vm in self.vm_objs_def:
            pvk_obj = PVKeyrotation(self.sc_obj_def_compr)
            assert pvk_obj.wait_till_keyrotation(
                vm.pvc_obj.get_pv_volume_handle_name
            ), f"Failed to rotate Key for the PVC {vm.pvc_obj.name}"

        for vm in self.vm_objs_aggr:
            pvk_obj = PVKeyrotation(self.sc_obj_aggressive)
            volume_name = vm.pvc_obj.get().get("spec").get("volumeName")
            for line in run_oc_command(
                f"describe pv {volume_name}", namespace=vm.namespace
            ):
                if "VolumeHandle:" in line:
                    volume_handle = line.split()[1]
                    break
            if volume_handle is None:
                logger.error(f"Cannot get volume handle for pv {volume_name}")
                raise Exception("Cannot get volume handle")
            assert pvk_obj.wait_till_keyrotation(
                volume_handle
            ), f"Failed to rotate Key for the PVC {vm.pvc_obj.name}"
