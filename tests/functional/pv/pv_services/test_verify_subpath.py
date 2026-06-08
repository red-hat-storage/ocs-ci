import logging
import os
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    polarion_id,
    ManageTest,
    tier2,
)
from ocs_ci.ocs.resources.pod import check_file_existence

logger = logging.getLogger(__name__)


@green_squad
class TestVerifySubpath(ManageTest):
    """
    Tests to verify subpath

    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory, pod_factory):
        """
        Create PVC and pod

        Args:
            pvc_factory: A fixture to create new PVC
            pod_factory: A fixture to create new pod

        """
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=3,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )
        self.pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
        )

    @tier2
    @polarion_id("OCS-2654")
    def test_verify_subpath(self, multi_pvc_factory, pod_factory):
        """
        Test case to verify subpath

        """
        subdir = "dir1/subdir1"
        filename1 = "file1frompod1"
        filename2 = "file2frompod1"
        filename3 = "file1frompod2"

        logger.test_step("Create a subdirectory and file from the first pod")
        # Create a sub directory
        self.pod_obj.exec_cmd_on_pod(
            command=f"mkdir -p {os.path.join(self.pod_obj.get_storage_path(), subdir)}"
        )

        # Create one file in the sub directory
        self.pod_obj.exec_cmd_on_pod(
            command=f"touch {os.path.join(self.pod_obj.get_storage_path(), os.path.join(subdir, filename1))}"
        )

        logger.test_step("Create second pod with subpath mounting the subdirectory")
        # Create another pod which can use the sub directory
        pod_obj2 = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            subpath=subdir,
        )

        logger.test_step("Verify file created from first pod is visible on second pod")
        # On the second pod, verify the presence the file created from the first pod
        file1_exists = check_file_existence(
            pod_obj2, os.path.join(pod_obj2.get_storage_path(), filename1)
        )
        logger.assertion(
            f"File {filename1} exists on pod {pod_obj2.name}: expected=True, actual={file1_exists}"
        )
        assert file1_exists, f"File {filename1} not found on pod {pod_obj2.name}"

        logger.test_step("Create another file from first pod and verify on second pod")
        # Create another file from the first pod
        self.pod_obj.exec_cmd_on_pod(
            command=f"touch {os.path.join(self.pod_obj.get_storage_path(), os.path.join(subdir, filename2))}"
        )

        # On the second pod, verify the presence the new file created from the first pod
        file2_exists = check_file_existence(
            pod_obj2, os.path.join(pod_obj2.get_storage_path(), filename2)
        )
        logger.assertion(
            f"File {filename2} exists on pod {pod_obj2.name}: expected=True, actual={file2_exists}"
        )
        assert file2_exists, f"File {filename2} not found on pod {pod_obj2.name}"

        logger.test_step("Create file from second pod and verify on first pod")
        # Create a file from the second pod
        pod_obj2.exec_cmd_on_pod(
            command=f"touch {os.path.join(self.pod_obj.get_storage_path(), filename3)}"
        )

        # On the first pod, verify the presence the file created from the second pod
        file3_exists = check_file_existence(
            self.pod_obj,
            os.path.join(
                self.pod_obj.get_storage_path(), os.path.join(subdir, filename3)
            ),
        )
        logger.assertion(
            f"File {filename3} exists on pod {self.pod_obj.name}: expected=True, actual={file3_exists}"
        )
        assert file3_exists, f"File {filename3} not found on pod {self.pod_obj.name}"
