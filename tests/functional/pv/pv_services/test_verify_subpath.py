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

log = logging.getLogger(__name__)


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

        # Create a sub directory
        self.pod_obj.exec_cmd_on_pod(
            command=f"mkdir -p {os.path.join(self.pod_obj.get_storage_path(), subdir)}"
        )

        # Create one file in the sub directory
        self.pod_obj.exec_cmd_on_pod(
            command=f"touch {os.path.join(self.pod_obj.get_storage_path(), os.path.join(subdir, filename1))}"
        )

        # Create another pod which can use the sub directory
        pod_obj2 = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            subpath=subdir,
        )

        # On the second pod, verify the presence the file created from the first pod
        assert check_file_existence(
            pod_obj2, os.path.join(pod_obj2.get_storage_path(), filename1)
        ), f"File {filename1} not found on pod {pod_obj2.name}"

        # Create another file from the first pod
        self.pod_obj.exec_cmd_on_pod(
            command=f"touch {os.path.join(self.pod_obj.get_storage_path(), os.path.join(subdir, filename2))}"
        )

        # On the second pod, verify the presence the new file created from the first pod
        assert check_file_existence(
            pod_obj2, os.path.join(pod_obj2.get_storage_path(), filename2)
        ), f"File {filename2} not found on pod {pod_obj2.name}"

        # Create a file from the second pod
        pod_obj2.exec_cmd_on_pod(
            command=f"touch {os.path.join(self.pod_obj.get_storage_path(), filename3)}"
        )

        # On the first pod, verify the presence the file created from the second pod
        assert check_file_existence(
            self.pod_obj,
            os.path.join(
                self.pod_obj.get_storage_path(), os.path.join(subdir, filename3)
            ),
        ), f"File {filename3} not found on pod {self.pod_obj.name}"
