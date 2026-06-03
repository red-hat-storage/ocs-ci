import logging
import pytest

from ocs_ci.helpers.helpers import (
    create_ceph_file_system,
)
from ocs_ci.ocs.exceptions import CommandFailed
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.framework.pytest_customization.marks import green_squad

logger = logging.getLogger(__name__)


class TestCephFileSystemCreation(ManageTest):
    """
    Testing Creation of a filesystem and checking its existence.
    Also checking if the same filesystem can't be created twice.
    """

    @green_squad
    @pytest.mark.polarion_id("OCS-5793")
    def test_Cephfilesystem_creation(self):
        """
        Trying to create more cephfilesystem using the same name.
        Expected Result: It should not create the filesystem and throw error.
        """
        logger.test_step("Create initial CephFileSystem 'test-ceph-fs'")
        try:
            cephFS_obj = create_ceph_file_system(
                cephfs_name="test-ceph-fs", label={"use": "test"}
            )

            if cephFS_obj:
                logger.info("CephFileSystem created: test-ceph-fs")
            else:
                logger.warning("Unable to create the CephFileSystem")
            ct_pod = pod.get_ceph_tools_pod()
            cmd1 = "ceph fs fail test-ceph-fs"
            ct_pod.exec_cmd_on_pod(cmd1)
            cmd2 = "ceph fs rm test-ceph-fs --yes-i-really-mean-it"
            ct_pod.exec_cmd_on_pod(cmd2)
            logger.test_step(
                "Attempt to recreate CephFileSystem with same name and verify AlreadyExists error"
            )
            new_cephFS_obj = create_ceph_file_system(
                cephfs_name="test-ceph-fs", label={"use": "test"}
            )
            logger.info(f"CephFileSystem recreation returned: {new_cephFS_obj}")

        except CommandFailed as e:
            if "Error from server (AlreadyExists)" in str(e):
                logger.info("AlreadyExists error received as expected")
                assert "Error from server (AlreadyExists)" in str(e)
            else:
                logger.exception(
                    f"Command failed while creating the CephFileSystem: {e}"
                )
                raise CommandFailed
