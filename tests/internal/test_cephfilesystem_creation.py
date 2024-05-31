import logging
import pytest

from ocs_ci.helpers.helpers import (
    create_ceph_file_system,
)
from ocs_ci.ocs.exceptions import CommandFailed
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    green_squad,
)

logger = logging.getLogger(__name__)


class TestCephFileSystemCreation(ManageTest):
    """
    Testing Creation of a filesystem and checking its existence.
    Also checking if the same filesystem can't be created twice.
    """

    @tier2
    @green_squad
    @pytest.mark.polarion_id("OCS-5793")
    def test_Cephfilesystem_creation(self):
        """
        Trying to create more cephfilesystem using the same name.
        Expected Result: It should not create the filesystem and throw error.
        """
        logger.info("Starting test of Ceph Filesystem Creation")
        try:
            cephFS_obj = create_ceph_file_system(
                cephfs_name="test-ceph-fs", label={"use": "test"}
            )

            if cephFS_obj:
                logger.info("CephFile System Created. : test-ceph-fs")
            else:
                logger.error("Unable to create the Ceph File System")
            ct_pod = pod.get_ceph_tools_pod()
            cmd1 = "ceph fs fail test-ceph-fs"
            ct_pod.exec_cmd_on_pod(cmd1)
            cmd2 = "ceph fs rm test-ceph-fs --yes-i-really-mean-it"
            ct_pod.exec_cmd_on_pod(cmd2)
            logger.info("Creating CephFileSystem in the namespace")
            new_cephFS_obj = create_ceph_file_system(
                cephfs_name="test-ceph-fs", label={"use": "test"}
            )
            logger.info(
                f"Not able to create a new ceph fs using same name {new_cephFS_obj}"
            )

        except CommandFailed as e:
            if "Error from server (AlreadyExists)" in str(e):
                logger.info("Test success!")
                assert "Error from server (AlreadyExists)" in str(e)
            else:
                logger.error(
                    f"Command Failed, while creating the ceph file system.\n{str(e)}"
                )
                raise CommandFailed
