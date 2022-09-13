import pytest
import logging
import re

from ocs_ci.helpers.helpers import (
    create_ceph_file_system,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.testlib import tier2
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)


class TestCephFileSystemCreation:
    """
    Testing Creation of a filesystem and checking its existence.
    Also checking if the same filesystem can't be created twice.
    """

    @pytest.fixture()
    def cephFileSystem(self, request):
        """
        Creating the CephFileSystem test-ceph-fs07 for the test.
        """
        logger.info("Creating CephFileSystem in the namespace")
        cephFS_obj = create_ceph_file_system(
            cephfs_name="test-ceph-fs07", label={"use": "test"}
        )

        if cephFS_obj:
            logger.info("CephFile System Created. : test-ceph-fs07")
        else:
            logger.error("Unable to create the Ceph File System")

        def teardown():
            """
            Teardown of the CephFileSystem test-ceph-fs07 after the test.
            """
            logger.info(
                "Teardown of the test, deleting the test Ceph File System test-ceph-fs07"
            )
            cephFS_obj.delete()
            logger.info("Deleted Ceph FIle System: test-ceph-fs07")

        request.addfinalizer(teardown)

    @tier2
    def test_Cephfilesystem_creation(self, cephFileSystem):
        """
        Trying to create more cephfilesystem using the same name.
        Expected Result: It should not create the filesystem and throw error.
        """
        logger.info("Starting test of Ceph Filesystem Creation")
        try:
            cmd1 = "ceph fs fail test-ceph-fs07"
            cmd2 = "ceph fs rm test-ceph-fs07 --yes-i-really-mean-it"
            ct_pod = pod.get_ceph_tools_pod()
            ct_pod.exec_cmd_on_pod(cmd1)
            ct_pod.exec_cmd_on_pod(cmd2)
            create_ceph_file_system(cephfs_name="test-ceph-fs07", label={"use": "test"})
            create_ceph_file_system(cephfs_name="test-ceph-fs07", label={"use": "test"})

        except CommandFailed as e:
            if "Error from server (AlreadyExists)" in str(e):
                logger.info("Test success!")
            else:
                logger.error(
                    f"Command Failed, while creating the ceph file system.\n{str(e)}"
                )
                raise CommandFailed

        cmd3 = "oc logs rook-ceph-operator-6db64cccdc-xfgrk --tail=20"
        command_output = run_cmd(cmd3)
        logger.info(command_output)
        assert re.match(r"(test-ceph-fs07 .* already exists)", command_output)
