import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import (
    create_ceph_file_system,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import tier2


logger = logging.getLogger(__name__)


class TestCephFileSystemCreation:
    """
    Testing Creation of a filesystem and checking its existence.
    Also checking if the same filesystem can't be created twice.
    """

    @pytest.fixture()
    def cephFileSystem(self, request):
        """
        Creating the CephFileSystem test-ceph-fs for the test.
        """
        logger.info("Creating CephFileSystem in the namespace")
        cephFS_obj = create_ceph_file_system(
            cephfs_name="test-ceph-fs", label={"use": "test"}
        )

        if cephFS_obj:
            logger.info("CephFile System Created. : test-ceph-fs")
        else:
            logger.error("Unable to create the Ceph File System")

        def teardown():
            """
            Teardown of the CephFileSystem test-ceph-fs after the test.
            """
            logger.info(
                "Teardown of the test, deleting the test Ceph File System test-ceph-fs"
            )
            cephFS_obj.delete()
            logger.info("Deleted Ceph FIle System: test-ceph-fs")

        request.addfinalizer(teardown)

    @tier2
    def test_Cephfilesystem_creation(self, cephFileSystem):
        """
        Trying to create more cephfilesystem using the same name.
        Expected Result: It should not create the filesystem and throw error.
        """
        logger.info("Starting test of Ceph Filesystem Creation")
        try:
            create_ceph_file_system(cephfs_name="test-ceph-fs", label={"use": "test"})

        except CommandFailed as e:
            if "Error from server (AlreadyExists)" in str(e):
                logger.info("Test success!")
            else:
                logger.error(
                    f"Command Failed, while creating the ceph file system.\n{str(e)}"
                )
                raise CommandFailed

        ocp = OCP(
            kind=constants.CEPHFILESYSTEM,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        cephfs = ocp.get(selector="use").get("items")
        assert len(cephfs) == 1, "New CephFS got crated with the same name."
