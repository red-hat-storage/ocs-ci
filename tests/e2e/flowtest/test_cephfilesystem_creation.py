import pytest
import logging

from ocs_ci.ocs import constants, defaults
from ocs_ci.helpers.helpers import create_resource, validate_cephfilesystem
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)
cephfs_data = templating.load_yaml(constants.CEPHFILESYSTEM_YAML)
cephfs_data["metadata"]["name"] = "test-ceph-fs"
cephfs_data["metadata"]["namespace"] = defaults.ROOK_CLUSTER_NAMESPACE
cephfs_data["metadata"]["labels"] = {"use": "test"}


class TestCephFileSystemCreation:
    """
    Testing Creation of a filesystem and checking its existence.
    Also checking if the same filesystem can be created more than once or not.
    """

    @pytest.fixture()
    def cephFileSystem(self, request):
        """
        Creating the CephFileSystem testCephFS for the test.
        """
        logger.info("Creating CephFileSystem in the namespace")
        cephFS_obj = create_resource(**cephfs_data)
        cephFS_obj.reload()
        assert validate_cephfilesystem(
            cephFS_obj.name
        ), f"File system {cephFS_obj.name} does not exist"
        logger.info("CephFile System Created. : testCephFS")

        def teardown():
            """
            Teardown of the CephFileSystem testCephFS after the test.
            """
            logger.info(
                "Teardown of the test, deleting the test Ceph File System testCephFS"
            )
            cephFS_obj.delete()
            logger.info("Deleted Ceph FIle System: testCephFS")

        request.addfinalizer(teardown)

    def test_Cephfilesystem_creation(self, cephFileSystem):
        """
        Trying to create more cephfilesystem using the same name.
        Expected Result: It should not create the filesystem and throw error.
        """
        logger.info("Starting test of Ceph Filesystem Creation")
        try:
            new_cepfs_obj = create_resource(**cephfs_data)
            new_cepfs_obj.reload()
        except CommandFailed as e:
            if "Error from server (AlreadyExists)" in str(e):
                logger.info("Test success!")
            else:
                logger.error(
                    f"Command Failed, while creating the ceph file system.\n{str(e)}"
                )
                raise CommandFailed

        ocp = OCP(
            kind=constants.CEPHFILESYSTEM, namespace=defaults.ROOK_CLUSTER_NAMESPACE
        )
        cephfs = ocp.get(selector="use").get("items")
        assert len(cephfs) == 1, "New CephFS got crated with the same name."
