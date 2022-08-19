import pytest
import logging

from ocs_ci.ocs import constants, defaults
from ocs_ci.helpers.helpers import create_resource, validate_cephfilesystem
from ocs_ci.ocs.exceptions import ResourceAlreadyExists
from ocs_ci.utility import templating


logger = logging.getLogger(__name__)
cephFS_data = templating.load_yaml(constants.CEPHFILESYSTEM_YAML)
cephFS_data["metadata"]["name"] = "test-ceph-fs"
cephFS_data["metadata"]["namespace"] = defaults.ROOK_CLUSTER_NAMESPACE


class TestCephFileSystemCreation:
    """
    Testing Creation of a filesystem and checking its existence.
    Also checking if the same filesystem can be created more than once or not.
    """

    @pytest.fixture()
    def cephFileSystem(self):
        logging.info("Creating CephFileSystem in the namespace")
        cephFS_obj = create_resource(**cephFS_data)
        cephFS_obj.reload()
        assert validate_cephfilesystem(
            cephFS_obj.name
        ), f"File system {cephFS_obj.name} does not exist"
        logging.info("CephFile System Created. : testCephFS")
        yield cephFS_obj
        logging.info(
            "Teardown of the test, deleting the test Ceph File System testCephFS"
        )
        cephFS_obj.delete()
        logging.info("Deleted Ceph FIle System: testCephFS")

    def test_Cephfilesystem_creation(self, cephFileSystem):
        """
        Trying to create more cephfilesystem using the same name.
        Expected Result: It should not create the filesystem and throw error.
        """
        logging.info("Starting test of Ceph Filesystem Creation")
        try:
            new_cepfs_obj = create_resource(**cephFS_data)
            new_cepfs_obj.reload()
        except ResourceAlreadyExists:
            logging.info("Unable to create the Ceph FS with the same name. Test Passed")
