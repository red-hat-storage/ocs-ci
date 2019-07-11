"""
A test for creating a CephFS
"""
import logging
import pytest

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.ocs import OCS

from tests import helpers

log = logging.getLogger(__name__)

CEPHFS_DELETED = '"{cephfs_name}" deleted'


@pytest.fixture(scope="class")
def cephfs():
    cephfs = ocp.OCP(kind=constants.CEPHFILESYSTEM)
    return cephfs


@pytest.fixture(scope="class")
def fs_data(cephfs):
    fs_data = cephfs.get(defaults.CEPHFILESYSTEM_NAME)
    return fs_data


@pytest.fixture(scope="class")
def ceph_obj(request, fs_data):
    ceph_obj = OCS(**fs_data)
    original_active_count = 1

    def teardown():
        ceph_obj.apply(**fs_data)
        assert helpers.verify_fs_exists(original_active_count * 2)

    request.addfinalizer(teardown)
    return ceph_obj


@tier1
class TestCephFilesystemCreation(ManageTest):
    """
    Testing creation of Ceph FileSystem
    """
    new_active_count = 2

    def test_cephfilesystem_creation(self, ceph_obj, fs_data):
        """
        Creating a Ceph Filesystem
        """

        fs_data['spec']['metadataServer']['activeCount'] = (
            self.new_active_count
        )
        ceph_obj.apply(**fs_data)
        assert helpers.verify_fs_exist(self.new_active_count * 2)
