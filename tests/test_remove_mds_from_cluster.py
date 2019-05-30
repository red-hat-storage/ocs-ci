"""
This testcase is for removing the MDS from the cluster
Polarion ID-OCS-361
"""

import logging
import pytest
from utility import templating
from ocsci import tier1, ManageTest
from tests import test_add_mds_to_cluster as obj

log = logging.getLogger(__name__)
_templating = templating.Templating()


@pytest.fixture(scope='class')
def test_fixture(request):
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Verifying the Filesystem exists
    """
    assert obj.verify_fs_exist(self.new_active_count * 2)


def teardown(self):
    """
    Creating the deleted filesystem back
    """
    assert obj.create_ceph_fs(self.fs_data)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCephFilesystemDeletion(ManageTest):
    """
    Testing deletion of Ceph FileSystem
    """
    fs_data = {}
    fs_name = 'myfs-1'
    fs_data['fs_name'] = fs_name
    new_active_count = 1

    def test_cephfilesystem_deletion(self):
        """
        Deleting a Ceph Filesystem
        """
        assert obj.verify_fs_exist(self.new_active_count * 2)
        assert obj.delete_fs(self.fs_name)

