import logging
import pytest
from utility import templating
from ocsci import tier1, ManageTest
from tests import test_add_mds_to_cluster as obj
import pdb

log = logging.getLogger(__name__)
_templating = templating.Templating()


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create disks
    """
    pdb.set_trace()
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test
    """
    pdb.set_trace()
    assert obj.verify_fs_exist(self.new_active_count * 2)

def teardown(self):
    """
    Tearing down the environment
    """
    pdb.set_trace()
    assert obj.create_ceph_fs(self.fs_data)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCephFilesystemCreation(ManageTest):
    """
    Testing creation of Ceph FileSystem
    """
    fs_data = {}
    fs_name = 'myfs'
    fs_data['fs_name'] = fs_name
    new_active_count = 1

    def test_cephfilesystem_deletion(self):
        """
        Creating a Ceph Filesystem
        """
        pdb.set_trace()
        assert obj.verify_fs_exist(self.new_active_count * 2)
        assert obj.delete_fs(self.fs_name)

