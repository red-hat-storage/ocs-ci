import logging
import pytest

from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.cluster import CephCluster


log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create disks
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test
    """
    self.cluster = CephCluster()
    assert self.cluster.create_user(self.username, self.caps)


def teardown(self):
    """
    Tearing down the environment
    """
    new_count = self.cluster.mon_count - 1
    self.cluster.mon_change_count(new_count)
    assert new_count == self.cluster.mon_count
    new_mdscount = int(self.cluster.mds_count / 2) - 1
    self.cluster.mds_change_count(new_mdscount)
    assert new_mdscount * 2 == self.cluster.mds_count
    del_cmd = f"ceph auth del {self.username}"
    self.cluster.toolbox.exec_ceph_cmd(del_cmd)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestClusterUtils(ManageTest):
    # Cluster will be populated in the fixture
    cluster = None
    username = "client.test"
    caps = "mon 'allow r' osd 'allow rwx'"

    def test_get_user_key(self):
        key = self.cluster.get_user_key(self.username)
        assert key
        logging.info(key)

    def test_get_admin_key(self):
        """
        By default admin user will be created by rook
        """
        key = self.cluster.get_admin_key()
        assert key

    def test_get_mon_info(self):
        for mon in self.cluster.mons:
            logging.info(mon.name)
            logging.info(mon.port)

    def test_add_mon(self):
        cur_count = self.cluster.mon_count
        logging.info(f"current mon count = {cur_count}")
        new_count = cur_count + 1
        self.cluster.mon_change_count(new_count)
        assert new_count == self.cluster.mon_count

    def test_add_mds(self):
        cur_count = int(self.cluster.mds_count / 2)
        logging.info(f"Current active count = {cur_count}")
        new_count = cur_count + 1
        self.cluster.mds_change_count(new_count)
        assert new_count * 2 == self.cluster.mds_count
