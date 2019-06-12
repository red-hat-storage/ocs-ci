import logging
import pytest

from ocsci.testlib import tier1, ManageTest
from ocs.cluster import CephCluster


log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create disks
    """
    self = request.node.cls

    def finalizer():
        teardown()
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test
    """
    global cluster
    cluster = CephCluster(name='rook-ceph')


def teardown():
    """
    Tearing down the environment
    """
    pass


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestClusterUtils(ManageTest):

    username = "client.test"
    caps = "mon 'allow r' osd 'allow rwx'"

    def test_user_creation(self):
        assert cluster.create_user(self.username, self.caps)

    def test_get_user_key(self):
        key = cluster.get_user_key(self.username)
        assert key
        logging.info(key)

    def test_get_admin_key(self):
        key = cluster.get_admin_key()
        assert key

    def test_get_mon_info(self):
        for mon in cluster.mons:
            logging.info(mon.name)
            logging.info(mon.port)

    def test_add_mon(self):
        cur_count = cluster.mon_count
        new_count = cur_count + 1
        cluster.mon_change_count(new_count)
        assert new_count == cluster.mon_count

    def test_add_mds(self):
        cur_count = cluster.mds_count
        new_count = cur_count + 2
        cluster.mds_change_count(new_count)
        assert new_count == cluster.mds_count
