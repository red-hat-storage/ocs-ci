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
    cluster = CephCluster()
    global username
    username = "client.test"
    caps = "mon 'allow r' osd 'allow rwx'"
    assert cluster.create_user(username, caps)


def teardown():
    """
    Tearing down the environment
    """
    global cluster
    global username
    new_count = cluster.mon_count - 1
    cluster.mon_change_count(new_count)
    assert new_count == cluster.mon_count
    new_mdscount = int(cluster.mds_count / 2) - 1
    cluster.mds_change_count(new_mdscount)
    assert new_mdscount * 2 == cluster.mds_count
    del_cmd = f"ceph auth del {username}"
    cluster.toolbox.exec_ceph_cmd(del_cmd)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestClusterUtils(ManageTest):

    username = "client.test"

    def test_get_user_key(self):
        key = cluster.get_user_key(self.username)
        assert key
        logging.info(key)

    def test_get_admin_key(self):
        """
        By default admin user will be created by rook
        """
        key = cluster.get_admin_key()
        assert key

    def test_get_mon_info(self):
        for mon in cluster.mons:
            logging.info(mon.name)
            logging.info(mon.port)

    def test_add_mon(self):
        cur_count = cluster.mon_count
        logging.info(f"current mon count = {cur_count}")
        new_count = cur_count + 1
        cluster.mon_change_count(new_count)
        assert new_count == cluster.mon_count

    def test_add_mds(self):
        cur_count = int(cluster.mds_count / 2)
        logging.info(f"Current active count = {cur_count}")
        new_count = cur_count + 1
        cluster.mds_change_count(new_count)
        assert new_count * 2 == cluster.mds_count
