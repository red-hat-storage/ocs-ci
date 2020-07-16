import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, libtest
from ocs_ci.ocs.cluster import CephCluster
from tests import helpers


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


@pytest.fixture
def mon_resource(request):
    """
    A fixture to handle mon resource cleanup,
    this function brings the mon count to what it was before test started
    """
    self = request.node.cls
    mon_count = self.cluster_obj.mon_count
    log.info(f"Mon count before add = {mon_count}")
    self.cluster_obj.scan_cluster()
    self.cluster_obj.cluster.reload()
    self.cluster_obj.cluster.data['spec']['mon']['allowMultiplePerNode'] = True
    self.cluster_obj.cluster.apply(**self.cluster_obj.cluster.data)
    yield
    self.cluster_obj.mon_change_count(mon_count)
    if mon_count != self.cluster_obj.mon_count:
        log.error("Mon teardown failure")
        log.error(
            f"Expected: {mon_count}",
            f"but found {self.cluster_obj.mon_count}"
        )
    log.info("Removed mon")
    self.cluster_obj.cluster.data['spec']['mon'][
        'allowMultiplePerNode'
    ] = False
    self.cluster_obj.cluster.apply(**self.cluster_obj.cluster.data)


@pytest.fixture
def mds_resource(request):
    """
    A fixture to handle mds resource cleanup
    This function brings mds count to what it was before test started
    """
    self = request.node.cls
    we_created_fs = False
    if not self.cluster_obj.cephfs:
        # cephfs doesn't exist , create one for this test
        assert helpers.create_cephfilesystem()
        self.cluster_obj.scan_cluster()
        assert self.cluster_obj.cephfs
        we_created_fs = True
    mds_count = int(self.cluster_obj.mds_count / 2)
    yield
    self.cluster_obj.mds_change_count(mds_count)
    current_count = int(self.cluster_obj.mds_count / 2)
    if mds_count != current_count:
        log.error("MDS teardown failure")
        log.error(f"Expected: {mds_count} but found {current_count}")
    if we_created_fs:
        self.cluster_obj.cephfs.delete()
        self.cluster_obj.cephfs = None


@pytest.fixture
def user_resource(request):
    """
    A fixture for creating user for test and cleaning up after test is done
    """
    self = request.node.cls
    log.info("Creating user")
    assert self.cluster_obj.create_user(self.username, self.caps)
    yield
    del_cmd = f"ceph auth del {self.username}"
    log.info("User deleted")
    self.cluster_obj.toolbox.exec_ceph_cmd(del_cmd)


def setup(self):
    """
    Create CephCluster object to be consumed by tests
    """
    self.cluster_obj = CephCluster()


def teardown(self):
    """
    Make sure at the end cluster is in HEALTH_OK state
    """
    self.cluster_obj.cluster_health_check(timeout=1200)


@libtest
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestClusterUtils(ManageTest):
    # Cluster will be populated in the fixture
    username = "client.test"
    caps = "mon 'allow r' osd 'allow rwx'"

    def test_get_user_key(self, user_resource):
        key = self.cluster_obj.get_user_key(self.username)
        assert key
        logging.info(key)

    def test_get_admin_key(self):
        """
        By default admin user will be created by rook
        """
        key = self.cluster_obj.get_admin_key()
        assert key

    def test_get_mon_info(self):
        for mon in self.cluster_obj.mons:
            logging.info(mon.name)
            logging.info(mon.port)

    def test_add_mon(self, mon_resource):
        cur_count = self.cluster_obj.mon_count
        logging.info(f"current mon count = {cur_count}")
        new_count = cur_count + 1
        self.cluster_obj.mon_change_count(new_count)
        assert new_count == self.cluster_obj.mon_count

    def test_add_mds(self, mds_resource):
        cur_count = int(self.cluster_obj.mds_count / 2)
        logging.info(f"Current active count = {cur_count}")
        new_count = cur_count + 1
        self.cluster_obj.mds_change_count(new_count)
        assert new_count * 2 == self.cluster_obj.mds_count
