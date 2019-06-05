"""
A test for creating a CephFS
"""
import os
import logging
import pytest

from ocs import ocp, defaults
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources.ocs import OCS

log = logging.getLogger(__name__)

CEPHFS_YAML = os.path.join(
    "templates/ocs-deployment", "cephfilesystem_new.yaml"
)
TEMP_YAML_FILE = 'test_cephfilesystem.yaml'
CEPHFS_DELETED = '"{cephfs_name}" deleted'

POD = ocp.OCP(kind='Pod', namespace=ENV_DATA['cluster_namespace'])
CEPH_OBJ = None


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
    global CEPH_OBJ
    CEPH_OBJ = OCS(**self.fs_data)
    OCS.create()

    # TODO: Change to:
    # CEPH_OBJ = helpers.create_resource(**self.fs_data)
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds'
    )
    pods = POD.get(selector='app=rook-ceph-mds')['items']
    assert len(pods) == 2


def teardown():
    """
    Tearing down the environment
    """
    CEPH_OBJ.delete()
    CEPH_OBJ.delete_temp_yaml_file()


def verify_fs_exist(pod_count):
    """
    Verifying if a ceph FS exist
    """
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds',
        resource_count=pod_count
    )
    pods = POD.get(selector='app=rook-ceph-mds')['items']
    if len(pods) == pod_count:
        return True
    return False


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCephFilesystemCreation(ManageTest):
    """
    Testing creation of Ceph FileSystem
    """
    fs_data = defaults.CEPHFILESYSTEM_DICT.copy()
    fs_data['metadata']['name'] = 'my-cephfs1'
    new_active_count = 2

    def test_cephfilesystem_creation(self):
        """
        Creating a Ceph Filesystem
        """
        self.fs_data['spec']['metadataServer']['activeCount'] = (
            self.new_active_count
        )
        CEPH_OBJ.apply(**self.fs_data)
        assert verify_fs_exist(self.new_active_count * 2)
