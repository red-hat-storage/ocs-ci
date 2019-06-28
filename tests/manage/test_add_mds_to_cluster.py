"""
A test for creating a CephFS
"""
import logging

import pytest

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.ocs import OCS

log = logging.getLogger(__name__)

CEPHFS_DELETED = '"{cephfs_name}" deleted'

POD = ocp.OCP(kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace'])
CEPHFS = ocp.OCP(kind=constants.CEPHFILESYSTEM)
CEPH_OBJ = None


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
    global CEPHFS, CEPH_OBJ
    CEPHFS = ocp.OCP(kind=constants.CEPHFILESYSTEM)
    self.fs_data = CEPHFS.get(defaults.CEPHFILESYSTEM_NAME)
    self.fs_name = self.fs_data['metadata']['name']
    CEPH_OBJ = OCS(**self.fs_data)


def teardown(self):
    """
    Tearing down the environment
    """
    self.fs_data = CEPHFS.get(defaults.CEPHFILESYSTEM_NAME)
    self.fs_data['spec']['metadataServer']['activeCount'] = (
        self.original_active_count
    )
    CEPH_OBJ.apply(**self.fs_data)
    assert verify_fs_exist(self.original_active_count * 2)


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
    new_active_count = 2
    original_active_count = 1
    fs_name = None

    def test_cephfilesystem_creation(self):
        """
        Creating a Ceph Filesystem
        """

        self.fs_data['spec']['metadataServer']['activeCount'] = (
            self.new_active_count
        )
        CEPH_OBJ.apply(**self.fs_data)
        assert verify_fs_exist(self.new_active_count * 2)
