"""
A test for creating a CephFS
"""
import logging

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.ocs import OCS

log = logging.getLogger(__name__)

CEPHFS_DELETED = '"{cephfs_name}" deleted'


def verify_fs_exist(pod_count):
    """
    Verifying if a ceph FS exist
    """
    POD = ocp.OCP(kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace'])
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds',
        resource_count=pod_count
    )
    pods = POD.get(selector='app=rook-ceph-mds')['items']
    if len(pods) == pod_count:
        return True
    return False


@tier1
class TestCephFilesystemCreation(ManageTest):
    """
    Testing creation of Ceph FileSystem
    """
    new_active_count = 2
    original_active_count = 1
    fs_name = None

    def setup_class(self):
        self.CEPHFS = ocp.OCP(kind=constants.CEPHFILESYSTEM)
        self.fs_data = self.CEPHFS.get(defaults.CEPHFILESYSTEM_NAME)
        self.fs_name = self.fs_data['metadata']['name']
        self.CEPH_OBJ = OCS(**self.fs_data)

    def teardown_class(self):
        self.fs_data = self.CEPHFS.get(defaults.CEPHFILESYSTEM_NAME)
        self.fs_data['spec']['metadataServer']['activeCount'] = (
            self.original_active_count
        )
        self.CEPH_OBJ.apply(**self.fs_data)
        assert verify_fs_exist(self.original_active_count * 2)

    def test_cephfilesystem_creation(self):
        """
        Creating a Ceph Filesystem
        """

        self.fs_data['spec']['metadataServer']['activeCount'] = (
            self.new_active_count
        )
        self.CEPH_OBJ.apply(**self.fs_data)
        assert verify_fs_exist(self.new_active_count * 2)
