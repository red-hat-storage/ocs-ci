import logging

import pytest

from ocs import ocp, constants
from framework import config
from framework.testlib import tier1, ManageTest
from ocs.resources.ocs import OCS
from ocs.resources.pod import get_admin_key_from_ceph_tools
from ocs.resources.pvc import PVC
from tests import helpers
from utility import templating

log = logging.getLogger(__name__)


POD = ocp.OCP(kind='Pod', namespace=config.ENV_DATA['cluster_namespace'])
CEPH_OBJ = None


@pytest.fixture()
def test_fixture(request):
    """
    Create disks
    """
    self = request.node.cls
    setup_fs(self)
    yield
    teardown_fs()


def setup_fs(self):
    """
    Setting up the environment for the test
    """
    global CEPH_OBJ
    self.fs_data = templating.load_yaml_to_dict(constants.CEPHFILESYSTEM_YAML)
    self.fs_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'cephfs'
    )
    self.fs_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']
    CEPH_OBJ = OCS(**self.fs_data)
    CEPH_OBJ.create()
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds'
    )
    pods = POD.get(selector='app=rook-ceph-mds')['items']
    assert len(pods) == 2


def teardown_fs():
    """
    Tearing down the environment
    """
    global CEPH_OBJ
    CEPH_OBJ.delete()


@tier1
class TestOSCBasics(ManageTest):
    mons = (
        f'rook-ceph-mon-a.{config.ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789,'
        f'rook-ceph-mon-b.{config.ENV_DATA["cluster_namespace"]}.'
        f'svc.cluster.local:6789,'
        f'rook-ceph-mon-c.{config.ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789'
    )

    @pytest.mark.polarion_id("OCS-336")
    def test_basics_rbd(self, test_fixture):
        """
        Testing basics: secret creation,
        storage class creation and pvc with cephfs
        """
        self.cephfs_secret = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_SECRET_YAML
        )
        del self.cephfs_secret['data']['userID']
        del self.cephfs_secret['data']['userKey']
        self.cephfs_secret['data']['adminKey'] = (
            get_admin_key_from_ceph_tools()
        )
        self.cephfs_secret['data']['adminID'] = constants.ADMIN_BASE64
        logging.info(self.cephfs_secret)
        secret = OCS(**self.cephfs_secret)
        secret.create()
        self.cephfs_sc = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_STORAGECLASS_YAML
        )
        self.cephfs_sc['parameters']['monitors'] = self.mons
        self.cephfs_sc['parameters']['pool'] = (
            f"{self.fs_data['metadata']['name']}-data0"
        )
        storage_class = OCS(**self.cephfs_sc)
        storage_class.create()
        self.cephfs_pvc = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_PVC_YAML
        )
        pvc = PVC(**self.cephfs_pvc)
        pvc.create()
        log.info(pvc.status)
        assert 'Bound' in pvc.status
        pvc.delete()
        storage_class.delete()
        secret.delete()

    @pytest.mark.polarion_id("OCS-346")
    def test_basics_cephfs(self):
        """
        Testing basics: secret creation,
         storage class creation  and pvc with rbd
        """
        self.rbd_secret = templating.load_yaml_to_dict(
            constants.CSI_RBD_SECRET_YAML
        )
        del self.rbd_secret['data']['kubernetes']
        self.rbd_secret['data']['admin'] = get_admin_key_from_ceph_tools()
        logging.info(self.rbd_secret)
        secret = OCS(**self.rbd_secret)
        secret.create()
        self.rbd_sc = templating.load_yaml_to_dict(
            constants.CSI_RBD_STORAGECLASS_YAML
        )
        self.rbd_sc['parameters']['monitors'] = self.mons
        del self.rbd_sc['parameters']['userid']
        storage_class = OCS(**self.rbd_sc)
        storage_class.create()
        self.rbd_pvc = templating.load_yaml_to_dict(constants.CSI_RBD_PVC_YAML)
        pvc = PVC(**self.rbd_pvc)
        pvc.create()
        assert 'Bound' in pvc.status
        pvc.delete()
        storage_class.delete()
        secret.delete()
