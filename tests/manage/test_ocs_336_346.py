import logging

import pytest

from ocs import ocp, defaults, constants
from ocsci import config
from ocsci.testlib import tier1, ManageTest
from resources.ocs import OCS
from resources.pod import get_admin_key_from_ceph_tools
from resources.pvc import PVC
from tests import helpers

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
    self.fs_data = helpers.get_crd_dict(defaults.CEPHFILESYSTEM_YAML)
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
    def test_ocs_336(self, test_fixture):
        """
        Testing basics: secret creation,
        storage class creation and pvc with cephfs
        """
        self.cephfs_secret = helpers.get_crd_dict(defaults.CSI_CEPHFS_SECRET)
        del self.cephfs_secret['data']['userID']
        del self.cephfs_secret['data']['userKey']
        self.cephfs_secret['data']['adminKey'] = (
            get_admin_key_from_ceph_tools()
        )
        self.cephfs_secret['data']['adminID'] = constants.ADMIN_BASE64
        logging.info(self.cephfs_secret)
        secret = OCS(**self.cephfs_secret)
        secret.create()
        self.cephfs_sc = helpers.get_crd_dict(
            defaults.CSI_CEPHFS_STORAGECLASS_DICT
        )
        self.cephfs_sc['parameters']['monitors'] = self.mons
        self.cephfs_sc['parameters']['pool'] = (
            f"{self.fs_data['metadata']['name']}-data0"
        )
        storage_class = OCS(**self.cephfs_sc)
        storage_class.create()
        self.cephfs_pvc = helpers.get_crd_dict(defaults.CSI_CEPHFS_PVC)
        pvc = PVC(**self.cephfs_pvc)
        pvc.create()
        log.info(pvc.status)
        assert 'Bound' in pvc.status
        pvc.delete()
        storage_class.delete()
        secret.delete()

    @pytest.mark.polarion_id("OCS-346")
    def test_ocs_346(self):
        """
        Testing basics: secret creation,
         storage class creation  and pvc with rbd
        """
        self.rbd_secret = helpers.get_crd_dict(defaults.CSI_RBD_SECRET)
        del self.rbd_secret['data']['kubernetes']
        self.rbd_secret['data']['admin'] = get_admin_key_from_ceph_tools()
        logging.info(self.rbd_secret)
        secret = OCS(**self.rbd_secret)
        secret.create()
        self.rbd_sc = helpers.get_crd_dict(defaults.CSI_RBD_STORAGECLASS_DICT)
        self.rbd_sc['parameters']['monitors'] = self.mons
        del self.rbd_sc['parameters']['userid']
        storage_class = OCS(**self.rbd_sc)
        storage_class.create()
        self.rbd_pvc = helpers.get_crd_dict(defaults.CSI_RBD_PVC)
        pvc = PVC(**self.rbd_pvc)
        pvc.create()
        assert 'Bound' in pvc.status
        pvc.delete()
        storage_class.delete()
        secret.delete()
