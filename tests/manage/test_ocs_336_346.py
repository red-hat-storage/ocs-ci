import logging

from ocs import ocp, defaults, constants
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources.ocs import OCS
from resources.pod import get_admin_key_from_ceph_tools
from resources.pvc import PVC
from ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


POD = ocp.OCP(kind='Pod', namespace=ENV_DATA['cluster_namespace'])


@tier1
class TestOSCBasics(ManageTest):
    mons = (
        f'rook-ceph-mon-a.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789,'
        f'rook-ceph-mon-b.{ENV_DATA["cluster_namespace"]}.'
        f'svc.cluster.local:6789,'
        f'rook-ceph-mon-c.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789'
    )

    def test_ocs_336(self):
        """
        Testing basics: secret creation,
        storage class creation and pvc with cephfs
        """
        self.fs_data = defaults.CEPHFILESYSTEM_DICT.copy()
        self.cephfs_secret = defaults.CSI_CEPHFS_SECRET.copy()
        del self.cephfs_secret['data']['userID']
        del self.cephfs_secret['data']['userKey']
        self.cephfs_secret['data']['adminKey'] = (
            get_admin_key_from_ceph_tools()
        )
        self.cephfs_secret['data']['adminID'] = constants.ADMIN_BASE64
        logging.info(self.cephfs_secret)
        secret = OCS(**self.cephfs_secret)
        cleanup_resource(secret, self.cephfs_secret)
        secret.create()
        self.cephfs_sc = defaults.CSI_CEPHFS_STORAGECLASS_DICT.copy()
        self.cephfs_sc['parameters']['monitors'] = self.mons
        self.cephfs_sc['parameters']['pool'] = (
            f"{self.fs_data['metadata']['name']}-data0"
        )
        storage_class = OCS(**self.cephfs_sc)
        cleanup_resource(storage_class, self.cephfs_sc)
        storage_class.create()
        self.cephfs_pvc = defaults.CSI_CEPHFS_PVC.copy()
        pvc = PVC(**self.cephfs_pvc)
        cleanup_resource(pvc, self.cephfs_pvc)
        pvc.create()
        pvc.reload()
        assert 'Bound' in pvc.status
        pvc.delete()
        storage_class.delete()
        secret.delete()

    def test_ocs_346(self):
        """
        Testing basics: secret creation,
         storage class creation  and pvc with rbd
        """
        self.rbd_pool = defaults.CEPHBLOCKPOOL_DICT.copy()
        pool = OCS(**self.rbd_pool)
        cleanup_resource(pool, self.rbd_pool)
        pool.create()
        self.rbd_secret = defaults.CSI_RBD_SECRET.copy()
        del self.rbd_secret['data']['kubernetes']
        self.rbd_secret['data']['admin'] = get_admin_key_from_ceph_tools()
        logging.info(self.rbd_secret)
        secret = OCS(**self.rbd_secret)
        cleanup_resource(secret, self.rbd_secret)
        secret.create()
        self.rbd_sc = defaults.CSI_RBD_STORAGECLASS_DICT.copy()
        self.rbd_sc['parameters']['monitors'] = self.mons
        self.rbd_sc['parameters']['pool'] = self.rbd_pool['metadata']['name']
        del self.rbd_sc['parameters']['userid']
        storage_class = OCS(**self.rbd_sc)
        cleanup_resource(storage_class, self.rbd_sc)
        storage_class.create()
        self.rbd_pvc = defaults.CSI_RBD_PVC.copy()
        pvc = PVC(**self.rbd_pvc)
        cleanup_resource(pvc, self.rbd_pvc)
        pvc.create()
        pvc.reload()
        assert 'Bound' in pvc.status
        pvc.delete()
        storage_class.delete()
        secret.delete()
        pool.delete()


def cleanup_resource(ocs_obj, resource_dict):
    """
    Cleans up resource if already created
    Args:
        ocs_obj: OCS object for ops
        resource_dict: resource dict for deleting resource
    Returns:
        bool: True if resource exists
    """
    try:
        output = str(ocs_obj.get())
        if resource_dict['metadata']['name'] in output:
            log.info(
                f"cleaning up resource {resource_dict['metadata']['name']}"
            )
            ocs_obj.delete()
            return True

    except CommandFailed:
        pass
