"""
Basic test for creating PVC with default StorageClass - RBD-CSI
"""

import logging
import pytest
from ocs import defaults
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources.ocs import OCS
from resources import pod

log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    This is a test fixture
    """
    self = request.node.cls

    def finalizer():
        teardown()
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment - Creating secret
    """
    global SECRET
    self.rbd_secret = defaults.CSI_RBD_SECRET.copy()
    del self.rbd_secret['data']['kubernetes']
    self.rbd_secret['data']['admin'] = pod.get_admin_key_from_ceph_tools()
    SECRET = OCS(**self.rbd_secret)
    assert SECRET.create()


def teardown():
    """
    Tearing down the environment
    """
    log.info("Deleting PVC")
    PVC.delete()
    log.info("Deleting StorageClass")
    STORAGE_CLASS.delete()
    log.info("Deleting Secret")
    SECRET.delete()


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCaseOCS347(ManageTest):
    mons = (
        f'rook-ceph-mon-a.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789,'
        f'rook-ceph-mon-b.{ENV_DATA["cluster_namespace"]}.'
        f'svc.cluster.local:6789,'
        f'rook-ceph-mon-c.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789'
    )

    def test_ocs_347(self):
        """
        Testing default storage class creation and pvc creation using rbd
        """
        global PVC, STORAGE_CLASS
        rbd_sc = defaults.CSI_RBD_STORAGECLASS_DICT.copy()
        rbd_sc['parameters']['monitors'] = self.mons
        STORAGE_CLASS = OCS(**rbd_sc)
        assert STORAGE_CLASS.create()
        rbd_pvc = defaults.CSI_RBD_PVC.copy()
        PVC = OCS(**rbd_pvc)
        assert PVC.create()
        get_pv = PVC.get()
        assert 'Bound' == get_pv['status']['phase']
