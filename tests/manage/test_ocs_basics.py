import logging

from ocs import ocp, constants
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources import pvc
from tests import helpers

import pytest

log = logging.getLogger(__name__)


POD = ocp.OCP(kind='Pod', namespace=ENV_DATA['cluster_namespace'])


@pytest.fixture(scope='class')
def test_fixture(request):
    def finalizer():
        teardown()

    request.addfinalizer(finalizer)
    setup()


def setup():
    """
    Setting up the environment for the test
    """
    log.info("Creating CephBlockPool")
    pool_obj = helpers.create_ceph_block_pool()

    global RBD_SECRET_OBJ
    RBD_SECRET_OBJ = helpers.create_secret(constants.CEPHBLOCKPOOL)
    global rbd_sc_obj
    log.info("Creating RBD Storage class ")
    rbd_sc_obj = helpers.create_storage_class(
        constants.CEPHBLOCKPOOL,
        interface_name=pool_obj.name,
        secret_name=RBD_SECRET_OBJ.name
    )

    log.info("Creating CEPHFS Secret")
    global CEPHFS_SECRET_OBJ
    CEPHFS_SECRET_OBJ = helpers.create_secret(constants.CEPHFILESYSTEM)

    global fs_sc_obj
    log.info("Creating CephFS Storage class ")
    fs_sc_obj = helpers.create_storage_class(
        constants.CEPHFILESYSTEM,
        helpers.get_cephfs_data_pool_name(),
        CEPHFS_SECRET_OBJ.name
    )


def teardown():
    """
    Tearing down the environment
    """
    global RBD_SECRET_OBJ, CEPHFS_SECRET_OBJ
    log.info("Deleting PVC")
    assert pvc.delete_all_pvcs()
    log.info("Deleting CEPH BLOCK POOL")
    assert helpers.delete_cephblockpool()
    log.info("Deleting RBD Secret")
    RBD_SECRET_OBJ.delete()
    log.info("Deleting CEPHFS Secret")
    CEPHFS_SECRET_OBJ.delete()
    log.info("Deleting Storageclass")
    assert helpers.delete_all_storageclass()


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
@tier1
class TestOSCBasics(ManageTest):
    """
    This test script covers testcases OCS-336 and OCS-346
    Osc basics such as pool creation, secret creation,
    storageclass creation and pvc creation with default values
    for CephFS and RBD
    """

    def test_ocs_bascis(self):
        log.info('creating pvc for RBD ')
        helpers.create_pvc(rbd_sc_obj.name)
        log.info('creating pvc for CephFS ')
        helpers.create_pvc(fs_sc_obj.name)
