import logging

from ocs import ocp, constants
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
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
    global RBD_POOL
    RBD_POOL = helpers.create_ceph_block_pool()

    global RBD_SECRET_OBJ
    RBD_SECRET_OBJ = helpers.create_secret(constants.CEPHBLOCKPOOL)
    global RBD_SC_OBJ
    log.info("Creating RBD Storage class ")
    RBD_SC_OBJ = helpers.create_storage_class(
        constants.CEPHBLOCKPOOL,
        interface_name=RBD_POOL.name,
        secret_name=RBD_SECRET_OBJ.name
    )

    log.info("Creating CEPHFS Secret")
    global CEPHFS_SECRET_OBJ
    CEPHFS_SECRET_OBJ = helpers.create_secret(constants.CEPHFILESYSTEM)

    global CEPHFS_SC_OBJ
    log.info("Creating CephFS Storage class ")
    CEPHFS_SC_OBJ = helpers.create_storage_class(
        constants.CEPHFILESYSTEM,
        helpers.get_cephfs_data_pool_name(),
        CEPHFS_SECRET_OBJ.name
    )


def teardown():
    """
    Tearing down the environment
    """
    log.info("Deleting RBD PVC")
    RBD_PVC_OBJ.delete()
    log.info("Deleting CEPH BLOCK POOL")
    RBD_POOL.delete()
    log.info("Deleting RBD Secret")
    RBD_SECRET_OBJ.delete()
    log.info("Deleting CephFS Storageclass")
    RBD_SC_OBJ.delete()
    log.info("Deleting CEPHFS Secret")
    CEPHFS_SECRET_OBJ.delete()
    log.info("Deleting CephFS Storageclass")
    CEPHFS_SC_OBJ.delete()
    log.info("Deleting CephFS PVC")
    CEPHFS_PVC_OBJ.delete()


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestOSCBasics(ManageTest):
    """
    This test script covers testcases OCS-336 and OCS-346
    Osc basics such as pool creation, secret creation,
    storageclass creation and pvc creation with default values
    for CephFS and RBD
    """

    def test_ocs_basics(self):
        global RBD_PVC_OBJ, CEPHFS_PVC_OBJ
        log.info('creating pvc for RBD ')
        RBD_PVC_OBJ = helpers.create_pvc(RBD_SC_OBJ.name)
        log.info('creating pvc for CephFS ')
        CEPHFS_PVC_OBJ = helpers.create_pvc(CEPHFS_SC_OBJ.name)
