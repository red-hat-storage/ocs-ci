import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import tier1, ManageTest
from tests import helpers

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def test_fixture_rbd(request):
    def finalizer():
        teardown_rbd()

    request.addfinalizer(finalizer)
    setup_rbd()


def setup_rbd():
    """
    Setting up the environment
    Creating replicated pool,secret,storageclass for rbd
    """
    log.info("Creating CephBlockPool")
    global RBD_POOL
    RBD_POOL = helpers.create_ceph_block_pool()
    global RBD_SECRET_OBJ
    RBD_SECRET_OBJ = helpers.create_secret(constants.CEPHBLOCKPOOL)
    global RBD_SC_OBJ
    log.info("Creating RBD Storage class ")
    RBD_SC_OBJ = helpers.create_storage_class(
        interface_type=constants.CEPHBLOCKPOOL,
        interface_name=RBD_POOL.name,
        secret_name=RBD_SECRET_OBJ.name
    )


def teardown_rbd():
    """
    Tearing down the environment
    Deleting pod,replicated pool,pvc,storageclass,secret of rbd
    """
    global RBD_PVC_OBJ, RBD_POD_OBJ
    log.info('deleting rbd pod')
    RBD_POD_OBJ.delete()
    log.info("Deleting RBD PVC")
    RBD_PVC_OBJ.delete()
    assert helpers.validate_pv_delete(RBD_SC_OBJ.name)
    log.info("Deleting CEPH BLOCK POOL")
    RBD_POOL.delete()
    log.info("Deleting RBD Secret")
    RBD_SECRET_OBJ.delete()
    log.info("Deleting RBD Storageclass")
    RBD_SC_OBJ.delete()
    log.info("Deleting CephFS PVC")


@pytest.fixture(scope='function')
def test_fixture_cephfs(request):
    def finalizer():
        teardown_fs()

    request.addfinalizer(finalizer)
    setup_fs()


def setup_fs():
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


def teardown_fs():
    global CEPHFS_PVC_OBJ, CEPHFS_POD_OBJ
    log.info('deleting cephfs pod')
    CEPHFS_POD_OBJ.delete()
    log.info('deleting cephfs pvc')
    CEPHFS_PVC_OBJ.delete()
    assert helpers.validate_pv_delete(CEPHFS_SC_OBJ.name)
    log.info("Deleting CEPHFS Secret")
    CEPHFS_SECRET_OBJ.delete()
    log.info("Deleting CephFS Storageclass")
    CEPHFS_SC_OBJ.delete()


@tier1
class TestOSCBasics(ManageTest):
    @pytest.mark.polarion_id("OCS-336")
    def test_basics_rbd(self, test_fixture_rbd):
        """
        Testing basics: secret creation,
        storage class creation,pvc and pod with rbd
        """
        global RBD_PVC_OBJ, RBD_POD_OBJ
        log.info('creating pvc for RBD ')
        pvc_name = helpers.create_unique_resource_name(
            'test-rbd', 'pvc'
        )
        RBD_PVC_OBJ = helpers.create_pvc(RBD_SC_OBJ.name, pvc_name)
        RBD_POD_OBJ = helpers.create_pod(
            constants.CEPHBLOCKPOOL, RBD_PVC_OBJ.name)

    @pytest.mark.polarion_id("OCS-346")
    def test_basics_cephfs(self, test_fixture_cephfs):
        """
        Testing basics: secret creation,
         storage class creation, pvc and pod with cephfs
        """
        global CEPHFS_PVC_OBJ, CEPHFS_POD_OBJ
        log.info('creating pvc for CephFS ')
        pvc_name = helpers.create_unique_resource_name(
            'test-cephfs', 'pvc'
        )
        CEPHFS_PVC_OBJ = helpers.create_pvc(
            CEPHFS_SC_OBJ.name, pvc_name=pvc_name)
        log.info('creating cephfs pod')
        CEPHFS_POD_OBJ = helpers.create_pod(
            constants.CEPHFILESYSTEM, CEPHFS_PVC_OBJ.name)
