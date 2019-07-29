"""
A test for creating pvc with random sc
"""
import logging
import random

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.ocs.resources import pvc
from tests import helpers
log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def ocs288_fixture(request):
    def finalizer():
        teardown()

    request.addfinalizer(finalizer)
    setup()


def setup():
    """
    Setting up the environment for the test
    """
    global RBD_SECRET_OBJ
    RBD_SECRET_OBJ = helpers.create_secret(constants.CEPHBLOCKPOOL)

    log.info("Creating CEPHFS Secret")
    global CEPHFS_SECRET_OBJ
    CEPHFS_SECRET_OBJ = helpers.create_secret(constants.CEPHFILESYSTEM)

    log.info("Creating RBD Storageclass")
    assert create_multiple_rbd_storageclasses(count=5)

    log.info("Creating CEPHFS Storageclass")
    assert create_storageclass_cephfs()


def teardown():
    """
    Tearing down the environment
    """
    global RBD_SECRET_OBJ, CEPHFS_SECRET_OBJ
    log.info("Deleting PVC")
    assert pvc.delete_pvcs(PVC_OBJS)
    log.info("Deleting CEPH BLOCK POOL")
    assert helpers.delete_cephblockpools(POOL_OBJS)
    log.info("Deleting RBD Secret")
    RBD_SECRET_OBJ.delete()
    log.info("Deleting CEPHFS Secret")
    CEPHFS_SECRET_OBJ.delete()
    log.info("Deleting RBD Storageclass")
    assert helpers.delete_storageclasses(SC_RBD_OBJS)
    log.info("Deleting CephFS Storageclass")
    assert helpers.delete_storageclasses([SC_CEPHFS_OBJ])


def create_multiple_rbd_storageclasses(count=1):
    """
    Function for creating multiple rbd storageclass
    By default if we haven't passed count function will create only one
    storageclass because by default count for creating sc is one

    Args:
         count (int): count specify no of storageclass want to create by
            default count is set to one i.e it will create one sc
    """
    global POOL_OBJS, SC_RBD_OBJS
    POOL_OBJS = [0] * count
    SC_RBD_OBJS = [0] * count
    for sc_count in range(count):
        log.info("Creating CephBlockPool")
        POOL_OBJS[sc_count] = helpers.create_ceph_block_pool()
        SC_RBD_OBJS[sc_count] = helpers.create_storage_class(
            constants.CEPHBLOCKPOOL,
            interface_name=POOL_OBJS[sc_count].name,
            secret_name=RBD_SECRET_OBJ.name
        )

    return True


def create_pvc(storageclass_list, count=1):
    """
    Function for creating pvc and multiple pvc

    Args:
        storageclass_list (list): This will contain storageclass list
        count (int): count specify no of pvc want's to create
    """
    global PVC_OBJS
    PVC_OBJS = [0] * count
    for i in range(count):
        sc_name = random.choice(storageclass_list)
        PVC_OBJS[i] = helpers.create_pvc(sc_name)
        log.info(f"{PVC_OBJS[i].name} got created and got Bounded")
    return True


def create_storageclass_cephfs():
    """
    Function for creating CephFs storageclass
    """
    global SC_CEPHFS_OBJ
    SC_CEPHFS_OBJ = helpers.create_storage_class(
        constants.CEPHFILESYSTEM,
        helpers.get_cephfs_data_pool_name(),
        CEPHFS_SECRET_OBJ.name
    )

    return True


@tier2
@pytest.mark.usefixtures(
    ocs288_fixture.__name__,
)
@pytest.mark.polarion_id("OCS-288")
class TestCreatePVCRandomStorageClass(ManageTest):
    """
    Creating PVC with random SC
    """

    def test_create_pvc_with_random_sc(self):
        storageclass_list = helpers.get_all_storageclass_names()
        if len(storageclass_list):
            assert create_pvc(storageclass_list, count=20)
        else:
            log.error("No Storageclass Found")
