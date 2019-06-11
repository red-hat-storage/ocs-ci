"""
A test for creating pvc with random sc
"""
import logging
import random

import pytest

from ocs import constants
from ocsci.testlib import tier1, ManageTest
from resources import pvc
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

    log.info("Creating CEPH FileSystem")
    assert helpers.create_cephfilesystem()

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
    assert pvc.delete_all_pvcs()
    log.info("Deleting CEPH BLOCK POOL")
    assert helpers.delete_cephblockpool()
    log.info("Deleting RBD Secret")
    RBD_SECRET_OBJ.delete()
    log.info("Deleting CEPHFS Secret")
    CEPHFS_SECRET_OBJ.delete()
    log.info("Deleting CEPH FILESYSTEM")
    assert helpers.delete_all_cephfilesystem()
    log.info("Deleting Storageclass")
    assert helpers.delete_all_storageclass()


def create_multiple_rbd_storageclasses(count=1):
    """
    Function for creating multiple rbd storageclass
    By default if we haven't passed count function will create only one
    storageclass because by default count for creating sc is one

    :Args count:

    """

    for sc_count in range(count):
        log.info("Creating CephBlockPool")
        pool_obj = helpers.create_ceph_block_pool()
        helpers.create_storage_class(
            constants.CEPHBLOCKPOOL,
            interface_name=pool_obj.name,
            secret_name=RBD_SECRET_OBJ.name
        )

    return True


def create_pvc(storageclass_list, count=1):
    """
    Function for creating pvc and multiple pvc
    :param storageclass_list,count:
    """
    for i in range(count):
        sc_name = random.choice(storageclass_list)
        pvc_obj = helpers.create_pvc(sc_name)
        log.info(f"{pvc_obj.name} got Created and got Bounded")
    return True


def create_storageclass_cephfs():
    """
    Function for creating CephFs storageclass
    :return:
    """
    helpers.create_storage_class(
        constants.CEPHFILESYSTEM,
        helpers.get_cephfs_data_pool_name(),
        CEPHFS_SECRET_OBJ.name
    )

    return True


@tier1
@pytest.mark.usefixtures(
    ocs288_fixture.__name__,
)
class TestCaseOCS288(ManageTest):
    """
    Creating PVC with random SC

    https://polarion.engineering.redhat.com/polarion/#/project/
    OpenShiftContainerStorage/workitem?id=OCS-288
    """

    def test_create_pvc_with_random_sc(self):
        storageclass_list = helpers.get_all_storageclass_name()
        if len(storageclass_list):
            assert create_pvc(storageclass_list, count=20)
        else:
            log.error("No Storageclass Found")
            return False
