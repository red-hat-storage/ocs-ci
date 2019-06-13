"""
A test for deleting an existing PVC and create a new PVC with the same name
"""
import logging
import pytest

from ocs import constants
from ocsci.testlib import ManageTest, tier1
from tests import helpers

logger = logging.getLogger(__name__)

PVC_OBJ = None


@pytest.fixture(params=[constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM])
def test_fixture(request):
    """
    Parametrized fixture which allows test to be run for different CEPH
    interface.
    The test will run for each interface provided in params.
    """
    self = request.node.cls
    self.interface_type = request.param

    setup(self)
    yield
    teardown(self)


def setup(self):
    """
    Creates the resources needed for the type of interface to be used.

    For CephBlockPool interface: Creates Secret, CephBlockPool, StorageClass
    For CephFilesystem interface: Creates Secret, CephFilesystem, StorageClass
    """
    logger.info(f"Creating resources for {self.interface_type} interface")

    self.secret_obj = helpers.create_secret(interface_type=self.interface_type)
    assert self.secret_obj, f"Failed to create secret"

    self.interface_name = None
    if self.interface_type == constants.CEPHBLOCKPOOL:
        self.cbp_obj = helpers.create_ceph_block_pool()
        assert self.cbp_obj, f"Failed to create block pool"
        self.interface_name = self.cbp_obj.name

    elif self.interface_type == constants.CEPHFILESYSTEM:
        assert helpers.create_cephfilesystem(), (
            f"Failed to create Ceph File System"
        )
        self.interface_name = helpers.get_cephfs_data_pool_name()

    self.sc_obj = helpers.create_storage_class(
        interface_type=self.interface_type,
        interface_name=self.interface_name,
        secret_name=self.secret_obj.name
    )
    assert self.sc_obj, f"Failed to create storage class"


def teardown(self):
    """
    Deletes the resources for the type of interface used.
    """
    logger.info(f"Deleting resources for {self.interface_type} interface")

    PVC_OBJ.delete()
    self.sc_obj.delete()
    if self.interface_type == constants.CEPHBLOCKPOOL:
        self.cbp_obj.delete()
    elif self.interface_type == constants.CEPHFILESYSTEM:
        logger.info("Deleting CephFileSystem")
        assert helpers.delete_all_cephfilesystem()
    self.secret_obj.delete()


@tier1
class TestCaseOCS324(ManageTest):
    """
    Delete PVC and create a new PVC with same name

    https://polarion.engineering.redhat.com/polarion/#/project
    /OpenShiftContainerStorage/workitem?id=OCS-324
    """
    def test_pvc_delete_create_same_name(self, test_fixture):
        """
        TC OCS 324
        """
        global PVC_OBJ

        PVC_OBJ = helpers.create_pvc(sc_name=self.sc_obj.name)
        logger.info(f"Deleting PersistentVolumeClaim with name {PVC_OBJ.name}")
        assert PVC_OBJ.delete(), f"Failed to delete PVC"
        PVC_OBJ = helpers.create_pvc(
            sc_name=self.sc_obj.name, pvc_name=PVC_OBJ.name
        )
        logger.info(
            f"PersistentVolumeClaim created with same name {PVC_OBJ.name}"
        )
