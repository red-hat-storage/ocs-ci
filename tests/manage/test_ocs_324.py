"""
A test for deleting an existing PVC and create a new PVC with the same name
"""
import logging
import pytest

from ocs import ocp, defaults, constants
from ocsci.config import ENV_DATA
from ocsci.testlib import ManageTest, tier1
from tests import helpers

logger = logging.getLogger(__name__)

POD = ocp.OCP(kind='Pod', namespace=ENV_DATA['cluster_namespace'])
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
    Creates the resources needed for the type of interface to be used and
    initializes pvc_data which is used to create/delete PVC by the test.

    For CephBlockPool interface: Creates Secret, CephBlockPool, StorageClass
    For CephFilesystem interface: Creates Secret, CephFilesystem, StorageClass
    """
    logger.info(f"Creating resources for {self.interface_type} interface")

    self.secret_obj = helpers.create_secret(interface_type=self.interface_type)
    assert self.secret_obj, f"Failed to create secret"

    if self.interface_type == constants.CEPHBLOCKPOOL:
        self.cbp_obj = helpers.create_ceph_block_pool()
        assert self.cbp_obj, f"Failed to create block pool"
        self.sc_obj = helpers.create_storage_class(
            interface_type=self.interface_type,
            interface_name=self.cbp_obj.name,
            secret_name=self.secret_obj.name
        )

    elif self.interface_type == constants.CEPHFILESYSTEM:
        assert create_ceph_fs(self)
        assert self.cephfs_obj, f"Failed to create Ceph File System"
        self.sc_obj = helpers.create_storage_class(
            interface_type=self.interface_type,
            interface_name=f"{self.cephfs_obj.name}-data0",
            secret_name=self.secret_obj.name
        )

    assert self.sc_obj, f"Failed to create storage class"
    self.pvc_data = defaults.CSI_PVC_DICT.copy()
    self.pvc_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'pvc'
    )
    self.pvc_data['metadata']['namespace'] = ENV_DATA['cluster_namespace']
    self.pvc_data['spec']['storageClassName'] = self.sc_obj.name


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
        self.cephfs_obj.delete()
    self.secret_obj.delete()


def create_ceph_fs(self):
    """
    Creates a new Ceph File System
    """
    fs_data = defaults.CEPHFILESYSTEM_DICT.copy()
    fs_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'cephfs'
    )
    fs_data['metadata']['namespace'] = ENV_DATA['cluster_namespace']
    self.cephfs_obj = helpers.create_resource(wait=False, **fs_data)
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mds', resource_count=2
    )
    return True


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

        PVC_OBJ = helpers.create_resource(
            desired_status=constants.STATUS_BOUND, **self.pvc_data
        )
        logger.info(f"Deleting PersistentVolumeClaim with name {PVC_OBJ.name}")
        assert PVC_OBJ.delete(), f"Failed to delete PVC"
        PVC_OBJ = helpers.create_resource(
            desired_status=constants.STATUS_BOUND, **self.pvc_data
        )
        logger.info(
            f"PersistentVolumeClaim created with same name {PVC_OBJ.name}"
        )
