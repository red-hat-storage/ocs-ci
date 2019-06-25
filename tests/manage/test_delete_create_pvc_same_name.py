"""
A test for deleting an existing PVC and create a new PVC with the same name
"""
import logging
import pytest

from ocs import constants, exceptions, ocp
from framework import config
from framework.testlib import ManageTest, tier2
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

    For CephBlockPool interface: Creates CephBlockPool, Secret and StorageClass
    For CephFilesystem interface: Creates Secret and StorageClass
    """
    logger.info(f"Creating resources for {self.interface_type} interface")

    self.interface_name = None
    if self.interface_type == constants.CEPHBLOCKPOOL:
        self.cbp_obj = helpers.create_ceph_block_pool()
        assert self.cbp_obj, f"Failed to create block pool"
        self.interface_name = self.cbp_obj.name

    elif self.interface_type == constants.CEPHFILESYSTEM:
        self.interface_name = helpers.get_cephfs_data_pool_name()

    self.secret_obj = helpers.create_secret(interface_type=self.interface_type)
    assert self.secret_obj, f"Failed to create secret"

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

    resources_list = [PVC_OBJ, self.sc_obj, self.secret_obj]
    if self.interface_type == constants.CEPHBLOCKPOOL:
        resources_list.append(self.cbp_obj)

    for resource in resources_list:
        try:
            logger.info(f"Deleting {resource.kind} {resource.name}")
            resource.delete()
        except AttributeError:
            continue
        except exceptions.CommandFailed:
            logger.error(f"Deletion of {resource.kind} {resource.name} failed")


@tier2
@pytest.mark.polarion_id("OCS-324")
class TestDeleteCreatePVCSameName(ManageTest):
    """
    Delete PVC and create a new PVC with same name
    """
    def test_pvc_delete_create_same_name(self, test_fixture):
        """
        TC OCS 324
        """
        global PVC_OBJ

        PVC_OBJ = helpers.create_pvc(sc_name=self.sc_obj.name)
        pv_obj = ocp.OCP(
            kind=constants.PV, namespace=config.ENV_DATA['cluster_namespace']
        )
        backed_pv = PVC_OBJ.get().get('spec').get('volumeName')
        pv_status = pv_obj.get(backed_pv).get('status').get('phase')
        assert constants.STATUS_BOUND in pv_status, (
            f"{pv_obj.kind} {backed_pv} failed to reach {constants.STATUS_BOUND}"
        )

        logger.info(f"Deleting {PVC_OBJ.kind} {PVC_OBJ.name}")
        assert PVC_OBJ.delete(), f"Failed to delete PVC"

        logger.info(f"Creating {PVC_OBJ.kind} with same name {PVC_OBJ.name}")
        PVC_OBJ = helpers.create_pvc(
            sc_name=self.sc_obj.name, pvc_name=PVC_OBJ.name
        )
        backed_pv = PVC_OBJ.get().get('spec').get('volumeName')
        pv_status = pv_obj.get(backed_pv).get('status').get('phase')
        assert constants.STATUS_BOUND in pv_status, (
            f"{pv_obj.kind} {backed_pv} failed to reach {constants.STATUS_BOUND}"
        )
