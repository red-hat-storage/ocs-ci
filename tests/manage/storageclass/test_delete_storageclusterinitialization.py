import logging

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import (
    ManageTest, tier4, polarion_id, ignore_leftovers
)
from tests import helpers

log = logging.getLogger(__name__)


@tier4
class TestDeleteStorageClusterInitialization(ManageTest):
    """
    Test to verify deletion of StorageClusterInitialization
    """

    @polarion_id('')
    @ignore_leftovers
    def test_verify_deleted_sc_cbp_recreated_after_deleting_sci(self):
        """
        The test case verifies that deleted default storage classes
        and cephblockpool will be recreated after deleting
        StorageClusterInitialization.
        Verifies bug 1761926
        """
        sci = OCP(
            kind='StorageClusterInitialization',
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE
        )
        cbp = OCP(
            kind=constants.CEPHBLOCKPOOL,
            resource_name=helpers.default_ceph_block_pool(),
            namespace=defaults.ROOK_CLUSTER_NAMESPACE
        )
        sc_rbd = helpers.default_storage_class(constants.CEPHBLOCKPOOL)
        sc_cephfs = helpers.default_storage_class(constants.CEPHFILESYSTEM)

        # Delete default storage classes and cephblockpool
        sc_rbd.delete()
        sc_cephfs.delete()
        cbp.delete(resource_name=cbp.resource_name)

        sc_rbd.ocp.wait_for_delete(resource_name=sc_rbd.name)
        log.info(f"StorageClass {sc_rbd.name} is deleted.")
        sc_cephfs.ocp.wait_for_delete(resource_name=sc_cephfs.name)
        log.info(f"StorageClass {sc_cephfs.name} is deleted.")
        cbp.wait_for_delete(resource_name=cbp.resource_name)
        log.info(f"CephBlockPool {cbp.resource_name} is deleted.")

        # Delete StorageClusterInitialization
        sci.delete(resource_name=sci.resource_name)
        log.info(
            f"StorageClusterInitialization {sci.resource_name} is deleted."
        )

        # Wait for StorageClusterInitialization, storage classes
        # and CephBlockPool to recreate
        sci.get(resource_name=sci.resource_name, retry=10)
        log.info(
            f"StorageClusterInitialization {sci.resource_name} is recreated."
        )
        cbp.get(resource_name=cbp.resource_name, retry=10)
        log.info(f"CephBlockPool {cbp.resource_name} is recreated.")
        sc_cephfs.ocp.get(resource_name=sc_cephfs.name, retry=10)
        log.info(f"StorageClass {sc_cephfs.name} is recreated.")
        sc_rbd.ocp.get(resource_name=sc_rbd.name, retry=10)
        log.info(f"StorageClass {sc_rbd.name} is recreated.")
