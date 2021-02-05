import logging

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import ManageTest, tier4, tier4c, polarion_id

log = logging.getLogger(__name__)


@tier4
class TestDeleteStorageClusterInitialization(ManageTest):
    """
    Test to verify deletion of StorageClusterInitialization
    """

    @polarion_id("OCS-2173")
    @tier4c
    def test_existing_sc_cbp_not_deleted_by_sci_deletion(self):
        """
        The test case verifies that deletion of StorageClusterInitialization
        will not delete or recreate existing default storage classes,
        CephBlockPool and db-noobaa-db-0 PVC.
        Verifies bug 1762822.
        """
        sci = OCP(
            kind="StorageClusterInitialization",
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        )
        cbp = OCP(
            kind=constants.CEPHBLOCKPOOL,
            resource_name=helpers.default_ceph_block_pool(),
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        )
        sc_rbd = helpers.default_storage_class(constants.CEPHBLOCKPOOL)
        sc_cephfs = helpers.default_storage_class(constants.CEPHFILESYSTEM)
        noobaa_pvc = get_all_pvc_objs(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            selector=constants.NOOBAA_APP_LABEL,
        )[0]

        resources = {cbp: None, sc_rbd: None, sc_cephfs: None, noobaa_pvc: None}

        # Get uid
        for resource in resources.keys():
            resources[resource] = resource.get()["metadata"]["uid"]
        sci_uid = sci.get()["metadata"]["uid"]

        # Delete StorageClusterInitialization
        sci.delete(resource_name=sci.resource_name)
        log.info(
            f"StorageClusterInitialization {sci.resource_name} delete "
            f"command succeeded."
        )

        # Wait for StorageClusterInitialization to recreate
        sci.get(resource_name=sci.resource_name, retry=10)
        assert (
            sci_uid != sci.get()["metadata"]["uid"]
        ), "Failed to delete StorageClusterInitialization."
        log.info(f"StorageClusterInitialization {sci.resource_name} is recreated.")

        # Verify uid
        for resource, uid in resources.items():
            name = (
                resource.resource_name if isinstance(resource, OCP) else resource.name
            )
            assert uid == resource.get()["metadata"]["uid"], (
                f"Unexpected: {resource.kind} {name} is recreated after "
                f"deleting StorageClusterInitialization."
            )
        log.info("Verified: Resources remains the same as expected.")
