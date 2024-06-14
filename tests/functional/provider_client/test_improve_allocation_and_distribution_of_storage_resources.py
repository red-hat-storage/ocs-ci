import pytest
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_client import StorageClient
from ocs_ci.helpers.helpers import (
    get_all_storageclass_names,
    verify_block_pool_exists,
    create_storage_class,
    get_cephfs_data_pool_name,
    create_ceph_block_pool,
)
from ocs_ci.ocs.rados_utils import (
    verify_cephblockpool_status,
    check_phase_of_rados_namespace,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    green_squad,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
)

log = logging.getLogger(__name__)


@tier1
@green_squad
@skipif_ocs_version("<4.16")
@skipif_ocp_version("<4.16")
@skipif_external_mode
@runs_on_provider
@skipif_managed_service
class TestStorageResourceAllocationAndDistribution(ManageTest):
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, request):
        """
        Setup method for the class


        """
        self.storage_client = StorageClient()
        self.storage_class_claims = [
            constants.CEPHBLOCKPOOL_SC,
            constants.CEPHFILESYSTEM_SC,
        ]
        self.native_storageclient_name = "ocs-storagecluster"

    def test_storage_allocation_for_a_storageclient_for_storageclaims_creation(
        self,
        secret_factory,
    ):
        """
        This test is to verify that for storageclaims created for a storageclient new radosnamespaces gets created
        using same or different storageprofiles.

        validate at Provider side:
        1. Verify that only one CephBlockPool remains
        2. Verify for each block type storageclaim creates a new radosnamespace corresponding to
        the storageprofile check the radosnamespaces are in "Ready" status
        3. Verify storageclassrequests gets created for the storageclaim
        4. Verify storageclasses gets created
        5. Verify storgeclass creation works as expected.
        6. Verify the same behavior for storageclaims created with different storageprofiles
        7. Verify data is isolated between the consumers sharing the same blockpool

        """
        # Check if native storageclient available else create storageclient
        if not self.storage_client.check_storageclient_availability(
            storage_client_name=self.native_storageclient_name
        ):

            self.storage_client.create_native_storage_client(
                namespace_to_create_storage_client=config.ENV_DATA["cluster_namespace"]
            )
            self.storage_client.verify_native_storageclient()

        # Validate cephblockpool created
        assert verify_block_pool_exists(
            constants.DEFAULT_BLOCKPOOL
        ), f"{constants.DEFAULT_BLOCKPOOL} is not created"
        assert verify_cephblockpool_status(), "the cephblockpool is not in Ready phase"

        # Validate radosnamespace created and in 'Ready' status
        assert (
            check_phase_of_rados_namespace()
        ), "The radosnamespace is not in Ready phase"

        # Validate storageclassrequests created
        assert self.storage_client.verify_storagerequest_exists(
            storageclient_name=self.native_storageclient_name
        ), "Storageclass requests are unavailable"

        # Verify storageclasses gets created
        storage_classes = get_all_storageclass_names()
        for storage_class in self.storage_class_claims:
            assert (
                storage_class in storage_classes
            ), "Storage classes ae not created as expected"

        # Verify storgeclass creation works as expected
        secret = secret_factory(interface=self.interface)
        sc_obj = create_storage_class(
            interface_type=constants.CEPHBLOCKPOOL,
            interface_name=get_cephfs_data_pool_name(),
            secret_name=secret.name,
        )
        assert sc_obj, f"Failed to create {sc_obj.name} storage class"
        log.info(f"Storage class: {sc_obj.name} created successfully")

        # Verify the radosnamespace is dispalying in ceph-csi-configs

        # Create a new blockpool
        cbp_obj = create_ceph_block_pool()
        assert cbp_obj, "Failed to create block pool"

        # Create storageclaim with the created blockpool value
        self.storage_client.create_storageclaim(
            storageclaim_name="claim-created-on-added-blockpool",
            type="block",
            storage_client_name=self.native_storageclient_name,
            storageprofile=cbp_obj.name,
        )

        # Verify storageclaim created successfully
        self.storage_client.verify_storage_claim_status(
            storage_client_name=self.native_storageclient_name
        )

        # Validate a new radosnamespace created and in 'Ready' status
        assert (
            check_phase_of_rados_namespace()
        ), "The radosnamespace is not in Ready phase"

        # Validate storageclassrequests created
        assert self.storage_client.verify_storagerequest_exists(
            storageclient_name=self.native_storageclient_name
        ), "Storageclass requests are unavailable"

    def test_storage_allocation_for_hcp_cluster_storageclient_for_storageclaims_creation(
        self,
        secret_factory,
    ):
        """
        This test is to verify that for storageclaims created for a storageclient new radosnamespaces gets created
        using same or different storageprofiles.

        validate at Provider side:
        1. Verify that only one CephBlockPool remains
        2. Verify for each block type storageclaim creates a new radosnamespace corresponding to
        the storageprofile check the radosnamespaces are in "Ready" status
        3. Verify storageclassrequests gets created for the storageclaim
        4. Verify storageclasses gets created
        5. Verify storgeclass creation works as expected.
        6. Verify the same behavior for storageclaims created with different storageprofiles
        7. Verify data is isolated between the consumers sharing the same blockpool

        """
        from tests.libtest.test_provider_create_hosted_cluster import TestProviderHosted

        test_hosted_client = TestProviderHosted()
        test_hosted_client.test_deploy_OCP_and_setup_ODF_client_on_hosted_clusters()
        test_hosted_client.test_storage_client_connected()
        # Validate cephblockpool created
        assert verify_block_pool_exists(
            constants.DEFAULT_BLOCKPOOL
        ), f"{constants.DEFAULT_BLOCKPOOL} is not created"
        assert verify_cephblockpool_status(), "the cephblockpool is not in Ready phase"

        # Validate radosnamespace created and in 'Ready' status
        assert (
            check_phase_of_rados_namespace()
        ), "The radosnamespace is not in Ready phase"

        # Validate storageclassrequests created
        assert self.storage_client.verify_storagerequest_exists(
            storageclient_name=self.native_storageclient_name
        ), "Storageclass requests are unavailable"

        # Verify storageclasses gets created
        storage_classes = get_all_storageclass_names()
        for storage_class in self.storage_class_claims:
            assert (
                storage_class in storage_classes
            ), "Storage classes ae not created as expected"

        # Verify storgeclass creation works as expected
        secret = secret_factory(interface=self.interface)
        sc_obj = create_storage_class(
            interface_type=constants.CEPHBLOCKPOOL,
            interface_name=get_cephfs_data_pool_name(),
            secret_name=secret.name,
        )
        assert sc_obj, f"Failed to create {sc_obj.name} storage class"
        log.info(f"Storage class: {sc_obj.name} created successfully")

        # Verify the radosnamespace is dispalying in ceph-csi-configs

        # Create a new blockpool
        cbp_obj = create_ceph_block_pool()
        assert cbp_obj, "Failed to create block pool"

        # Create storageclaim with the created blockpool value
        self.storage_client.create_storageclaim(
            storageclaim_name="claim-created-on-added-blockpool",
            type="block",
            storage_client_name=self.native_storageclient_name,
            storageprofile=cbp_obj.name,
        )

        # Verify storageclaim created successfully
        self.storage_client.verify_storage_claim_status(
            storage_client_name=self.native_storageclient_name
        )

        # Validate a new radosnamespace created and in 'Ready' status
        assert (
            check_phase_of_rados_namespace()
        ), "The radosnamespace is not in Ready phase"

        # Validate storageclassrequests created
        assert self.storage_client.verify_storagerequest_exists(
            storageclient_name=self.native_storageclient_name
        ), "Storageclass requests are unavailable"

    def test_accociated_radosnamespace_gets_deleted_after_deletion_of_storageclient(
        self,
        secret_factory,
    ):
        """
        This test is to verify that accociated radosnamespace will be deleted after deletion of a storageclient.

        validate at Provider side:
        1. Verify that only one CephBlockPool remains
        2. Verify for each block type storageclaim creates a new radosnamespace corresponding to
        the storageprofile check the radosnamespaces are in "Ready" status
        3. Verify storageclassrequests gets created for the storageclaim
        4. Verify storageclasses gets created
        5. Verify storgeclass creation works as expected.
        6. Verify the same behavior for storageclaims created with different storageprofiles
        7. Verify data is isolated between the consumers sharing the same blockpool

        """
        # Check if native storageclient available else create storageclient
        if not self.storage_client.check_storageclient_availability(
            storage_client_name=self.native_storageclient_name
        ):

            self.storage_client.create_native_storage_client(
                namespace_to_create_storage_client=config.ENV_DATA["cluster_namespace"]
            )
            self.storage_client.verify_native_storageclient()

        # Validate cephblockpool created
        assert verify_block_pool_exists(
            constants.DEFAULT_BLOCKPOOL
        ), f"{constants.DEFAULT_BLOCKPOOL} is not created"
        assert verify_cephblockpool_status(), "the cephblockpool is not in Ready phase"

        # Validate radosnamespace created and in 'Ready' status
        assert (
            check_phase_of_rados_namespace()
        ), "The radosnamespace is not in Ready phase"

        # Validate storageclassrequests created
        assert self.storage_client.verify_storagerequest_exists(
            storageclient_name=self.native_storageclient_name
        ), "Storageclass requests are unavailable"

        # Verify storageclasses gets created
        storage_classes = get_all_storageclass_names()
        for storage_class in self.storage_class_claims:
            assert (
                storage_class in storage_classes
            ), "Storage classes ae not created as expected"

        # Verify storgeclass creation works as expected
        secret = secret_factory(interface=self.interface)
        sc_obj = create_storage_class(
            interface_type=constants.CEPHBLOCKPOOL,
            interface_name=get_cephfs_data_pool_name(),
            secret_name=secret.name,
        )
        assert sc_obj, f"Failed to create {sc_obj.name} storage class"
        log.info(f"Storage class: {sc_obj.name} created successfully")

        # Verify the radosnamespace is dispalying in ceph-csi-configs

        # Create a new blockpool
        cbp_obj = create_ceph_block_pool()
        assert cbp_obj, "Failed to create block pool"

        # Create storageclaim with the created blockpool value
        self.storage_client.create_storageclaim(
            storageclaim_name="claim-created-on-added-blockpool",
            type="block",
            storage_client_name=self.native_storageclient_name,
            storageprofile=cbp_obj.name,
        )

        # Verify storageclaim created successfully
        self.storage_client.verify_storage_claim_status(
            storage_client_name=self.native_storageclient_name
        )

        # Validate a new radosnamespace created and in 'Ready' status
        assert (
            check_phase_of_rados_namespace()
        ), "The radosnamespace is not in Ready phase"

        # Validate storageclassrequests created
        assert self.storage_client.verify_storagerequest_exists(
            storageclient_name=self.native_storageclient_name
        ), "Storageclass requests are unavailable"
