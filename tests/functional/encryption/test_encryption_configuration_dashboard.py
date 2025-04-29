import pytest
import logging

log = logging.getLogger(__name__)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.framework.pytest_customization.marks import green_squad, tier1
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.helpers.helpers import storagecluster_independent_check


@green_squad
@tier1
class TestEncryptionConfigurationDashboard:
    @pytest.fixture(autouse=True)
    def encryption_status(self):
        """
        Collect Encryption status from storagecluster and noobaa spec.
        """
        # Retrieve encryption details
        cluster_name = (
            constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE
            if storagecluster_independent_check()
            else constants.DEFAULT_CLUSTERNAME
        )

        sc_obj = StorageCluster(
            resource_name=cluster_name,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        self.enc_details = sc_obj.data["spec"].get("encryption", {})
        self.intransit_encryption_status = (
            sc_obj.data["spec"]
            .get("network", {})
            .get("connections", {})
            .get("encryption", {})
            .get("enabled", False)
        )
        log.info(f"Encryption details from storagecluster Spec: {self.enc_details}")

        noobaa_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

        self.noobaa_kms = (
            noobaa_obj.data["spec"]
            .get("security", {})
            .get("kms", {})
            .get("connectionDetails", {})
            .get("KMS_PROVIDER", None)  # Provide a default value of None if not found
        )
        log.info(f"Noobaa Spec has mentioned KMS: {self.noobaa_kms}")

    def validate_encryption(
        self, context, actual_status, expected_status, error_message
    ):
        """Helper function to validate encryption details

        Args:
            context (str): Encryption Type
            actual_status (str): Encryption status in the spec file
            expected_status (str): Encryption status shown on the dashboard.
            error_message (str): Error message to display.
        """
        assert actual_status == expected_status, error_message
        log.info(f"{context} status is as expected: {actual_status}")

    @pytest.mark.polarion_id("OCS-6300")
    def test_file_block_encryption_configuration_dashboard(self, setup_ui_class):
        """Test the encryption configuration dashboard of File And Block details for correctness.

        Steps:
            1. Navigate to file and block details page
            2. Open encryption details.
            3. verify encryption data with the nooba and storagecluster spec.
        """

        # Navigate to the block and file page
        block_and_file_page = (
            PageNavigator()
            .nav_odf_default_page()
            .nav_storage_systems_tab()
            .nav_storagecluster_storagesystem_details()
            .nav_block_and_file()
        )

        # Retrieve encryption summary from the dashboard
        encryption_summary = block_and_file_page.get_block_file_encryption_summary()

        # Validate cluster-wide encryption
        cluster_wide_details = self.enc_details.get("clusterWide", {})
        if isinstance(cluster_wide_details, dict):
            self.validate_encryption(
                "ClusterWide Encryption",
                encryption_summary["cluster_wide_encryption"]["status"],
                cluster_wide_details.get("status", False),
                "ClusterWide Encryption is not showing correctly in the dashboard.",
            )
            self.validate_encryption(
                "ClusterWide KMS",
                encryption_summary["cluster_wide_encryption"]["kms"],
                cluster_wide_details.get("kms", {}).get("enable", False),
                "KMS is not mentioned in the encryption summary.",
            )
        else:
            log.warning(
                "ClusterWide Encryption details are not a dictionary, skipping checks."
            )

        # Validate storage class encryption
        storage_class_details = self.enc_details.get("storageClass", {})
        if isinstance(storage_class_details, dict):
            self.validate_encryption(
                "StorageClass Encryption",
                encryption_summary["storageclass_encryption"]["status"],
                storage_class_details.get("status", False),
                "StorageClass encryption is not showing correctly in the dashboard.",
            )
        else:
            log.warning("StorageClass details are not a dictionary, skipping checks.")

        # Validate in-transit encryption
        self.validate_encryption(
            "InTransit Encryption",
            encryption_summary["intransit_encryption"]["status"],
            self.intransit_encryption_status,
            "InTransit Encryption status is incorrect in the dashboard.",
        )

    @pytest.mark.polarion_id("OCS-6301")
    def test_object_storage_encryption_configuration_dashboard(self, setup_ui_class):
        """Test the encryption configuration dashboard of Object details for correctness.

        Steps:
            1. Navigate to object details page
            2. Open encryption details.
            3. verify encryption data with the nooba and storagecluster spec.
        """
        # Navigate to the Object Storage page
        object_details_page = (
            PageNavigator()
            .nav_odf_default_page()
            .nav_storage_systems_tab()
            .nav_storagecluster_storagesystem_details()
            .nav_details_object()
        )

        encryption_summary = object_details_page.get_object_encryption_summary()
        log.info(f"Encryption Summary from page : {encryption_summary}")

        # Validate Object Encryption Summary
        assert encryption_summary["object_storage"][
            "status"
        ], "Object encryption summary is wrong"

        if bool(encryption_summary["object_storage"]["kms"]):
            log.info("Verifying object_storage KMS status")
            assert (
                self.noobaa_kms.upper()
                in encryption_summary["object_storage"]["kms"].upper()
            ), "KMS details is not correct"

        # Validate in-transit encryption
        self.validate_encryption(
            "InTransit Encryption",
            encryption_summary["intransit_encryption"]["status"],
            self.intransit_encryption_status,
            "InTransit Encryption status is incorrect in the dashboard.",
        )
