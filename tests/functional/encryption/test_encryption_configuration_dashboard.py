import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import green_squad, tier1
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.helpers.helpers import storagecluster_independent_check
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


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
        logger.info(f"Encryption details from storagecluster Spec: {self.enc_details}")

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
        logger.info(f"Noobaa Spec has mentioned KMS: {self.noobaa_kms}")

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
        logger.assertion(
            f"{context}: expected='{expected_status}', actual='{actual_status}'"
        )
        assert actual_status == expected_status, error_message
        logger.info(f"{context} status is as expected: {actual_status}")

    @pytest.mark.polarion_id("OCS-6300")
    def test_file_block_encryption_configuration_dashboard(self, setup_ui_class):
        """Test the encryption configuration dashboard of File And Block details for correctness.

        Steps:
            1. Navigate to file and block details page
            2. Open encryption details.
            3. verify encryption data with the nooba and storagecluster spec.
        """

        # Navigate to the block and file page
        logger.test_step("Navigate to block and file details page")
        block_and_file_page = PageNavigator().nav_storage_cluster_default_page()
        block_and_file_page.validate_block_and_file_tab_active()

        # Retrieve encryption summary from the dashboard
        logger.test_step("Retrieve encryption summary from dashboard")
        encryption_summary = block_and_file_page.get_block_file_encryption_summary()

        # Validate cluster-wide encryption
        logger.test_step("Validate cluster-wide encryption settings on dashboard")
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
            logger.warning(
                "ClusterWide Encryption details are not a dictionary, skipping checks."
            )

        # Validate storage class encryption
        logger.test_step("Validate storage class encryption settings on dashboard")
        storage_class_details = self.enc_details.get("storageClass", {})
        if isinstance(storage_class_details, dict):
            self.validate_encryption(
                "StorageClass Encryption",
                encryption_summary["storageclass_encryption"]["status"],
                storage_class_details.get("status", False),
                "StorageClass encryption is not showing correctly in the dashboard.",
            )
        else:
            logger.warning(
                "StorageClass details are not a dictionary, skipping checks."
            )

        # Validate in-transit encryption
        logger.test_step("Validate in-transit encryption status on dashboard")
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
        logger.test_step("Navigate to object storage details page")
        object_details_page = PageNavigator().navigate_object_tab()

        logger.test_step("Retrieve object encryption summary from dashboard")
        encryption_summary = object_details_page.get_object_encryption_summary()
        logger.debug(f"Encryption Summary from page: {encryption_summary}")

        # Validate Object Encryption Summary
        logger.test_step("Validate object encryption summary and KMS details")
        logger.assertion(
            f"Object storage encryption status: expected='True', "
            f"actual='{encryption_summary['object_storage']['status']}'"
        )
        assert encryption_summary["object_storage"][
            "status"
        ], "Object encryption summary is wrong"

        if bool(encryption_summary["object_storage"]["kms"]):
            logger.info("Verifying object_storage KMS status")
            logger.assertion(
                f"KMS provider in object storage: expected='{self.noobaa_kms}' "
                f"in '{encryption_summary['object_storage']['kms']}'"
            )
            assert (
                self.noobaa_kms.upper()
                in encryption_summary["object_storage"]["kms"].upper()
            ), "KMS details is not correct"

        # Validate in-transit encryption
        logger.test_step(
            "Validate in-transit encryption status on object storage dashboard"
        )
        self.validate_encryption(
            "InTransit Encryption",
            encryption_summary["intransit_encryption"]["status"],
            self.intransit_encryption_status,
            "InTransit Encryption status is incorrect in the dashboard.",
        )
