import logging
import re

from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.framework.testlib import (
    tier1,
    tier2,
    skipif_ui_not_support,
    skipif_ocs_version,
    polarion_id,
    ui,
)
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ibm_cloud_managed,
    skipif_hci_provider_or_client,
    runs_on_provider,
)
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


@black_squad
@skipif_ibm_cloud_managed
class TestUserInterfaceValidation(object):
    """
    Test User Interface Validation

    """

    @ui
    @runs_on_provider
    @tier1
    @polarion_id("OCS-2575")
    @skipif_ui_not_support("validation")
    def test_dashboard_validation_ui(self, setup_ui_class_factory):
        """
        Validate User Interface of OCS/ODF dashboard
        Verify GET requests initiated by kube-probe on odf-console pod [cover bz-2155743]


        Args:
            setup_ui_class_factory: login function on conftest file

        """
        setup_ui_class_factory()

        logger.info("Verify GET requests initiated by kube-probe on odf-console pod")
        pod_odf_console_name = get_pod_name_by_pattern("odf-console")
        pod_odf_console_logs = get_pod_logs(pod_name=pod_odf_console_name[0])
        if (
            re.search(
                "GET /plugin-manifest.json HTTP.*kube-probe", pod_odf_console_logs
            )
            is None
        ):
            raise ValueError("GET request initiated by kube-probe does not exist")

        validation_ui_obj = ValidationUI()
        validation_ui_obj.odf_overview_ui()

    @ui
    @runs_on_provider
    @tier1
    @polarion_id("OCS-4642")
    @skipif_ocs_version("<4.9")
    @skipif_ui_not_support("validation")
    def deprecated_test_odf_storagesystems_ui(self, setup_ui_class_factory):
        """
        ! StorageSystem removed from management-console starting from ODF 4.20

        Validate User Interface for ODF Storage Systems Tab for ODF 4.9

        Args:
            setup_ui_class_factory: login function on conftest file

        """
        setup_ui_class_factory()

        validation_ui_obj = ValidationUI()
        validation_ui_obj.odf_storagesystems_ui()

    @ui
    @tier1
    @runs_on_provider
    @skipif_ocs_version("<4.9")
    @skipif_external_mode
    @skipif_mcg_only
    @polarion_id("OCS-4685")
    @skipif_hci_provider_or_client
    def test_odf_cephblockpool_compression_status(self, setup_ui_class_factory):
        """
        Validate Compression status for cephblockpool at Storage Cluster details and ocs-storagecluster-cephblockpool
        are matching

         Args:
            setup_ui_class: login function on conftest file

        """
        setup_ui_class_factory()

        storage_cluster_details = ValidationUI().nav_storage_cluster_default_page()
        storage_cluster_details.nav_cephblockpool_verify_statusready()

        compression_statuses = (
            storage_cluster_details.get_blockpools_compression_status_from_storagesystem()
        )
        compression_status_expected = "Disabled"
        assert all(
            val == compression_status_expected for val in compression_statuses
        ), (
            "Compression status validation failed:\n"
            f"'Compression status' from StorageSystem details page = {compression_statuses[0]};\n"
            f"'Compression status' from ocs-storagecluster-cephblockpool = {compression_statuses[1]}\n"
            f"Expected: {compression_status_expected}"
        )

    @ui
    @tier2
    @runs_on_provider
    def test_ocs_operator_is_not_present(self, setup_ui_class_factory):
        """
        Validate odf operator is present in the installed operator tab in ui.
        """
        setup_ui_class_factory()

        validation_ui_obj = ValidationUI()
        (
            odf_operator_present,
            ocs_operator_present,
        ) = validation_ui_obj.verify_odf_without_ocs_in_installed_operator()
        assert (
            odf_operator_present
        ), "ODF operator is not present in the installed operator tab"
        assert not ocs_operator_present, (
            "OCS operator is present in the installed operator tab, expected to see only ODF "
            "operator"
        )

    @ui
    @polarion_id("OCS-7681")
    @runs_on_provider
    @skipif_ocs_version("<4.20")
    @skipif_mcg_only
    def test_external_systems_empty_state(self, setup_ui_class_factory):
        """
        Verify that the External Systems page shows correct information
        when no external system is connected.

        Steps:
        1. Navigate to Storage -> External systems
        2. Verify page title 'External systems'
        3. Verify empty state message 'No external systems connected'
        4. Verify description text about configuring storage platform
        5. Verify 'Connect external system' button is present
        6. Verify 'Explore all supported external systems' link is present

        """
        setup_ui_class_factory()

        nav = PageNavigator()
        external_systems = nav.nav_external_systems_page()

        logger.info("Verify page loaded — waiting for page stability")
        external_systems.page_has_loaded()

        logger.info("Verify page title 'External systems'")
        page_title = external_systems.get_element_text(
            external_systems.external_systems_loc["page_title"]
        )
        assert (
            page_title == "External systems"
        ), f"Expected page title 'External systems', got '{page_title}'"
        logger.info("Verify empty state message 'No external systems connected'")
        assert external_systems.check_element_text(
            expected_text="No external systems connected", element="h4"
        ), "Empty state heading 'No external systems connected' not found"

        logger.info("Verify description text")
        assert external_systems.check_element_text(
            expected_text="Start configuring your storage platform",
        ), "Empty state description about configuring storage platform not found"

        logger.info("Verify 'Connect external system' button is present")
        connect_button = external_systems.wait_for_element_to_be_visible(
            external_systems.external_systems_loc["connect_external_system_button"],
            timeout=10,
        )
        assert (
            connect_button.is_displayed()
        ), "'Connect external system' button is not visible"
        assert (
            connect_button.is_enabled()
        ), "'Connect external system' button is not enabled"
        logger.info("Verify 'Explore all supported external systems' link is present")
        assert external_systems.check_element_text(
            expected_text="Explore all supported external systems", element="a"
        ), "'Explore all supported external systems' link not found"
        logger.info(
            "External Systems empty state validated successfully — "
            "all expected elements are present"
        )
        external_systems.take_screenshot()
