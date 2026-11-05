"""
UI tests for Volume Health Card on Block and File tab
Extended to support both Internal and External mode clusters
"""

import logging
import pytest
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    runs_on_provider,
    skipif_mcg_only,
    skipif_ocs_version,
    tier1,
    ui,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.resources.storage_cluster import get_default_storagecluster

logger = logging.getLogger(__name__)


@ui
@black_squad
@skipif_ocs_version("<4.23")
@runs_on_provider
@skipif_mcg_only
class TestVolumeHealthCardHealthy(ManageTest):
    """
    Test Volume Health Card in healthy state on the Block and File tab.
    Supports both Internal and External mode clusters.
    """

    def get_storage_mode(self):
        """
        Determine if cluster is running in external mode.

        Returns:
            bool: True if external mode, False if internal mode
        """
        try:
            sc_obj = get_default_storagecluster()
            is_external = (
                sc_obj.get()
                .get("spec", {})
                .get("externalStorage", {})
                .get("enable", False)
            )
            logger.info(
                f"Storage mode detected: {'External' if is_external else 'Internal'}"
            )
            return is_external
        except Exception as e:
            logger.warning(
                f"Could not determine storage mode: {e}, defaulting to internal"
            )
            return False

    def navigate_to_volume_health_card(self, page_nav):
        """
        Navigate to Volume Health Card based on storage mode.

        For Internal Mode:
            Storage -> Storage Cluster -> Block and File tab

        For External Mode:
            Storage -> External Systems -> External Storage Cluster -> Block and File tab

        Args:
            page_nav: PageNavigator instance

        Returns:
            tuple: (sc_page, is_external) - StorageCluster/ExternalStorageCluster page object and mode flag
        """
        is_external = self.get_storage_mode()

        if is_external:
            logger.info(
                "External mode detected - navigating to External Storage Systems page"
            )
            logger.test_step(
                "Navigate to External Systems -> External Storage Cluster -> Block and File tab"
            )
            external_systems_page = page_nav.nav_external_systems_page()
            external_sc_page = external_systems_page.nav_to_external_storage_cluster()
            sc_page = external_sc_page.nav_block_and_file_tab()
        else:
            logger.info("Internal mode detected - navigating to Storage Cluster page")
            logger.test_step("Navigate to Storage Cluster -> Block and File tab")
            sc_page = page_nav.nav_storage_cluster_default_page()
            sc_page = sc_page.nav_block_and_file_tab()

        return sc_page, is_external

    @tier1
    @pytest.mark.polarion_id("OCS-8264")
    def test_volume_health_card_healthy_state(self, setup_ui_class_factory):
        """
        Test Volume Health Card shows healthy state when no PVC health issues exist.
        Works for both Internal and External mode clusters.

        Steps:
            1. Detect storage mode (internal vs external).
            2. Navigate to appropriate page:
               - Internal: Storage Cluster -> Block and File tab
               - External: External Systems -> External Storage Cluster -> Block and File tab
            3. Verify Volume Health Card is present.
            4. Verify card shows healthy state (success icon visible).
            5. Verify 'No issues found.' text is displayed.
            6. Verify 'View all PVCs' link navigates to PVC list page.
        """
        setup_ui_class_factory()
        page_nav = PageNavigator()

        # Navigate based on storage mode
        sc_page, is_external = self.navigate_to_volume_health_card(page_nav)

        mode_label = "External" if is_external else "Internal"
        logger.info(f"Testing Volume Health Card in {mode_label} mode")

        logger.info("Accessing Volume Health Card")
        card = sc_page.get_volume_health_card()

        logger.test_step(
            f"Verify Volume Health Card shows healthy state ({mode_label} mode)"
        )
        is_present = card.is_card_present()
        logger.assertion(f"Card present: expected=True, actual={is_present}")
        assert (
            is_present
        ), f"Volume Health Card not found on Block and File tab ({mode_label} mode)"

        is_healthy = card.is_healthy()
        logger.assertion(f"Card healthy: expected=True, actual={is_healthy}")
        assert (
            is_healthy
        ), f"Volume Health Card should show healthy state ({mode_label} mode)"

        no_issues_text = card.get_no_issues_text()
        logger.assertion(
            f"No issues text: expected='No issues found.', actual='{no_issues_text}'"
        )
        assert (
            no_issues_text == "No issues found."
        ), f"Expected 'No issues found.', got: {no_issues_text} ({mode_label} mode)"

        logger.test_step(f"Verify 'View all PVCs' navigation link ({mode_label} mode)")
        card.take_screenshot(f"healthy_state_before_click_{mode_label.lower()}")
        pvc_list_page = card.click_view_all_pvcs()

        current_url = pvc_list_page.driver.current_url
        logger.assertion(f"URL contains 'persistentvolumeclaims': {current_url}")
        assert (
            "persistentvolumeclaims" in current_url
        ), f"Expected URL to contain 'persistentvolumeclaims', got: {current_url}"

        logger.info(f"Volume Health Card healthy state test passed ({mode_label} mode)")
