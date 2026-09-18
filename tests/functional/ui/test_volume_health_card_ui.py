"""
UI tests for Volume Health Card on Block and File tab
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
from ocs_ci.ocs.ui.base_ui import SeleniumDriver
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


@ui
@black_squad
@skipif_ocs_version("<4.23")
@runs_on_provider
@skipif_mcg_only
class TestVolumeHealthCardHealthy(ManageTest):
    """
    Test Volume Health Card in healthy state on the Block and File tab.
    """

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_volume_health_card_healthy_state(self, setup_ui_class_factory):
        """
        Test Volume Health Card shows healthy state when no PVC health issues exist.

        Steps:
            1. Navigate to Storage Cluster -> Block and File tab.
            2. Verify Volume Health Card is present.
            3. Verify card shows healthy state (success icon visible).
            4. Verify 'No issues found.' text is displayed.
            5. Verify 'View all PVCs' link navigates to PVC list page.
        """
        logger.test_step("Navigate to Volume Health Card on Block and File tab")
        setup_ui_class_factory()
        logger.info("Navigating to Storage Cluster default page")
        page_nav = PageNavigator().nav_storage_cluster_default_page()
        logger.info("Navigating to Block and File tab")
        sc_page = page_nav.nav_block_and_file_tab()

        logger.info("Accessing Volume Health Card")
        card = sc_page.get_volume_health_card()

        logger.test_step("Verify Volume Health Card shows healthy state")
        is_present = card.is_card_present()
        logger.assertion(f"Card present: expected=True, actual={is_present}")
        assert is_present, "Volume Health Card not found on Block and File tab"

        is_healthy = card.is_healthy()
        logger.assertion(f"Card healthy: expected=True, actual={is_healthy}")
        assert is_healthy, "Volume Health Card should show healthy state"

        no_issues_text = card.get_no_issues_text()
        logger.assertion(
            f"No issues text: expected='No issues found.', actual='{no_issues_text}'"
        )
        assert (
            no_issues_text == "No issues found."
        ), f"Expected 'No issues found.', got: {no_issues_text}"

        logger.test_step("Verify 'View all PVCs' navigation link")
        card.take_screenshot("healthy_state_before_click")
        card.click_view_all_pvcs()

        current_url = SeleniumDriver().current_url
        logger.assertion(f"URL contains 'persistentvolumeclaims': {current_url}")
        assert (
            "persistentvolumeclaims" in current_url
        ), f"Expected URL to contain 'persistentvolumeclaims', got: {current_url}"

        logger.info("Volume Health Card healthy state test passed")
