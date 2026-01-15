import logging

import pytest

from ocs_ci.framework.testlib import ui, skipif_ui_not_support
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    resiliency,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


@ui
@skipif_ui_not_support("external_storage_systems")
@resiliency
@green_squad
class TestExternalStorageSystemsUI(object):
    """
    Test External Storage Systems User Interface

    Note: @resiliency marker is used to skip Ceph health checks for Fusion/FDF
    deployments without OCS cluster configured.
    """

    @pytest.mark.polarion_id("OCS-TBD")
    def test_click_san_storage_radio_navigation(
        self,
        setup_ui_class_factory,
    ):
        """
        Test clicking on SAN storage radio button and verifying navigation to
        Connect Storage Area Network page.

        Steps:
        1. Navigate to Storage -> External Systems page
        2. Navigate to Connect to external storage page (if needed)
        3. Click on SAN storage radio label
        4. Verify that the page navigates to "Connect Storage Area Network" page
        5. Verify the page title is "Connect Storage Area Network"

        """
        setup_ui_class_factory()

        logger.info("Navigate to Storage -> External Systems page")
        page_navigator = PageNavigator()
        external_systems = page_navigator.nav_external_systems_page()

        logger.info("Click on SAN storage radio label")
        external_systems.click_san_storage_radio()

        logger.info("Verify navigation to Connect Storage Area Network page")
        # Wait for the page to load and verify the title element is present
        connect_san_title_locator = external_systems.external_storage_systems_loc[
            "connect_san_title"
        ]
        external_systems.wait_for_element_to_be_visible(
            connect_san_title_locator, timeout=30
        )

        # Verify the title text
        title_text = external_systems.get_element_text(connect_san_title_locator)
        expected_title = "Connect Storage Area Network"
        assert (
            title_text == expected_title
        ), f"Expected title '{expected_title}' but found '{title_text}'"

        logger.info(
            f"Successfully verified navigation to '{expected_title}' page after clicking SAN storage radio"
        )
        external_systems.take_screenshot("san_storage_radio_navigation_success")
