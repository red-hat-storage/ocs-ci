from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationDefaultTab,
)
from ocs_ci.framework import config
from ocs_ci.ocs.ui.page_objects.storage_system_details import StorageSystemDetails


class OverviewTab(DataFoundationDefaultTab):
    """
    Overview tab Class
    Content of Data Foundation/Overview tab (default for ODF bellow 4.13)
    """

    def __init__(self):
        DataFoundationDefaultTab.__init__(self)

    def open_quickstarts_page(self):
        logger.info("Navigate to Quickstarts Page")
        self.scroll_into_view(self.page_nav["quickstarts"])
        self.do_click(locator=self.page_nav["quickstarts"], enable_screenshot=False)

    def wait_storagesystem_popup(self) -> bool:
        logger.info(
            "Wait and check for Storage System under Status card on Overview page"
        )
        is_present = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["storagesystem-status-card"],
            timeout=30,
            expected_text="Storage System",
        )
        if not is_present:
            logger.warning(
                "Storage System under Status card on Data Foundation Overview tab is missing"
            )
        return is_present

    def open_storage_popup_from_status_card(self):
        """
        Open Storage System popup from Status card
        """
        logger.info("Open Storage System popup from Status card")
        self.do_click(
            self.validation_loc["storagesystem-status-card"], enable_screenshot=True
        )

    def validate_block_and_file_ready(self):
        """
        Validate Block and File Storage are Ready

        Returns:
            bool: True if Block and File Storage are Ready, False otherwise
        """
        logger.info("Validate Block and File Storage are Ready")

        if not self.get_elements(self.validation_loc["status-storage-popup-content"]):
            self.open_storage_popup_from_status_card()

        is_ready = not self.wait_until_expected_text_is_found(
            locator=self.validation_loc["block-and-file-health-message"],
            timeout=5,
            expected_text="Block and File service is unhealthy",
        )
        if not is_ready:
            logger.warning("Block and File Storage is not ready")
        return is_ready

    def nav_storage_system_details_from_storage_status_popup(self):
        """
        Navigate to Storage System Details from Storage Status popup

        Returns:
            StorageSystemDetails: Storage System Details page
        """
        logger.info("Navigate to Data Foundation from Storage Status popup")
        logger.info(
            "Click on storage system hyperlink from Storage System pop-up "
            "under Status Card on Data Foundation Overview page"
        )
        if not self.get_elements(self.validation_loc["status-storage-popup-content"]):
            self.open_storage_popup_from_status_card()

        if config.DEPLOYMENT["external_mode"]:
            self.do_click(
                self.validation_loc["storage-system-external-status-card-hyperlink"],
                enable_screenshot=True,
            )
        else:
            self.do_click(
                self.validation_loc["storage-system-status-card-hyperlink"],
                enable_screenshot=True,
            )

        from ocs_ci.ocs.ui.page_objects.storage_system_details import (
            StorageSystemDetails,
        )

        return StorageSystemDetails()

    def validate_overview_tab_active(self) -> bool:
        """
        Validate Overview tab is active

        Returns:
            bool: True if active, False otherwise
        """
        logger.info("Validate Overview tab is active")
        is_default = self.is_overview_tab()
        if not is_default:
            logger.warning("Overview tab is not active")

        return is_default

    def validate_system_capacity_card_present(self) -> bool:
        """
        Validate System Capacity Card is present on Overview page

        Returns:
            bool: True if present, False otherwise
        """
        is_present = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["system-capacity"],
            expected_text="System raw capacity",
        )
        if not is_present:
            logger.warning(
                "System raw capacity Card not found on OpenShift Data Foundation Overview page"
            )
        return is_present

    def nav_storage_system_details_via_system_capacity_card(self):
        """
        Navigate to Storage System Details via System Capacity Card

        Returns:
            StorageSystemDetails: Storage System Details page
        """
        logger.info("Navigate to Storage System Details via System Capacity Card")
        self.do_click(
            self.validation_loc["odf-capacityCardLink"], enable_screenshot=True
        )
        return StorageSystemDetails()

    def validate_performance_card_header_present(self) -> bool:
        """
        Validate Performance Card is present on Overview page

        Returns:
            bool: True if present, False otherwise
        """
        is_present = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["performance-card"],
            expected_text="Performance",
            timeout=15,
        )
        if not is_present:
            logger.warning(
                "Performance Card not found on OpenShift Data Foundation Overview page"
            )
        return is_present

    def nav_storage_systems_details_via_performance_card(self):
        """
        Navigate to Storage System Details via Performance Card

        Returns:
            StorageSystemDetails: Storage System Details page
        """
        logger.info("Navigate to Storage System Details via Performance Card")
        self.do_click(
            self.validation_loc["odf-performanceCardLink"], enable_screenshot=True
        )
        return StorageSystemDetails()
