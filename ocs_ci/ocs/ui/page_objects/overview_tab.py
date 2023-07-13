from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationDefaultTab,
)


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
        return self.wait_until_expected_text_is_found(
            locator=self.validation_loc["storagesystem-status-card"],
            timeout=30,
            expected_text="Storage System",
        )
