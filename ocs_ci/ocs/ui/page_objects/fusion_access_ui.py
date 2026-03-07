import logging

from selenium.webdriver.common.by import By
from ocs_ci.ocs.ui.base_ui import BaseUI, wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.views import FDF_SAN_LOCATORS, SCALE_DASHBOARD_LOCATORS
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import TimeoutExpiredError


logger = logging.getLogger(__name__)


class FusionAccessUI(PageNavigator, BaseUI):
    """
    FusionAccessUI class for add capacity, device replacement, node replacement

    """

    def __init__(self):
        super().__init__()
        self.base_ui = BaseUI()

    def click_connect_external_systems(self):
        """
        Click on 'Connect external systems' button.

        Raises:
            TimeoutExpiredError: If button is not found or clickable
        """
        try:
            self.base_ui.do_click(
                FDF_SAN_LOCATORS["connect_external_storage_button"],
                enable_screenshot=False,
            )
            logger.info("Clicked on Connect external systems button")
        except Exception as e:
            logger.error(f"Failed to click Connect external systems button: {e}")
            raise

    def select_storage_area_network(self):
        """
        Select Storage Area Network radio button.

        Raises:
            TimeoutExpiredError: If radio button is not found
        """
        try:
            wait_for_element_to_be_clickable(
                locator=FDF_SAN_LOCATORS["san_radio_button"], timeout=60
            )
            logger.info("Clicked on Storage Area network radio button")
            self.base_ui.do_click(
                FDF_SAN_LOCATORS["san_radio_button"], enable_screenshot=False
            )
            logger.info("Selected Storage Area Network option")
        except Exception as e:
            logger.error(f"Failed to select Storage Area Network: {e}")
            self.base_ui.take_screenshot("san_selection_error")
            raise

    def click_next_button(self):
        """
        Click the Next button to proceed.

        Raises:
            TimeoutExpiredError: If Next button is not found or clickable
        """
        try:
            self.base_ui.do_click(
                FDF_SAN_LOCATORS["next_button"], enable_screenshot=False
            )
            logger.info("Clicked Next button")
        except Exception as e:
            logger.error(f"Failed to click Next button: {e}")
            self.base_ui.take_screenshot("next_button_error")
            raise

    def select_all_nodes_option(self):
        """
        Select AllNodes (Default) radio button.

        Raises:
            TimeoutExpiredError: If radio button is not found
        """
        try:
            san_element = self.base_ui.get_elements(
                FDF_SAN_LOCATORS["all_nodes_radio"]
            )[0]
            if not san_element.is_selected():
                logger.info("Selecting now")
                self.base_ui.do_click(
                    FDF_SAN_LOCATORS["all_nodes_radio"], enable_screenshot=True
                )
            else:
                logger.info("Selected All Nodes (Default) option")
        except Exception as e:
            logger.error(f"Failed to select AllNodes option: {e}")
            self.base_ui.take_screenshot("all_nodes_error")
            raise

    def enter_lun_group_name(self, lun_group_name):
        """
        Enter LUN group name in the Name text field.

        Args:
            lun_group_name (str): Name for the LUN group

        Raises:
            TimeoutExpiredError: If text field is not found
        """
        try:
            self.base_ui.do_send_keys(
                FDF_SAN_LOCATORS["lun_group_name_input"], lun_group_name
            )
            logger.info(f"Entered LUN group name: {lun_group_name}")
        except Exception as e:
            logger.error(f"Failed to enter LUN group name: {e}")
            self.base_ui.take_screenshot("lun_name_error")
            raise

    def select_luns_from_table(self, num_luns=2):
        """
        Select a subset of LUNs from the available LUNs table.

        Args:
            num_luns (int): Number of LUNs to select (default: 2)

        Returns:
            list: List of selected LUN identifiers

        Raises:
            TimeoutExpiredError: If LUN table is not found
        """
        try:
            selected_luns = []
            for i in range(1, 2):
                # XPath for checkbox in row i
                lun_checkbox_xpath = (
                    f"//table[@aria-label='LUNs table' or contains(@class, 'pf-v5-c-table')]"
                    f"//tbody//tr[{i}]//input[@type='checkbox']"
                )
                lun_checkbox_locator = (lun_checkbox_xpath, By.XPATH)
                self.base_ui.do_click(lun_checkbox_locator, enable_screenshot=False)

                # XPath for LUN identifier in column 2 of row i
                lun_id_xpath = (
                    f"//table[@aria-label='LUNs table' or contains(@class, 'pf-v5-c-table__text')]"
                    f"//tbody//tr[{i}]//td[2]"
                )
                lun_id_locator = (lun_id_xpath, By.XPATH)
                lun_id = self.base_ui.get_element_text(lun_id_locator)
                selected_luns.append(lun_id)
                logger.info(f"Selected LUN: {lun_id}")

            return selected_luns
        except Exception as e:
            logger.error(f"Failed to select LUNs: {e}")
            self.base_ui.take_screenshot("lun_selection_error")
            raise

    def click_connect_and_create(self):
        """
        Click the 'Connect and Create' button.

        Raises:
            TimeoutExpiredError: If button is not found or clickable
        """
        try:
            self.base_ui.do_click(
                FDF_SAN_LOCATORS["connect_and_create_button"], enable_screenshot=True
            )
            logger.info("Clicked Connect and Create button")
        except Exception as e:
            logger.error(f"Failed to click Connect and Create: {e}")
            self.base_ui.take_screenshot("connect_create_error")
            raise

    def navigate_to_san_storage_tab(self):
        """
        Navigate to san_storage tab under external systems page

        Raises:
            TimeoutExpiredError: If tab is not found
        """
        try:
            self.base_ui.do_click(
                FDF_SAN_LOCATORS["san_storage_link"], enable_screenshot=True
            )
            logger.info("Navigated to storage san dashboard")
        except Exception as e:
            logger.error(f"Failed to navigate to storage san dashboard: {e}")
            self.base_ui.take_screenshot("navigation_error")
            raise

    @retry(TimeoutExpiredError, tries=20, delay=30, backoff=1)
    def wait_for_filesystem_creation(self, lun_group_name, timeout=600):
        """
        Wait until the LUN group (filesystem) appears in the Scale Dashboard table.
        """
        try:
            by, xpath = SCALE_DASHBOARD_LOCATORS["lun_group_row_by_name"]
            locator = (
                by,
                xpath.format(lun_group_name=lun_group_name),
            )
            if self.base_ui.check_element_presence(locator=locator, timeout=timeout):
                logger.info(f"LUN group / filesystem '{lun_group_name}' found")
                return lun_group_name

            raise TimeoutExpiredError(
                f"LUN group '{lun_group_name}' not found within timeout"
            )

        except Exception as e:
            logger.warning(f"Waiting for filesystem creation: {e}")
            raise

    @retry(AssertionError, tries=20, delay=30)
    def verify_lun_group_connection(self, lun_group_name):
        """
        Verify Scale connection and LUN group health.
        """
        try:
            # Dashboard connection must be green
            assert self.base_ui.check_element_presence(
                SCALE_DASHBOARD_LOCATORS["scale_connection_green"]
            ), "Scale dashboard connection is not green"

            # # LUN group row exists
            by, xpath = SCALE_DASHBOARD_LOCATORS["lun_group_row_by_name"]
            row_locator = (
                by,
                xpath.format(lun_group_name=lun_group_name),
            )

            assert self.base_ui.check_element_presence(
                row_locator
            ), f"LUN group '{lun_group_name}' not found"

            # LUN group status OK
            by, xpath = SCALE_DASHBOARD_LOCATORS["lun_group_status_ok_by_name"]
            status_locator = (
                by,
                xpath.format(lun_group_name=lun_group_name),
            )
            assert self.base_ui.check_element_presence(
                status_locator
            ), f"LUN group '{lun_group_name}' is not healthy"

            logger.info(
                f"LUN group '{lun_group_name}' connection verified successfully"
            )

        except Exception as e:
            logger.error(f"LUN group connection verification failed: {e}")
            self.base_ui.take_screenshot("lun_connection_error")
            raise

    @retry(AssertionError, tries=20, delay=30)
    def verify_filesystem_status(self, filesystem_name):
        """
        Verify that the filesystem (LUN group) status is OK (green).
        """
        try:
            by, xpath = SCALE_DASHBOARD_LOCATORS["lun_group_status_ok_by_name"]
            locator = (
                by,
                xpath.format(lun_group_name=filesystem_name),
            )

            assert self.base_ui.check_element_presence(
                locator
            ), f"Filesystem '{filesystem_name}' is not in OK state"

            logger.info(f"Filesystem '{filesystem_name}' status verified: OK")

        except Exception as e:
            logger.error(f"Filesystem status verification failed: {e}")
            self.base_ui.take_screenshot("filesystem_status_error")
            raise
