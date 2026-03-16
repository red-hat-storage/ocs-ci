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
    FusionAccessUI class implements san connection and lun group management

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
        self.base_ui.do_click(FDF_SAN_LOCATORS["connect_external_storage_button"])
        logger.info("Clicked on Connect external systems button")

    def select_storage_area_network(self):
        """
        Select Storage Area Network radio button.

        """

        wait_for_element_to_be_clickable(
            locator=FDF_SAN_LOCATORS["san_radio_button"], timeout=60
        )
        self.base_ui.do_click(FDF_SAN_LOCATORS["san_radio_button"])
        logger.info("Selected Storage Area Network option")

    def click_next_button(self):
        """
        Click the Next button to proceed.

        """
        self.base_ui.do_click(FDF_SAN_LOCATORS["next_button"])
        logger.info("Clicked Next button")

    def select_all_nodes_option(self):
        """
        Select AllNodes (Default) radio button.

        Raises:
            TimeoutExpiredError: If radio button is not found
        """

        elements = self.base_ui.get_elements(FDF_SAN_LOCATORS["all_nodes_radio"])

        if not elements:
            raise TimeoutExpiredError("AllNodes radio button not found")

        san_element = elements[0]

        if not san_element.is_selected():
            logger.info("Selecting All Nodes option")
            self.base_ui.do_click(
                FDF_SAN_LOCATORS["all_nodes_radio"], enable_screenshot=True
            )
        else:
            logger.info("All Nodes (Default) option already selected")

    def enter_lun_group_name(self, lun_group_name):
        """
        Enter LUN group name in the Name text field.

        Args:
            lun_group_name (str): Name for the LUN group

        Raises:
            TimeoutExpiredError: If text field is not found
        """
        self.base_ui.do_send_keys(
            FDF_SAN_LOCATORS["lun_group_name_input"], lun_group_name
        )
        logger.info(f"Entered LUN group name: {lun_group_name}")

    def select_luns_from_table(self, num_luns=1):
        """
        Select a subset of LUNs from the available LUNs table.

        Args:
            num_luns (int): Number of LUNs to select (default: 1)

        Returns:
            list: List of selected LUN identifiers

        Raises:
            TimeoutExpiredError: If LUN table is not found
        """
        selected_luns = []
        for i in range(1, num_luns + 1):
            # XPath for checkbox in row i
            lun_checkbox_xpath = (
                f"//table[@aria-label='LUNs table' or contains(@class, 'pf-v5-c-table')]"
                f"//tbody//tr[{i}]//input[@type='checkbox']"
            )
            lun_checkbox_locator = (lun_checkbox_xpath, By.XPATH)
            self.base_ui.do_click(lun_checkbox_locator)

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

    def click_connect_and_create(self):
        """
        Click the 'Connect and Create' button.

        """
        self.base_ui.do_click(
            FDF_SAN_LOCATORS["connect_and_create_button"], enable_screenshot=True
        )

    def navigate_to_san_storage_tab(self):
        """
        Navigate to san_storage tab under external systems page

        Raises:
            TimeoutExpiredError: If tab is not found
        """
        self.base_ui.do_click(
            FDF_SAN_LOCATORS["san_storage_link"], enable_screenshot=True
        )
        logger.info("Navigated to storage san dashboard")

    @retry((AssertionError, TimeoutExpiredError), tries=20, delay=30)
    def wait_for_filesystem_and_verify_connection(self, lun_group_name):
        """
        Wait for filesystem and verify connection

        Args:
            lun_group_name (str): Name for the LUN group

        """

        # 1. Check Connection (Standard Swap)
        path, strategy = SCALE_DASHBOARD_LOCATORS["scale_connection_green"]
        assert self.base_ui.check_element_presence(
            (strategy, path), timeout=20
        ), "Scale dashboard connection is not green"

        # 2. Check for the SPECIFIC LUN group row
        path_row, strategy_row = SCALE_DASHBOARD_LOCATORS["lun_group_row_by_name"]
        specific_row_xpath = f"{path_row}[contains(., '{lun_group_name}')]"

        assert self.base_ui.check_element_presence(
            (strategy_row, specific_row_xpath), timeout=20
        ), f"LUN group '{lun_group_name}' not found in the table"

        # 3. Check that the SPECIFIC LUN group is OK
        path_ok, strategy_ok = SCALE_DASHBOARD_LOCATORS["lun_group_status_ok_by_name"]
        specific_ok_xpath = f"//tr[contains(., '{lun_group_name}')]//*[text()='OK']"

        assert self.base_ui.check_element_presence(
            (strategy_ok, specific_ok_xpath), timeout=20
        ), f"LUN group '{lun_group_name}' is not healthy (OK status missing)"

        return lun_group_name
