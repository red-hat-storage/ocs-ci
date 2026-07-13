import logging

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

    def click_connect_external_systems(self):
        """
        Click on 'Connect external systems' button.

        """
        self.do_click(FDF_SAN_LOCATORS["connect_external_storage_button"])
        logger.info("Clicked on Connect external systems button")

    def select_storage_area_network(self):
        """
        Select Storage Area Network radio button.

        """

        wait_for_element_to_be_clickable(
            locator=FDF_SAN_LOCATORS["san_radio_button"], timeout=60
        )
        self.do_click(FDF_SAN_LOCATORS["san_radio_button"])
        logger.info("Selected Storage Area Network option")

    def click_next_button(self):
        """
        Click the Next button to proceed.

        """
        self.do_click(FDF_SAN_LOCATORS["next_button"])
        logger.info("Clicked Next button")

    def enter_image_registry_url(self, image_registry_url):
        """
        Enter the Image registry URL in the text field.

        Args:
            image_registry_url (str): URL of the image registry e.g. quay.io

        """
        self.do_send_keys(
            FDF_SAN_LOCATORS["image_registry_url_input"], image_registry_url
        )
        logger.info(f"Entered Image registry URL: {image_registry_url}")

    def enter_image_repository_name(self, image_repository_name):
        """
        Enter the Image repository name in the text field.

        Args:
            image_repository_name (str): Name of the image repository

        """
        self.do_send_keys(
            FDF_SAN_LOCATORS["image_repository_name_input"], image_repository_name
        )
        logger.info(f"Entered Image repository name: {image_repository_name}")

    def select_secret_key(self):
        """
        Select the last option from the Secret key dropdown.

        Raises:
            TimeoutExpiredError: If the dropdown or its options are not found

        """
        # Open the dropdown
        self.do_click(FDF_SAN_LOCATORS["secret_key_dropdown"])
        logger.info("Opened Secret key dropdown")

        # Wait for dropdown options to appear in the DOM (up to 15 seconds).
        opt_path, opt_by = FDF_SAN_LOCATORS["secret_key_dropdown_options"]
        logger.info("Waiting for Secret key dropdown options to appear...")
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((opt_by, opt_path))
            )
        except Exception as err:
            self.take_screenshot("secret_dropdown_no_options")
            raise TimeoutExpiredError(
                "Secret key dropdown options did not appear after 15 seconds"
            ) from err

        # Get all enabled options
        options = self.get_elements(FDF_SAN_LOCATORS["secret_key_dropdown_options"])
        if not options:
            raise TimeoutExpiredError("No options found in Secret key dropdown")

        # Click the last option
        last_option = options[-1]
        last_option_text = last_option.text
        last_option.click()
        logger.info(f"Selected last Secret key option: '{last_option_text}'")

    def select_all_nodes_option(self):
        """
        Select AllNodes (Default) radio button.

        """

        elements = self.get_elements(FDF_SAN_LOCATORS["all_nodes_radio"])

        if not elements:
            raise TimeoutExpiredError("AllNodes radio button not found")

        san_element = elements[0]

        if not san_element.is_selected():
            logger.info("Selecting All Nodes option")
            self.do_click(FDF_SAN_LOCATORS["all_nodes_radio"], enable_screenshot=True)
        else:
            logger.info("All Nodes (Default) option already selected")

    def enter_lun_group_name(self, lun_group_name):
        """
        Enter LUN group name in the Name text field.

        Args:
            lun_group_name (str): Name for the LUN group

        """
        self.do_send_keys(FDF_SAN_LOCATORS["lun_group_name_input"], lun_group_name)
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
        checkbox_path, checkbox_by = FDF_SAN_LOCATORS["lun_table_checkbox"]
        row_id_path, row_id_by = FDF_SAN_LOCATORS["lun_table_row_id"]
        for i in range(1, num_luns + 1):
            lun_checkbox_locator = (checkbox_path.format(i=i), checkbox_by)
            self.do_click(lun_checkbox_locator)

            lun_id_locator = (row_id_path.format(i=i), row_id_by)
            lun_id = self.get_element_text(lun_id_locator)
            selected_luns.append(lun_id)
            logger.info(f"Selected LUN: {lun_id}")

        return selected_luns

    def click_connect_and_create(self):
        """
        Click the 'Connect and Create' button.

        """
        self.do_click(
            FDF_SAN_LOCATORS["connect_and_create_button"], enable_screenshot=True
        )

    def navigate_to_san_storage_tab(self):
        """
        Navigate to san_storage tab under external systems page

        """
        self.do_click(FDF_SAN_LOCATORS["san_storage_link"], enable_screenshot=True)
        logger.info("Navigated to storage san dashboard")

    @retry((AssertionError, TimeoutExpiredError), tries=40, delay=30, backoff=1)
    def wait_for_filesystem_and_verify_connection(self, lun_group_name):
        """
        Wait for filesystem and verify connection.

        Retries every 30 seconds for up to 20 minutes (40 x 30s) waiting for
        the LUN group to reach a healthy/connected state on the UI.

        Args:
            lun_group_name (str): Name for the LUN group

        """
        # 1. Check Connection (Standard Swap)
        path, strategy = SCALE_DASHBOARD_LOCATORS["scale_connection_green"]
        assert self.check_element_presence(
            (strategy, path), timeout=20
        ), "Scale dashboard connection is not green"
        logger.info("Scale dashboard connection is green")

        # 2. Check for the SPECIFIC LUN group row
        path_row, strategy_row = SCALE_DASHBOARD_LOCATORS["lun_group_row_by_name"]
        specific_row_xpath = f"{path_row}[contains(., '{lun_group_name}')]"

        assert self.check_element_presence(
            (strategy_row, specific_row_xpath), timeout=20
        ), f"LUN group '{lun_group_name}' not found in the table"
        logger.info(f"LUN group {lun_group_name} found in the table")

        # 3. Check that the SPECIFIC LUN group has an OK/Healthy/Connected status.
        status_path, strategy_ok = SCALE_DASHBOARD_LOCATORS[
            "lun_group_status_ok_by_name"
        ]
        specific_ok_xpath = status_path.format(lun_group_name=lun_group_name)

        assert self.check_element_presence(
            (strategy_ok, specific_ok_xpath), timeout=25
        ), f"LUN group '{lun_group_name}' is not in Healthy/OK/Connected state"
        logger.info(f"LUN group {lun_group_name} health status is OK/Healthy/Connected")
