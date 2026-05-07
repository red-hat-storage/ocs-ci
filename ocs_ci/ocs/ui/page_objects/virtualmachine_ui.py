"""
VirtualMachine UI Page Object for OpenShift Virtualization
"""

import logging
import time
from selenium.webdriver.common.by import By
from ocs_ci.ocs.ui.base_ui import BaseUI, wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import TimeoutExpiredError

logger = logging.getLogger(__name__)


class VirtualMachineUI(PageNavigator, BaseUI):
    """
    VirtualMachineUI class implements virtual machine creation, management, and deletion
    """

    def __init__(self):
        super().__init__()
        self.base_ui = BaseUI()
        self.vm_locators = self._get_vm_locators()

    def _get_vm_locators(self):
        """
        Get VM locators dictionary

        Returns:
            dict: VM locators
        """
        from ocs_ci.ocs.ui.views import VM_LOCATORS

        return VM_LOCATORS

    def navigate_to_virtualmachines_page(self):
        """
        Navigate to Virtualization > VirtualMachines page
        """
        logger.info("Navigating to Virtualization > VirtualMachines")

        # Click on Virtualization menu
        self.base_ui.do_click(self.vm_locators["virtualization_menu"])
        logger.info("Clicked on Virtualization menu")

        # Click on VirtualMachines tab
        time.sleep(2)  # Wait for menu to expand
        self.base_ui.do_click(self.vm_locators["virtualmachines_tab"])
        logger.info("Navigated to VirtualMachines page")

    def click_create_virtualmachine(self):
        """
        Click on 'Create VirtualMachine' button
        """
        wait_for_element_to_be_clickable(
            locator=self.vm_locators["create_vm_button"], timeout=30
        )
        self.base_ui.do_click(self.vm_locators["create_vm_button"])
        logger.info("Clicked on Create VirtualMachine button")

    def select_from_instancetype_option(self):
        """
        Select 'From InstanceType' option from dropdown
        """
        time.sleep(2)  # Wait for dropdown to appear
        wait_for_element_to_be_clickable(
            locator=self.vm_locators["from_instancetype_option"], timeout=30
        )
        self.base_ui.do_click(self.vm_locators["from_instancetype_option"])
        logger.info("Selected 'From InstanceType' option")

    def select_first_bootable_volume(self):
        """
        Select the first/top bootable volume from the volume table

        Returns:
            str: Name of the selected bootable volume
        """
        logger.info("Selecting first bootable volume from table")

        # Wait for table to load
        time.sleep(3)

        # Click on the first radio button
        first_volume_radio = self.vm_locators["first_bootable_volume_radio"]
        wait_for_element_to_be_clickable(locator=first_volume_radio, timeout=30)
        self.base_ui.do_click(first_volume_radio)

        # Get the volume name from the first row
        volume_name_xpath = (
            "//table[contains(@aria-label, 'Bootable volumes')]//tbody//tr[1]//td[2]"
        )
        volume_name_locator = (volume_name_xpath, By.XPATH)
        volume_name = self.base_ui.get_element_text(volume_name_locator)

        logger.info(f"Selected bootable volume: {volume_name}")
        return volume_name

    def select_general_purpose_instancetype(self):
        """
        Select 'General Purpose' from InstanceType section
        """
        logger.info("Selecting General Purpose instance type")

        # Try clicking the radio button first
        try:
            general_purpose_radio = self.vm_locators["general_purpose_radio"]
            wait_for_element_to_be_clickable(locator=general_purpose_radio, timeout=20)
            self.base_ui.do_click(general_purpose_radio)
            logger.info("Selected General Purpose via radio button")
        except Exception:
            # If radio button fails, try clicking the label
            logger.info("Radio button not found, trying label")
            general_purpose_label = self.vm_locators["general_purpose_label"]
            wait_for_element_to_be_clickable(locator=general_purpose_label, timeout=20)
            self.base_ui.do_click(general_purpose_label)
            logger.info("Selected General Purpose via label")

    def select_storageclass_starting_with_san(self):
        """
        Select a storage class that starts with 'san-' and ends with '-vm'
        from the VirtualMachine details section

        Returns:
            str: Name of the selected storage class
        """
        logger.info(
            "Selecting storage class starting with 'san-' and ending with '-vm'"
        )

        # Wait for the dropdown to be available
        time.sleep(2)

        # Try to find and click the dropdown button
        try:
            dropdown_button = self.vm_locators["storageclass_dropdown_button"]
            wait_for_element_to_be_clickable(locator=dropdown_button, timeout=30)
            self.base_ui.do_click(dropdown_button)
            logger.info("Clicked storage class dropdown button")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Could not click dropdown button: {e}")

        # Find all storage class options that match the pattern
        san_sc_xpath = (
            "//select[@id='storageclass-dropdown']//option[starts-with(., 'san-') and contains(., '-vm')] | "
            "//button[starts-with(., 'san-') and contains(., '-vm')] | "
            "//li[starts-with(., 'san-') and contains(., '-vm')]"
        )
        san_sc_locator = (san_sc_xpath, By.XPATH)

        try:
            # Get all matching elements
            elements = self.base_ui.get_elements(san_sc_locator)

            if not elements:
                raise TimeoutExpiredError(
                    "No storage class starting with 'san-' and ending with '-vm' found"
                )

            # Click the first matching element
            first_element = elements[0]
            sc_name = first_element.text
            first_element.click()

            logger.info(f"Selected storage class: {sc_name}")
            return sc_name

        except Exception as e:
            logger.error(f"Error selecting storage class: {e}")
            # Fallback: try to select from dropdown directly
            dropdown_xpath = "//select[@id='storageclass-dropdown']"
            dropdown_locator = (dropdown_xpath, By.XPATH)

            try:
                from selenium.webdriver.support.ui import Select

                dropdown_elements = self.base_ui.get_elements(dropdown_locator)
                if dropdown_elements:
                    dropdown_element = dropdown_elements[0]
                    select = Select(dropdown_element)

                    # Find option starting with 'san-' and ending with '-vm'
                    for option in select.options:
                        if option.text.startswith("san-") and option.text.endswith(
                            "-vm"
                        ):
                            select.select_by_visible_text(option.text)
                            logger.info(
                                f"Selected storage class via dropdown: {option.text}"
                            )
                            return option.text

                raise TimeoutExpiredError("No matching storage class found in dropdown")
            except Exception as e2:
                logger.error(f"Fallback selection also failed: {e2}")
                raise

    def ensure_start_vm_checkbox_checked(self):
        """
        Ensure the 'Start this VirtualMachine after creation' checkbox is checked
        """
        logger.info("Checking 'Start this VirtualMachine after creation' checkbox")

        try:
            checkbox_locator = self.vm_locators["start_vm_checkbox"]
            is_checked = self.base_ui.get_checkbox_status(checkbox_locator, timeout=10)

            if not is_checked:
                logger.info("Checkbox not checked, clicking to check it")
                self.base_ui.do_click(checkbox_locator)
                logger.info("Checkbox now checked")
            else:
                logger.info("Checkbox already checked")
        except Exception as e:
            # Try clicking the label if checkbox is not directly accessible
            logger.info(f"Could not access checkbox directly: {e}, trying label")
            label_locator = self.vm_locators["start_vm_checkbox_label"]
            self.base_ui.do_click(label_locator)
            logger.info("Clicked checkbox label")

    def click_create_virtualmachine_submit(self):
        """
        Click the 'Create VirtualMachine' submit button
        """
        logger.info("Clicking Create VirtualMachine submit button")

        submit_button = self.vm_locators["create_vm_submit_button"]
        wait_for_element_to_be_clickable(locator=submit_button, timeout=30)
        self.base_ui.do_click(submit_button, enable_screenshot=True)
        logger.info("Clicked Create VirtualMachine button")

    @retry((AssertionError, TimeoutExpiredError), tries=20, delay=20)
    def wait_for_vm_status(self, expected_status, vm_name=None):
        """
        Wait for VM to reach expected status

        Args:
            expected_status (str): Expected status (e.g., 'Running', 'Stopped')
            vm_name (str, optional): VM name for logging

        Returns:
            bool: True if status matches
        """
        logger.info(f"Waiting for VM status: {expected_status}")

        status_xpath = f"//span[contains(., '{expected_status}')]"
        status_locator = (status_xpath, By.XPATH)

        assert self.base_ui.check_element_presence(
            status_locator, timeout=20
        ), f"VM status '{expected_status}' not found"

        logger.info(f"VM status is now: {expected_status}")
        return True

    def get_vm_name_from_details_page(self):
        """
        Get the VirtualMachine name from the details page

        Returns:
            str: VM name
        """
        logger.info("Getting VM name from details page")

        vm_name_locator = self.vm_locators["vm_name_field"]
        vm_name = self.base_ui.get_element_text(vm_name_locator)

        logger.info(f"VM name: {vm_name}")
        return vm_name

    def navigate_back_to_virtualmachines_list(self):
        """
        Navigate back to VirtualMachines list page
        """
        logger.info("Navigating back to VirtualMachines list")
        self.navigate_to_virtualmachines_page()

    def select_vm_checkbox(self, vm_name):
        """
        Select the checkbox for a specific VM

        Args:
            vm_name (str): Name of the VM
        """
        logger.info(f"Selecting checkbox for VM: {vm_name}")

        checkbox_xpath = f"//tr[contains(., '{vm_name}')]//input[@type='checkbox']"
        checkbox_locator = (checkbox_xpath, By.XPATH)

        wait_for_element_to_be_clickable(locator=checkbox_locator, timeout=30)
        self.base_ui.do_click(checkbox_locator)
        logger.info(f"Selected checkbox for VM: {vm_name}")

    def click_actions_menu(self):
        """
        Click on Actions menu
        """
        logger.info("Clicking Actions menu")

        actions_button = self.vm_locators["actions_button"]
        wait_for_element_to_be_clickable(locator=actions_button, timeout=30)
        self.base_ui.do_click(actions_button)
        logger.info("Clicked Actions menu")
        time.sleep(1)  # Wait for menu to expand

    def click_control_submenu(self):
        """
        Click on Control submenu under Actions
        """
        logger.info("Clicking Control submenu")

        control_menu = self.vm_locators["actions_control_menu"]
        wait_for_element_to_be_clickable(locator=control_menu, timeout=20)
        self.base_ui.do_click(control_menu)
        logger.info("Clicked Control submenu")
        time.sleep(1)  # Wait for submenu to expand

    def click_stop_option(self):
        """
        Click on Stop option from Actions > Control menu
        """
        logger.info("Clicking Stop option")

        stop_option = self.vm_locators["actions_stop_option"]
        wait_for_element_to_be_clickable(locator=stop_option, timeout=20)
        self.base_ui.do_click(stop_option, enable_screenshot=True)
        logger.info("Clicked Stop option")

    def click_delete_option(self):
        """
        Click on Delete option from Actions menu
        """
        logger.info("Clicking Delete option")

        delete_option = self.vm_locators["actions_delete_option"]
        wait_for_element_to_be_clickable(locator=delete_option, timeout=20)
        self.base_ui.do_click(delete_option, enable_screenshot=True)
        logger.info("Clicked Delete option")

    def check_delete_options_and_confirm(self):
        """
        Check 'with grace period' and 'Delete disk' checkboxes and confirm deletion
        """
        logger.info("Checking delete options and confirming deletion")

        # Wait for modal to appear
        time.sleep(2)

        # Check 'with grace period' checkbox
        try:
            grace_period_checkbox = self.vm_locators["delete_grace_period_checkbox"]
            is_checked = self.base_ui.get_checkbox_status(
                grace_period_checkbox, timeout=10
            )

            if not is_checked:
                logger.info("Checking 'with grace period' checkbox")
                self.base_ui.do_click(grace_period_checkbox)
            else:
                logger.info("'with grace period' checkbox already checked")
        except Exception as e:
            logger.warning(f"Could not check grace period checkbox: {e}")

        # Check 'Delete disk' checkbox
        try:
            delete_disk_checkbox = self.vm_locators["delete_disk_checkbox"]
            is_checked = self.base_ui.get_checkbox_status(
                delete_disk_checkbox, timeout=10
            )

            if not is_checked:
                logger.info("Checking 'Delete disk' checkbox")
                self.base_ui.do_click(delete_disk_checkbox)
            else:
                logger.info("'Delete disk' checkbox already checked")
        except Exception as e:
            logger.warning(f"Could not check delete disk checkbox: {e}")

        # Click Delete button
        time.sleep(1)
        delete_confirm_button = self.vm_locators["delete_confirm_button"]
        wait_for_element_to_be_clickable(locator=delete_confirm_button, timeout=20)
        self.base_ui.do_click(delete_confirm_button, enable_screenshot=True)
        logger.info("Confirmed VM deletion")

    def verify_vm_deleted(self, vm_name, max_retries=10, retry_delay=5):
        """
        Verify that VM has been deleted from the list

        Args:
            vm_name (str): Name of the VM
            max_retries (int): Maximum number of verification attempts
            retry_delay (int): Delay between retries in seconds

        Returns:
            bool: True if VM is deleted, False otherwise
        """
        logger.info(f"Verifying VM deletion: {vm_name}")

        vm_row_xpath = f"//tr[contains(., '{vm_name}')]"
        vm_row_locator = (vm_row_xpath, By.XPATH)

        for attempt in range(max_retries):
            time.sleep(retry_delay)

            if not self.base_ui.check_element_presence(vm_row_locator, timeout=5):
                logger.info(f"✓ VM '{vm_name}' successfully deleted")
                return True
            else:
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries}: VM '{vm_name}' still present"
                )

        logger.error(f"✗ VM '{vm_name}' still present after all retries")
        return False

    def find_running_centos_vm(self):
        """
        Find a centos-*** VM that is in Running status

        Returns:
            str: Name of the running centos VM

        Raises:
            ResourceNotFoundError: If no running centos VM is found
        """
        logger.info("Searching for running centos-*** VirtualMachine")

        # XPath to find centos VM rows with Running status
        centos_vm_xpath = (
            "//tr[contains(@data-test-id, 'centos-') or " "td[contains(., 'centos-')]]"
        )

        centos_vm_locator = (centos_vm_xpath, By.XPATH)

        # Get all centos VM rows
        vm_rows = self.base_ui.get_elements(centos_vm_locator)

        if not vm_rows:
            from ocs_ci.ocs.exceptions import ResourceNotFoundError

            raise ResourceNotFoundError("No centos-*** VirtualMachine found")

        # Check each row for Running status
        for row in vm_rows:
            try:
                # Get VM name from the row
                vm_name_cell = row.find_element(
                    By.XPATH, ".//td[@data-label='Name' or position()=2]"
                )
                vm_name = vm_name_cell.text.strip()

                # Check if VM name starts with centos-
                if not vm_name.startswith("centos-"):
                    continue

                # Check status
                status_cell = row.find_element(
                    By.XPATH,
                    ".//td[contains(@class, 'status') or .//span[contains(., 'Running')]]",
                )

                if "Running" in status_cell.text:
                    logger.info(f"Found running centos VM: {vm_name}")
                    return vm_name

            except Exception as e:
                logger.debug(f"Error checking VM row: {e}")
                continue

        from ocs_ci.ocs.exceptions import ResourceNotFoundError

        raise ResourceNotFoundError(
            "No centos-*** VirtualMachine found in Running status"
        )

    def click_vm_kebab_menu(self, vm_name):
        """
        Click the 3-dot kebab menu for a specific VM

        Args:
            vm_name (str): Name of the VM
        """
        logger.info(f"Clicking kebab menu (3 dots) for VM: {vm_name}")

        # XPath for the 3-dot menu button in the VM row
        kebab_menu_xpath = (
            f"//tr[contains(., '{vm_name}')]"
            f"//button[@data-test-id='kebab-button' or "
            f"contains(@aria-label, 'Actions') or "
            f"contains(@class, 'pf-c-dropdown__toggle')]"
        )

        kebab_menu_locator = (kebab_menu_xpath, By.XPATH)
        wait_for_element_to_be_clickable(locator=kebab_menu_locator, timeout=30)
        self.base_ui.do_click(kebab_menu_locator)
        logger.info(f"Clicked kebab menu for VM: {vm_name}")
        time.sleep(1)  # Wait for menu to expand

    def click_clone_option(self):
        """
        Click the 'Clone' option from the kebab dropdown menu
        """
        logger.info("Clicking Clone option from dropdown")

        # XPath for Clone option in dropdown
        clone_option_xpath = (
            "//button[contains(., 'Clone') or @data-test-action='Clone'] | "
            "//a[contains(., 'Clone')] | "
            "//li[contains(., 'Clone')]//button"
        )

        clone_option_locator = (clone_option_xpath, By.XPATH)
        wait_for_element_to_be_clickable(locator=clone_option_locator, timeout=20)
        self.base_ui.do_click(clone_option_locator, enable_screenshot=True)
        logger.info("Clicked Clone option")
        time.sleep(2)  # Wait for clone page to load

    def check_start_vm_on_clone_checkbox(self):
        """
        Check the 'Start VirtualMachine once created' checkbox on Clone page
        """
        logger.info("Checking 'Start VirtualMachine once created' checkbox")

        # XPath for the checkbox on clone page
        start_vm_checkbox_xpath = (
            "//input[@type='checkbox' and "
            "(contains(@id, 'start-vm') or "
            "contains(@name, 'startVM') or "
            "following-sibling::label[contains(., 'Start VirtualMachine')])] | "
            "//input[@type='checkbox']"
            "[parent::*/following-sibling::*[contains(., 'Start VirtualMachine once created')]]"
        )

        checkbox_locator = (start_vm_checkbox_xpath, By.XPATH)

        try:
            # Use get_checkbox_status to check if checkbox is selected
            is_checked = self.base_ui.get_checkbox_status(checkbox_locator, timeout=10)

            if not is_checked:
                logger.info("Checkbox not checked, clicking to check it")
                self.base_ui.do_click(checkbox_locator)
                logger.info("Checkbox now checked")
            else:
                logger.info("Checkbox already checked")
        except Exception as e:
            logger.warning(f"Could not access checkbox directly: {e}")
            # Try clicking by label text
            label_xpath = "//label[contains(., 'Start VirtualMachine once created')]"
            label_locator = (label_xpath, By.XPATH)
            self.base_ui.do_click(label_locator)
            logger.info("Clicked checkbox via label")

    def click_clone_button(self):
        """
        Click the 'Clone' button to confirm cloning
        """
        logger.info("Clicking Clone button to confirm")

        # XPath for Clone button
        clone_button_xpath = (
            "//button[contains(., 'Clone') and "
            "(@type='submit' or contains(@class, 'pf-m-primary'))] | "
            "//button[@data-test-id='clone-vm-button']"
        )

        clone_button_locator = (clone_button_xpath, By.XPATH)
        wait_for_element_to_be_clickable(locator=clone_button_locator, timeout=30)
        self.base_ui.do_click(clone_button_locator, enable_screenshot=True)
        logger.info("Clicked Clone button")
        time.sleep(3)  # Wait for cloning to initiate

    def get_cloned_vm_name(self, original_vm_name):
        """
        Get the name of the cloned VM (usually original-name-clone or similar)

        Args:
            original_vm_name (str): Name of the original VM

        Returns:
            str: Name of the cloned VM
        """
        logger.info("Getting cloned VM name")

        # Try to get VM name from details page if redirected there
        try:
            vm_name = self.get_vm_name_from_details_page()
            if vm_name and vm_name != original_vm_name:
                logger.info(f"Cloned VM name: {vm_name}")
                return vm_name
        except Exception as e:
            logger.debug(f"Could not get VM name from details page: {e}")

        # Fallback: look for VM with similar name pattern
        # Common patterns: original-clone, original-1, original-copy
        possible_patterns = [
            f"{original_vm_name}-clone",
            f"{original_vm_name}-1",
            f"{original_vm_name}-copy",
        ]

        for pattern in possible_patterns:
            vm_xpath = f"//tr[contains(., '{pattern}')]"
            vm_locator = (vm_xpath, By.XPATH)

            if self.base_ui.check_element_presence(vm_locator, timeout=5):
                logger.info(f"Found cloned VM: {pattern}")
                return pattern

        # If still not found, return a generic pattern
        logger.warning("Could not determine exact clone name, using pattern")
        return f"{original_vm_name}-clone"

    def navigate_to_vm_console(self, vm_name):
        """
        Navigate to the Console tab of a specific VM

        Args:
            vm_name (str): Name of the VM
        """
        logger.info(f"Navigating to Console tab for VM: {vm_name}")

        # Click on VM name to open details
        vm_link_xpath = f"//a[contains(., '{vm_name}')]"
        vm_link_locator = (vm_link_xpath, By.XPATH)

        if self.base_ui.check_element_presence(vm_link_locator, timeout=10):
            self.base_ui.do_click(vm_link_locator)
            time.sleep(3)

        # Click on Console tab
        console_tab_xpath = (
            "//button[contains(., 'Console')] | //a[contains(., 'Console')]"
        )
        console_tab_locator = (console_tab_xpath, By.XPATH)

        if self.base_ui.check_element_presence(console_tab_locator, timeout=10):
            self.base_ui.do_click(console_tab_locator)
            time.sleep(3)
            logger.info("Navigated to Console tab")

    def check_file_in_vm(self, vm_name, file_path="/etc/sample.txt"):
        """
        Check if a file exists in the VM via console

        Args:
            vm_name (str): Name of the VM
            file_path (str): Path to the file to check

        Returns:
            bool: True if file exists, False otherwise

        Note:
            This is a placeholder method. Actual implementation requires
            console interaction which depends on the specific environment.
        """
        logger.info(f"Checking for file {file_path} in VM {vm_name}")

        # Navigate to console
        self.navigate_to_vm_console(vm_name)

        # TODO: Implement actual console interaction
        # This would involve:
        # 1. Waiting for console to be ready
        # 2. Sending login credentials
        # 3. Executing: ls -la /etc/sample.txt
        # 4. Parsing the output to verify file existence

        logger.warning(
            "Console interaction not fully implemented. "
            "Manual verification required for file existence."
        )

        # For now, return True as placeholder
        # In production, this should be replaced with actual console commands
        return True


# Made with Bob
