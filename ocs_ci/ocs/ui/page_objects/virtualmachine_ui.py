"""
VirtualMachine UI Page Object for OpenShift Virtualization
"""

import logging
import time
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ui.base_ui import (
    wait_for_element_to_be_clickable,
    wait_for_element_to_be_visible,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


class VirtualMachineUI(PageNavigator):
    """
    VirtualMachineUI implements virtual machine creation, management, and deletion.
    """

    def __init__(self):
        super().__init__()

    def navigate_to_workloads_pods(self):
        """
        Navigate to Workloads > Pods in the left-side navigation menu.
        """
        logger.info("Navigating to Workloads > Pods")
        self.choose_expanded_mode(mode=True, locator=self.vm_loc["workloads_menu"])
        self.do_click(self.vm_loc["workloads_pods_option"])
        logger.info("Navigated to Workloads > Pods")

    def select_project_from_all_projects(self, namespace):
        """
        Click the 'All Projects' dropdown, enable 'Show default projects',
        search for the namespace, and select it.

        Args:
            namespace (str): The namespace/project name to select
        """
        logger.info(f"Opening 'All Projects' dropdown for namespace: {namespace}")
        self.do_click(self.vm_loc["project_selector_dropdown"])

        try:
            toggle_locator = self.vm_loc["project_show_default_toggle"]
            wait_for_element_to_be_clickable(locator=toggle_locator, timeout=10)
            if not self.get_checkbox_status(locator=toggle_locator, timeout=10):
                self.do_click(toggle_locator)
                logger.info("Enabled 'Show default projects' toggle")
            else:
                logger.info(
                    "'Show default projects' toggle is already enabled, skipping click"
                )
        except (NoSuchElementException, WebDriverException, TimeoutException):
            pass

        try:
            search_input = self.vm_loc["project_search_input"]
            wait_for_element_to_be_clickable(locator=search_input, timeout=15)
            self.do_send_keys(search_input, namespace)
        except (NoSuchElementException, WebDriverException) as e:
            logger.warning(f"Could not type in project search field: {e}")

        ns_option_xpath = self.vm_loc["project_namespace_item_tmpl"][0].format(
            namespace=namespace
        )
        ns_option_locator = (
            ns_option_xpath,
            self.vm_loc["project_namespace_item_tmpl"][1],
        )
        wait_for_element_to_be_clickable(locator=ns_option_locator, timeout=20)
        self.do_click(ns_option_locator)
        logger.info(f"Selected project/namespace: {namespace}")

    def navigate_to_virtualmachines_page(self):
        """
        Navigate to Virtualization > VirtualMachines page and dismiss the
        welcome modal if it appears.
        """
        logger.info("Navigating to Virtualization > VirtualMachines")
        # Wait for the page to fully settle (e.g. after a namespace switch) before
        self.page_has_loaded()
        self.choose_expanded_mode(mode=True, locator=self.vm_loc["virtualization_menu"])
        self.do_click(self.vm_loc["virtualmachines_tab"])
        logger.info("Navigated to VirtualMachines page")
        self.dismiss_welcome_modal()

    def dismiss_welcome_modal(self):
        """
        Close the 'Welcome to OpenShift Virtualization' modal if present.
        Uses driver.find_elements to avoid AI fallback when no modal is shown.
        """
        locator = self.vm_loc["modal_close_button"]
        try:
            elements = self.driver.find_elements(locator[1], locator[0])
            if elements and elements[0].is_displayed():
                elements[0].click()
                logger.info("Dismissed welcome modal")
                return
        except (NoSuchElementException, WebDriverException):
            pass
        logger.info("No welcome modal present")

    def dismiss_welcome_modal_if_present(self, wait_for_modal=False, timeout=15):
        """
        Dismiss any overlay modal currently blocking the page.
        Uses driver.find_elements to avoid AI fallback when no modal is shown.

        Args:
            wait_for_modal (bool): If True, poll until the modal appears or timeout
                                   expires before attempting to close it.
            timeout (int): Seconds to wait for the modal when wait_for_modal=True.
        """
        import time as _time

        locator = self.vm_loc["modal_close_button"]
        if wait_for_modal:
            end = _time.time() + timeout
            while _time.time() < end:
                try:
                    els = self.driver.find_elements(locator[1], locator[0])
                    if els and els[0].is_displayed():
                        els[0].click()
                        logger.info("Dismissed modal")
                        return
                except (NoSuchElementException, WebDriverException):
                    pass
                _time.sleep(1)
            logger.info("No modal appeared within timeout")
            return
        try:
            elements = self.driver.find_elements(locator[1], locator[0])
            if elements and elements[0].is_displayed():
                elements[0].click()
                logger.info("Dismissed modal")
                return
        except (NoSuchElementException, WebDriverException):
            pass
        logger.info("No modal to dismiss")

    def enter_vm_name(self, vm_name):
        """
        Enter the VM name in the creation wizard.

        Args:
            vm_name (str): Name to give the VirtualMachine
        """
        logger.info(f"Entering VM name: {vm_name}")
        name_input = self.vm_loc["vm_name_input"]
        wait_for_element_to_be_clickable(locator=name_input, timeout=30)
        self.do_clear(name_input)
        self.do_send_keys(name_input, vm_name)
        logger.info(f"Entered VM name: {vm_name}")

    def click_create_virtualmachine(self):
        """
        Click on 'Create VirtualMachine' button (top-right).
        Falls back to JS click if an overlay intercepts.
        """
        locator = self.vm_loc["create_vm_button"]
        wait_for_element_to_be_clickable(locator=locator, timeout=30)
        try:
            self.do_click(locator)
        except WebDriverException:
            element = self.get_element(locator)
            self.driver.execute_script("arguments[0].click();", element)
            logger.info("Clicked Create VirtualMachine button via JS fallback")
            return
        logger.info("Clicked Create VirtualMachine button")

    def click_next_button(self):
        """
        Click the 'Next' button on the current wizard page.
        """
        wait_for_element_to_be_clickable(
            locator=self.vm_loc["creation_wizard_next"], timeout=30
        )
        self.do_click(self.vm_loc["creation_wizard_next"])
        logger.info("Clicked Next button")

    def select_guest_os_other_linux(self):
        """
        On the Guest OS page select the 'Other Linux' card (3rd card).
        """
        other_linux = self.vm_loc["guest_os_other_linux"]
        wait_for_element_to_be_clickable(locator=other_linux, timeout=30)
        element = self.driver.find_element(other_linux[1], other_linux[0])
        self.driver.execute_script("arguments[0].click();", element)
        logger.info("Selected 'Other Linux' card")

    def select_guest_os(self):
        """
        Open the 'Guest operating system type' dropdown, collect all
        centos.stream* options and select the one with the highest version number.

        Returns:
            str: Text of the selected option (e.g. 'centos.stream11')
        """
        dropdown = self.vm_loc["guest_os_type_dropdown"]
        wait_for_element_to_be_clickable(locator=dropdown, timeout=30)
        self.do_click(dropdown)

        options_locator = self.vm_loc["guest_os_type_centos_stream_options"]
        wait_for_element_to_be_clickable(locator=options_locator, timeout=20)
        elements = self.get_elements(options_locator)
        if not elements:
            raise RuntimeError(
                "No centos.stream* options found in Guest OS type dropdown"
            )

        def _version(el):
            text = el.text.strip()
            # text is e.g. 'centos.stream10'; extract the trailing integer
            suffix = text.replace("centos.stream", "")
            return int(suffix) if suffix.isdigit() else 0

        latest = max(elements, key=_version)
        selected_text = latest.text.strip()
        latest.click()
        logger.info(f"Selected Guest OS type: {selected_text} (latest centos.stream)")
        return selected_text

    def select_compute_size_small(self):
        """
        On the Compute resources page open the size dropdown and select
        'small: 1 CPUs, 2 GiB Memory'.
        """
        toggle_locator = self.vm_loc["compute_size_dropdown"]
        wait_for_element_to_be_clickable(locator=toggle_locator, timeout=20)
        self.do_click(toggle_locator)

        small_locator = self.vm_loc["compute_size_small_option"]
        wait_for_element_to_be_clickable(locator=small_locator, timeout=20)
        self.do_click(small_locator)
        logger.info("Selected compute size: small: 1 CPUs, 2 GiB Memory")

    def select_boot_volume_centos_stream_latest(self):
        """
        On the Boot source page click on the centos-stream volume row with the
        highest version number (e.g. centos-stream11 is preferred over centos-stream10).
        """
        options_locator = self.vm_loc["boot_volume_centos_stream_options"]
        wait_for_element_to_be_clickable(locator=options_locator, timeout=30)
        elements = self.get_elements(options_locator)
        if not elements:
            raise RuntimeError(
                "No centos-stream* boot volume rows found on Boot source page"
            )

        def _version(el):
            text = el.text.strip()
            # text is e.g. 'centos-stream10'; extract the trailing integer
            suffix = text.replace("centos-stream", "")
            return int(suffix) if suffix.isdigit() else 0

        latest = max(elements, key=_version)
        selected_text = latest.text.strip()
        latest.click()
        logger.info(f"Clicked boot volume: {selected_text} (latest centos-stream)")

    def click_customization_storage_tab(self):
        """
        On the Customization page click the 'Storage' tab.
        """
        storage_tab = self.vm_loc["customization_storage_tab"]
        wait_for_element_to_be_clickable(locator=storage_tab, timeout=30)
        self.do_click(storage_tab)
        logger.info("Clicked Storage tab")

    def click_rootdisk_kebab_and_edit(self):
        """
        Click the kebab menu on the rootdisk row then select 'Edit'.
        """
        kebab = self.vm_loc["rootdisk_kebab_button"]
        wait_for_element_to_be_clickable(locator=kebab, timeout=30)
        element = self.driver.find_element(kebab[1], kebab[0])
        self.driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", element
        )
        self.driver.execute_script("arguments[0].click();", element)
        edit_opt = self.vm_loc["rootdisk_kebab_edit"]
        wait_for_element_to_be_clickable(locator=edit_opt, timeout=20)
        self.do_click(edit_opt)
        logger.info("Clicked Edit on rootdisk")

    def change_storageclass_to_vm_option(self):
        """
        In the 'Edit disk' popup open the StorageClass dropdown and select
        the option ending with '-vm'.

        Returns:
            str: Name of the selected storage class
        """
        sc_dropdown = self.vm_loc["edit_disk_storageclass_dropdown"]
        wait_for_element_to_be_clickable(locator=sc_dropdown, timeout=30)
        element = self.driver.find_element(sc_dropdown[1], sc_dropdown[0])
        self.driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", element
        )
        self.driver.execute_script("arguments[0].click();", element)

        vm_opt = self.vm_loc["edit_disk_storageclass_vm_option"]
        wait_for_element_to_be_clickable(locator=vm_opt, timeout=20)
        opt_element = self.driver.find_element(vm_opt[1], vm_opt[0])
        name_span = opt_element.find_element(
            self.vm_loc["sc_resource_name_span"][1],
            self.vm_loc["sc_resource_name_span"][0],
        )
        sc_name = name_span.text.strip()
        self.do_click(vm_opt)
        logger.info(f"Selected storage class: {sc_name}")
        return sc_name

    def click_edit_disk_save(self):
        """
        Click the 'Save' button inside the 'Edit disk' popup.
        """
        save_btn = self.vm_loc["edit_disk_save_button"]
        wait_for_element_to_be_clickable(locator=save_btn, timeout=20)
        self.do_click(save_btn)
        logger.info("Clicked Save")

    def click_create_virtualmachine_submit(self):
        """
        Click the 'Create VirtualMachine' button on the Review and create page.
        """
        submit_button = self.vm_loc["create_vm_submit_button"]
        wait_for_element_to_be_clickable(locator=submit_button, timeout=30)
        self.do_click(submit_button, enable_screenshot=True)
        logger.info("Clicked Create VirtualMachine submit button")

    @retry(
        (AssertionError, TimeoutExpiredError, TimeoutException),
        tries=30,
        delay=30,
        backoff=1,
    )
    def wait_for_vm_running(self):
        """
        Wait up to 15 minutes for the Status field to show 'Running'.
        Each attempt probes for up to 5 s; the retry loop provides the
        remaining wait budget (30 tries × 30 s delay = 15 minutes).
        """
        logger.info("Checking for Running status on VM detail page...")
        wait_for_element_to_be_visible(
            locator=self.vm_loc["vm_status_running"], timeout=5
        )
        logger.info("VM status is now: Running")
        return True

    @retry(
        (AssertionError, TimeoutExpiredError, TimeoutException),
        tries=20,
        delay=10,
        backoff=1,
    )
    def wait_for_vm_stopped(self):
        """
        Wait for the Status field to show 'Stopped'.
        Each attempt probes for up to 5 s; the retry loop provides the
        remaining wait budget (20 tries × 10 s delay = ~3.5 minutes).
        """
        logger.info("Checking for Stopped status on VM detail page...")
        wait_for_element_to_be_visible(
            locator=self.vm_loc["vm_status_stopped"], timeout=5
        )
        logger.info("VM status is now: Stopped")
        return True

    def click_actions_menu(self):
        """
        Click on Actions menu on the VM detail page.
        """
        actions_button = self.vm_loc["actions_button"]
        wait_for_element_to_be_clickable(locator=actions_button, timeout=30)
        self.do_click(actions_button)
        logger.info("Clicked Actions menu")

    def click_actions_control_then_stop(self):
        """
        From the Actions menu click Control (submenu) then Stop.
        """
        logger.info("Clicking Actions > Control")
        control_menu = self.vm_loc["actions_control_menu"]
        wait_for_element_to_be_clickable(locator=control_menu, timeout=20)
        self.do_click(control_menu)
        logger.info("Clicking Stop")
        stop_option = self.vm_loc["actions_stop_option"]
        wait_for_element_to_be_clickable(locator=stop_option, timeout=20)
        self.do_click(stop_option, enable_screenshot=True)
        logger.info("Clicked Stop")

    def click_actions_delete(self):
        """
        From the Actions menu click Delete.
        Waits for the modal to load before returning.
        """
        logger.info("Clicking Actions > Delete")
        delete_option = self.vm_loc["actions_delete_option"]
        wait_for_element_to_be_clickable(locator=delete_option, timeout=20)
        self.do_click(delete_option, enable_screenshot=True)
        logger.info("Clicked Delete; waiting for modal to load")
        wait_for_element_to_be_visible(
            locator=self.vm_loc["delete_confirm_button"], timeout=30
        )

    def check_grace_period_and_confirm_delete(self):
        """
        In the 'Delete VirtualMachine' modal:
          - Check the 'With grace period' checkbox (unchecked by default)
          - Click the Delete button
        """
        logger.info("Checking 'With grace period' checkbox in delete modal")
        grace_locator = self.vm_loc["delete_grace_period_checkbox"]
        els = self.driver.find_elements(grace_locator[1], grace_locator[0])
        assert (
            els and els[0].is_displayed()
        ), "Could not find 'With grace period' checkbox"
        if not els[0].is_selected():
            els[0].click()
            logger.info("Checked 'With grace period'")

        delete_btn = self.vm_loc["delete_confirm_button"]
        wait_for_element_to_be_clickable(locator=delete_btn, timeout=20)
        self.do_click(delete_btn, enable_screenshot=True)
        logger.info("Clicked Delete in confirmation modal")

    def verify_namespace_gone_from_left_tree(self, namespace, timeout=30):
        """
        Verify the namespace row has disappeared from the left-side tree.
        Uses driver.find_elements to avoid AI fallback.

        Args:
            namespace (str): Namespace name to check
            timeout (int): How many seconds to poll

        Returns:
            bool: True if namespace is gone
        """
        logger.info(f"Verifying namespace '{namespace}' is gone from left-side tree")
        ns_xpath = self.vm_loc["namespace_left_tree_item_tmpl"][0].format(
            namespace=namespace
        )
        ns_by = self.vm_loc["namespace_left_tree_item_tmpl"][1]
        end = time.time() + timeout
        while time.time() < end:
            try:
                els = self.driver.find_elements(ns_by, ns_xpath)
                if not els or not any(e.is_displayed() for e in els):
                    logger.info(
                        f"Namespace '{namespace}' no longer visible in left tree"
                    )
                    return True
            except (NoSuchElementException, WebDriverException):
                return True
        logger.warning(f"Namespace '{namespace}' still visible after {timeout}s")
        return False
