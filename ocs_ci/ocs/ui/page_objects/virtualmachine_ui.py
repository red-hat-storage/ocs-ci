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
from ocs_ci.ocs.ui.base_ui import wait_for_element_to_be_visible
from ocs_ci.ocs.ui.helpers_ui import format_locator
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
            self.do_send_keys(self.vm_loc["project_search_input"], namespace)
        except (NoSuchElementException, WebDriverException) as e:
            logger.warning(f"Could not type in project search field: {e}")

        self.do_click(
            format_locator(
                self.vm_loc["project_namespace_item_tmpl"], namespace=namespace
            )
        )
        logger.info(f"Selected project/namespace: {namespace}")

    def navigate_to_virtualmachines_page(self):
        """
        Navigate to Virtualization > VirtualMachines page and wait for the
        page to fully load.

        """
        logger.info("Navigating to Virtualization > VirtualMachines")
        self.choose_expanded_mode(mode=True, locator=self.vm_loc["virtualization_menu"])
        self.do_click(self.vm_loc["virtualmachines_tab"])
        logger.info("Navigated to VirtualMachines page — waiting for page to load")
        self.page_has_loaded()

    def click_create_virtualmachine(self):
        """
        Click the **Create** split-button on the VirtualMachines list page and
        select "From InstanceType" from the dropdown that opens.

        """
        self.do_click(self.vm_loc["create_vm_button"])
        logger.info("Clicked Create button — selecting 'From InstanceType'")
        self.do_click(self.vm_loc["create_vm_from_instancetype_option"])
        logger.info("Selected 'From InstanceType'")

    def select_boot_volume_first_row(self):
        """
        In the "Select volume to boot from" step, click the first available
        boot volume row.

        """
        rows = self.get_elements(self.vm_loc["boot_volume_first_row"])
        assert (
            rows
        ), "No boot volume rows found in the 'Select volume to boot from' table"
        self.scroll_into_view(self.vm_loc["boot_volume_first_row"])
        rows[0].click()
        logger.info("Selected first available boot volume row")

    def select_instance_type_general_purpose(self):
        """
        In the "Select InstanceType" step, scroll the General Purpose card
        into view and click it.

        """
        self.scroll_into_view(self.vm_loc["instance_type_general_purpose_card"])
        self.do_click(self.vm_loc["instance_type_general_purpose_card"])
        logger.info("Selected General Purpose instance type card")

    def enter_instancetype_vm_name(self, vm_name):
        """
        In the "VirtualMachine details" section of the instance-type wizard,
        clear the auto-generated name and enter *vm_name*.

        Args:
            vm_name (str): The unique name to set for the VirtualMachine.
                Must be ≤ 63 characters.
        """
        locator = self.vm_loc["instancetype_vm_name_input"]
        self.clear_input_gradually(locator)
        self.do_send_keys(locator, vm_name)
        logger.info(f"Entered VM name '{vm_name}' in VirtualMachine details")

    def select_storageclass_ending_with_vm(self):
        """
        In the "VirtualMachine details" step, open the Storage class dropdown
        and pick the option whose name ends with ``-vm``.

        """
        self.do_click(self.vm_loc["vm_details_storageclass_dropdown"])
        logger.info("Opened Storage class dropdown in VirtualMachine details")
        self.do_click(self.vm_loc["vm_details_storageclass_vm_option"])
        logger.info("Selected storage class ending with '-vm'")

    def click_create_virtualmachine_submit(self):
        """
        Click the **Create VirtualMachine** button at the bottom of the
        instance-type wizard page.

        """
        self.scroll_into_view(self.vm_loc["create_vm_instancetype_submit_button"])
        self.do_click(
            self.vm_loc["create_vm_instancetype_submit_button"], enable_screenshot=True
        )
        logger.info("Clicked 'Create VirtualMachine' submit button")

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

    def ensure_cloned_vm_running(self):
        """
        After clone submission the UI lands on the cloned VM detail page.
        Check whether the VM reaches Running within 4 minutes (240 s).
        If it is Stopped instead, start it via Actions > Control > Start
        and wait for Running.
        """
        logger.info("Checking cloned VM status (up to 4 min for Running)...")
        end = time.time() + 240
        while time.time() < end:
            running_els = self.get_elements(self.vm_loc["vm_status_running"])
            running = bool(running_els) and running_els[0].is_displayed()
            stopped_els = self.get_elements(self.vm_loc["vm_status_stopped"])
            stopped = bool(stopped_els) and stopped_els[0].is_displayed()

            if running:
                logger.info("Cloned VM is already Running")
                return

            if stopped:
                logger.info(
                    "Cloned VM is Stopped — starting via Actions > Control > Start"
                )
                self.click_actions_menu()
                self.click_actions_control_then_start()
                logger.info("Start issued — waiting for Running status...")
                self.wait_for_vm_running()
                return

            time.sleep(3)

        logger.info("Status not yet visible after 4 min — waiting for Running...")
        self.wait_for_vm_running()

    def click_actions_menu(self):
        """
        Click on Actions menu on the VM detail page.
        """
        self.do_click(self.vm_loc["actions_button"])
        logger.info("Clicked Actions menu")

    def click_actions_control_then_stop(self):
        """
        From the Actions menu click Control (submenu) then Stop.
        """
        logger.info("Clicking Actions > Control")
        self.do_click(self.vm_loc["actions_control_menu"])
        logger.info("Clicking Stop")
        self.do_click(self.vm_loc["actions_stop_option"], enable_screenshot=True)
        logger.info("Clicked Stop")

    def click_virtual_machines_tab_and_open_vm(self, vm_name):
        """
        Click the VM name link in the tree view to open its detail page.

        Args:
            vm_name (str): Name of the VirtualMachine to click.
        """
        self.do_click(
            format_locator(self.vm_loc["vm_left_tree_link_tmpl"], vm_name=vm_name)
        )
        logger.info(f"Clicked VM '{vm_name}'")
        self.page_has_loaded()
        logger.info(f"VM detail page for '{vm_name}' loaded")

    def click_actions_clone(self):
        """
        From an open Actions menu click Clone to open the Clone popup.
        """
        self.do_click(self.vm_loc["actions_clone_option"])
        logger.info("Clicked Clone from Actions menu")

    def get_clone_vm_name(self):
        """
        Read the pre-filled VM name from the Clone VirtualMachine popup.

        Returns:
            str: The clone VM name shown in the Name field.
        """
        name_input = self.vm_loc["clone_vm_name_input"]
        wait_for_element_to_be_visible(locator=name_input, timeout=20)
        clone_name = self.get_element_attribute(
            name_input, "value"
        ) or self.get_element_text(name_input)
        logger.info(f"Clone VM name from popup: '{clone_name}'")
        return clone_name

    def tick_start_vm_once_created(self):
        """
        Tick the 'Start VirtualMachine once created' checkbox in the Clone popup.
        """
        checkbox_locator = self.vm_loc["clone_start_vm_checkbox"]
        if not self.get_checkbox_status(locator=checkbox_locator, timeout=20):
            self.do_click(checkbox_locator)
            logger.info("Checked 'Start VirtualMachine once created'")
        else:
            logger.info("'Start VirtualMachine once created' was already checked")

    def click_clone_submit_button(self):
        """
        Click the Clone button at the bottom of the Clone VirtualMachine popup
        and wait for the dialog to close before returning.

        """
        self.do_click(self.vm_loc["clone_submit_button"], enable_screenshot=True)
        logger.info("Clicked Clone submit button — waiting for dialog to close...")

        # Wait up to 3 minutes for the clone dialog to disappear.
        dialog_loc = self.vm_loc["dialog_overlay"]
        end = time.time() + 180
        while time.time() < end:
            els = self.get_elements(dialog_loc)
            if not els or not any(e.is_displayed() for e in els):
                logger.info("Clone dialog closed — navigation to clone VM started")
                return
            time.sleep(2)

        logger.warning("Clone dialog did not close within 3 min — proceeding anyway")

    def click_actions_control_then_start(self):
        """
        From the Actions menu click Control (submenu) then Start.
        """
        logger.info("Clicking Actions > Control")
        self.do_click(self.vm_loc["actions_control_menu"])
        logger.info("Clicking Start")
        self.do_click(self.vm_loc["actions_start_option"], enable_screenshot=True)
        logger.info("Clicked Start")

    def click_actions_take_snapshot(self):
        """
        From an open Actions menu click 'Take snapshot' to open the Take
        snapshot popup.
        """
        self.do_click(self.vm_loc["actions_take_snapshot_option"])
        logger.info("Clicked 'Take snapshot' from Actions menu")

    def click_take_snapshot_save(self):
        """
        Click the 'Save' button inside the 'Take snapshot' popup.
        The snapshot name is auto-filled so no input is required.
        """
        self.do_click(self.vm_loc["take_snapshot_save_button"], enable_screenshot=True)
        logger.info("Clicked Save in Take snapshot popup")

    def click_vm_detail_snapshots_tab(self):
        """
        Click the 'Snapshots' tab on the VM detail page.
        """
        self.do_click(self.vm_loc["vm_detail_snapshots_tab"])
        logger.info("Clicked Snapshots tab")

    def click_vm_detail_overview_tab(self):
        """
        Click the 'Overview' tab on the VM detail page.
        """
        self.do_click(self.vm_loc["vm_detail_overview_tab"])
        logger.info("Clicked Overview tab")
        self.page_has_loaded()

    @retry(
        (AssertionError, TimeoutExpiredError, TimeoutException),
        tries=20,
        delay=15,
        backoff=1,
    )
    def wait_for_snapshot_succeeded(self):
        """
        Wait up to 5 minutes (20 tries × 15 s delay) for the snapshot row
        Status column to show 'Succeeded'.
        """
        logger.info("Checking for Succeeded snapshot status...")
        wait_for_element_to_be_visible(
            locator=self.vm_loc["snapshot_row_status_succeeded"], timeout=5
        )
        logger.info("Snapshot status is now: Succeeded")
        return True

    def click_snapshot_kebab_and_restore(self):
        """
        Click the kebab menu on the snapshot row then select
        'Restore VirtualMachine from snapshot'.
        """
        self.scroll_into_view(self.vm_loc["snapshot_kebab_button"])
        self.do_click(self.vm_loc["snapshot_kebab_button"])
        logger.info("Clicked snapshot row kebab menu")

        self.do_click(self.vm_loc["snapshot_kebab_restore_option"])
        logger.info("Clicked 'Restore VirtualMachine from snapshot'")

    def click_restore_snapshot_confirm(self):
        """
        Click the 'Restore' button in the 'Restore snapshot' confirmation popup.
        """
        self.do_click(self.vm_loc["restore_snapshot_confirm_button"])
        logger.info("Clicked Restore in confirmation popup")

    def wait_for_vm_stopped_long(self, timeout=600):
        """
        Wait up to 10 minutes for the VM status to reach 'Stopped' after a
        snapshot restore.  The VM passes through 'WaitingForVolumeBinding'
        before reaching 'Stopped' — this method polls until Stopped appears.

        Args:
            timeout (int): Maximum seconds to wait (default 600 = 10 minutes).
        """
        logger.info(
            f"Waiting up to {timeout} s for VM status to reach Stopped "
            "(may pass through WaitingForVolumeBinding)..."
        )
        end = time.time() + timeout
        while time.time() < end:
            stopped_els = self.get_elements(self.vm_loc["vm_status_stopped"])
            if stopped_els and stopped_els[0].is_displayed():
                logger.info("VM status is now: Stopped")
                return
            time.sleep(15)
        raise TimeoutExpiredError(f"VM did not reach Stopped status within {timeout} s")
