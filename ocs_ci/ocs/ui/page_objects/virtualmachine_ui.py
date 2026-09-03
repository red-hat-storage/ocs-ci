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
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
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
        Navigate to Virtualization > VirtualMachines page, wait for the page
        to fully load, then wait up to 60 seconds for the welcome modal to
        appear and dismiss it.
        """
        logger.info("Navigating to Virtualization > VirtualMachines")
        self.choose_expanded_mode(mode=True, locator=self.vm_loc["virtualization_menu"])
        self.do_click(self.vm_loc["virtualmachines_tab"])
        logger.info(
            "Navigated to VirtualMachines page — waiting for page to fully load"
        )
        self.page_has_loaded()
        logger.info("Page loaded — waiting up to 60 s for welcome modal")
        self.dismiss_welcome_modal_if_present(wait_for_modal=True, timeout=60)

    def dismiss_welcome_modal_if_present(self, wait_for_modal=False, timeout=15):
        """
        Dismiss any overlay modal currently blocking the page.

        Args:
            wait_for_modal (bool): If True, poll until the modal appears or timeout
                                   expires before attempting to close it.
            timeout (int): Seconds to wait for the modal when wait_for_modal=True.
        """
        locator = self.vm_loc["modal_close_button"]
        if wait_for_modal:
            end = time.time() + timeout
            while time.time() < end:
                els = self.get_elements(locator)
                if els and els[0].is_displayed():
                    self.do_click(locator)
                    logger.info("Dismissed modal")
                    return
                time.sleep(1)
            logger.info("No modal appeared within timeout")
            return
        els = self.get_elements(locator)
        if els and els[0].is_displayed():
            self.do_click(locator)
            logger.info("Dismissed modal")
            return
        logger.info("No modal to dismiss")

    def enter_vm_name(self, vm_name):
        """
        Enter the VM name in the creation wizard.

        Args:
            vm_name (str): Name to give the VirtualMachine
        """
        logger.info(f"Entering VM name: {vm_name}")
        self.do_clear(self.vm_loc["vm_name_input"])
        self.do_send_keys(self.vm_loc["vm_name_input"], vm_name)
        logger.info(f"Entered VM name: {vm_name}")

    def click_create_virtualmachine(self):
        """
        Click on 'Create VirtualMachine' button (top-right).

        Dismisses any blocking welcome modal before attempting the click
        """

        self.dismiss_welcome_modal_if_present(wait_for_modal=True, timeout=60)
        try:
            self.do_click(self.vm_loc["create_vm_button"])
        except WebDriverException:
            self.click_with_script(self.vm_loc["create_vm_button"])
            logger.info("Clicked Create VirtualMachine button via JS fallback")
            return
        logger.info("Clicked Create VirtualMachine button")

    def click_next_button(self):
        """
        Click the 'Next' button on the current wizard page.
        """
        self.do_click(self.vm_loc["creation_wizard_next"])
        logger.info("Clicked Next button")

    def select_guest_os_other_linux(self):
        """
        On the Guest OS page select the 'Other Linux' card (3rd card).
        """
        self.do_click(self.vm_loc["guest_os_other_linux"])
        logger.info("Selected 'Other Linux' card")

    def select_guest_os(self):
        """
        Open the 'Guest operating system type' dropdown, collect all
        centos.stream* options and select the one with the highest version number.

        Returns:
            str: Text of the selected option (e.g. 'centos.stream11')
        """
        self.do_click(self.vm_loc["guest_os_type_dropdown"])

        options_locator = self.vm_loc["guest_os_type_centos_stream_options"]
        wait_for_element_to_be_visible(locator=options_locator, timeout=20)
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
        On the Compute resources page select the General Purpose card to reveal
        the size dropdown, then open it and select 'small: 1 CPUs, 2 GiB Memory'.
        """
        self.do_click(self.vm_loc["compute_general_purpose_card"])
        logger.info("Selected General Purpose instance type card")
        self.do_click(self.vm_loc["compute_size_dropdown"])
        logger.info("Opened size dropdown")
        self.do_click(self.vm_loc["compute_size_small_option"])
        logger.info("Selected compute size: small: 1 CPUs, 2 GiB Memory")

    def select_boot_volume_centos_stream_latest(self):
        """
        On the Boot source page click on the centos-stream volume row with the
        highest version number (e.g. centos-stream11 is preferred over centos-stream10).
        """
        options_locator = self.vm_loc["boot_volume_centos_stream_options"]
        wait_for_element_to_be_visible(locator=options_locator, timeout=30)
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

    def click_create_virtualmachine_submit(self):
        """
        Click the 'Create VirtualMachine' button on the Review and create page.
        """
        self.do_click(self.vm_loc["create_vm_submit_button"], enable_screenshot=True)
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
        Click the "Virtual machines" tab on the VirtualMachines page to show
        the list, then click the VM name link to open its detail page.

        Args:
            vm_name (str): Name of the VirtualMachine to click.
        """
        logger.info("Clicking 'Virtual machines' tab to show the VM list")
        self.do_click(self.vm_loc["virtual_machines_list_tab"])
        self.page_has_loaded()

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

    def add_boot_volume_via_dialog(self, vm_name):
        """
        On the Boot source page, click 'Add volume', fill in the Add Volume
        dialog, and save.

        Steps performed inside the dialog:

        1. Source type: select 'Volume / Use volume already available on the
           cluster'.
        2. Volume project: open dropdown, select and click it.
        3. Volume name: open dropdown, select the first option (highest
           centos-stream version).
        4. Destination Volume name: type ``<vm_name>-volume``.
        5. StorageClass: open dropdown, select the option ending with '-vm'.
        6. Preference: open dropdown, select the latest centos.stream* option.
        7. Click Save.

        Args:
            vm_name (str): The VM name generated for this run; used to derive
                the destination volume name (``<vm_name>-volume``).

        Returns:
            str: The destination volume name.
        """
        self.do_click(self.vm_loc["boot_source_add_volume_button"])
        logger.info("Clicked 'Add volume' button on Boot source page")

        self.do_click(self.vm_loc["add_volume_source_type_dropdown"])
        logger.info("Opened Source type dropdown")

        wait_for_element_to_be_visible(
            locator=self.vm_loc["add_volume_source_use_existing_volume"], timeout=10
        )
        self.do_click(self.vm_loc["add_volume_source_use_existing_volume"])
        logger.info("Selected 'Volume / Use volume already available on the cluster'")

        self.do_click(self.vm_loc["add_volume_project_dropdown"])
        logger.info("Opened Volume project dropdown")

        self.do_send_keys(
            self.vm_loc["add_volume_project_search_input"],
            "openshift-virtualization-os-images",
        )
        logger.info(
            "Typed 'openshift-virtualization-os-images' in Volume project search"
        )

        self.do_click(
            format_locator(
                self.vm_loc["add_volume_project_option_tmpl"],
                project="openshift-virtualization-os-images",
            )
        )
        logger.info("Selected 'openshift-virtualization-os-images' project")

        self.do_click(self.vm_loc["add_volume_name_dropdown"])
        logger.info("Opened Volume name dropdown")

        first_centos_loc = self.vm_loc["add_volume_first_centos_stream_option"]
        wait_for_element_to_be_visible(locator=first_centos_loc, timeout=20)
        first_centos_els = self.get_elements(first_centos_loc)
        selected_vol_text = first_centos_els[0].text.strip() if first_centos_els else ""
        self.do_click(first_centos_loc)
        logger.info(
            f"Selected Volume name: '{selected_vol_text}' (first centos-stream)"
        )

        dest_volume_name = f"{vm_name}-volume"
        self.scroll_into_view(self.vm_loc["add_volume_destination_name_input"])
        self.do_clear(self.vm_loc["add_volume_destination_name_input"])
        self.do_send_keys(
            self.vm_loc["add_volume_destination_name_input"], dest_volume_name
        )
        logger.info(f"Entered destination Volume name: '{dest_volume_name}'")

        self.scroll_into_view(self.vm_loc["add_volume_storageclass_dropdown"])
        self.do_click(self.vm_loc["add_volume_storageclass_dropdown"])
        logger.info("Opened StorageClass dropdown")

        self.do_click(self.vm_loc["add_volume_storageclass_vm_option"])
        logger.info("Selected StorageClass ending with '-vm'")

        # Preference — open dropdown, pick latest centos.stream* option
        self.scroll_into_view(self.vm_loc["add_volume_preference_dropdown"])
        self.do_click(self.vm_loc["add_volume_preference_dropdown"])
        logger.info("Opened Preference dropdown")

        pref_options_loc = self.vm_loc["add_volume_preference_centos_stream_options"]
        wait_for_element_to_be_visible(locator=pref_options_loc, timeout=20)
        pref_els = self.get_elements(pref_options_loc)
        if not pref_els:
            raise RuntimeError(
                "No centos.stream* preference options found in Add Volume dialog"
            )

        def _pref_version(el):
            # data-test-id is e.g. 'select-option-VirtualMachineClusterPreference-centos.stream10'
            # fall back to visible text if needed
            dt = el.get_attribute("data-test-id") or ""
            label = el.get_attribute("label") or el.text.strip()
            for src in (dt, label):
                if "centos.stream" in src:
                    suffix = src.split("centos.stream")[-1]
                    digits = ""
                    for ch in suffix:
                        if ch.isdigit():
                            digits += ch
                        else:
                            break
                    if digits:
                        return int(digits)
            return 0

        latest_pref = max(pref_els, key=_pref_version)
        selected_pref_text = (
            latest_pref.get_attribute("label") or latest_pref.text.strip()
        )
        latest_pref.click()
        logger.info(
            f"Selected Preference: '{selected_pref_text}' (latest centos.stream)"
        )

        self.scroll_into_view(self.vm_loc["add_volume_save_button"])
        self.do_click(self.vm_loc["add_volume_save_button"])
        logger.info("Clicked Save in Add Volume dialog")

        return dest_volume_name

    def select_boot_volume_by_name(self, volume_name):
        """
        On the Boot source page, click the row whose name cell exactly matches
        ``volume_name``.  This selects the cloned destination volume created by
        :meth:`add_boot_volume_via_dialog`

        Args:
            volume_name (str): Exact name of the volume row to select
        """
        locator = format_locator(
            self.vm_loc["boot_volume_row_by_name_tmpl"], volume_name=volume_name
        )
        wait_for_element_to_be_visible(locator=locator, timeout=30)
        self.do_click(locator)
        logger.info(f"Selected boot volume row: '{volume_name}'")

    def wait_for_clone_in_progress_to_finish(self, timeout=1200):
        """
        After saving the Add Volume dialog, wait for the 'Clone in progress'
        badge to disappear from the boot volume row on the Boot source page.
        The clone can take up to 15 minutes.

        Args:
            timeout (int): Maximum seconds to wait for the clone to finish
                (default 1200s = 20 minutes).

        Raises:
            TimeoutExpiredError: If the badge is still present after *timeout*.
        """
        clone_loc = self.vm_loc["boot_volume_clone_in_progress"]

        # Phase 1: wait up to 60 s for the badge to appear.
        appear_deadline = time.time() + 60
        while time.time() < appear_deadline:
            if any(e.is_displayed() for e in self.get_elements(clone_loc)):
                logger.info("'Clone in progress' badge detected")
                break
            time.sleep(5)
        else:
            logger.info(
                "'Clone in progress' badge never appeared — clone already finished"
            )
            return

        # Phase 2: poll until the badge disappears.
        logger.info(f"Waiting up to {timeout} s for 'Clone in progress' to finish...")
        end = time.time() + timeout
        while time.time() < end:
            try:
                els = self.get_elements(clone_loc)
            except NoSuchElementException:
                els = []
            if not els or not any(e.is_displayed() for e in els):
                logger.info("'Clone in progress' badge is gone — volume is ready")
                return
            time.sleep(15)
        raise TimeoutExpiredError(
            f"'Clone in progress' did not finish within {timeout} s"
        )

    def delete_lungroup_via_ui(self):
        """
        Fetch the LUN group name from the cluster via CLI, navigate to
        Storage > External systems > SAN_Storage dashboard, then delete the LUN
        group via the kebab menu, type the name to confirm, and click Delete.

        Returns:
            str: The LUN group name that was deleted.
        """
        logger.info("Fetching LUN group name")
        ocp = OCP(
            kind=constants.IBM_STORAGE_SCALE_FILESYSTEM,
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )
        fs_out = ocp.exec_oc_cmd(
            f"get {constants.IBM_STORAGE_SCALE_FILESYSTEM}"
            f" -n {constants.IBM_STORAGE_SCALE_NAMESPACE} --no-headers",
            out_yaml_format=False,
        )
        lungroup_name = None
        for line in fs_out.splitlines():
            line = line.strip()
            if line:
                lungroup_name = line.split()[0]
                break
        assert lungroup_name, "Could not parse LUN group name"
        logger.info(f"LUN group name '{lungroup_name}'")

        logger.info("Navigating to Storage > External systems")
        self.choose_expanded_mode(mode=True, locator=self.vm_loc["storage_menu"])
        self.do_click(self.vm_loc["external_systems_nav_link"])
        self.page_has_loaded()
        logger.info("On External systems page")

        self.do_click(self.vm_loc["san_storage_link"])
        self.page_has_loaded()
        logger.info("On SAN Storage dashboard page")
        self.take_screenshot("delete_lungroup_san_storage_page")

        self.scroll_into_view(self.vm_loc["lungroup_kebab_button"])
        self.do_click(self.vm_loc["lungroup_kebab_button"])
        logger.info("Clicked LUN group kebab menu")

        self.do_click(self.vm_loc["lungroup_delete_option"])
        logger.info("Clicked 'Delete LUN group'")

        self.do_send_keys(self.vm_loc["lungroup_confirm_name_input"], lungroup_name)
        logger.info(f"Typed '{lungroup_name}' in confirm input")

        self.do_click(self.vm_loc["lungroup_delete_confirm_button"])
        logger.info(f"Clicked Delete — LUN group '{lungroup_name}' deletion initiated")

        return lungroup_name
