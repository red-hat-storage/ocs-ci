"""
Virtual Machine Snapshot UI Page Object
This module contains the UI page object for Virtual Machine Snapshot operations
"""

from ocs_ci.ocs.ui.base_ui import BaseUI, logger
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.utility.utils import TimeoutSampler


class VirtualMachineSnapshotUI(BaseUI):
    """
    Virtual Machine Snapshot UI Page Object Class
    Handles all UI operations related to VM snapshots
    """

    def __init__(self):
        """
        Initialize VirtualMachineSnapshotUI
        """
        super().__init__()
        self.page_nav = PageNavigator()
        self.vm_snapshot_loc = self.deep_get(
            self.locators_for_current_ocp_version(), "virtualmachine_snapshot"
        )

    @staticmethod
    def locators_for_current_ocp_version():
        """
        Get locators for current OCP version

        Returns:
            dict: Locators dictionary
        """
        from ocs_ci.ocs.ui.views import locators_for_current_ocp_version

        return locators_for_current_ocp_version()

    def navigate_to_virtualization_vms(self):
        """
        Navigate to Virtualization > VirtualMachines in the web console

        Returns:
            bool: True if navigation successful
        """
        logger.info("Navigating to Virtualization > VirtualMachines")
        try:
            # Click on Virtualization menu
            self.do_click(
                locator=self.vm_snapshot_loc["virtualization_menu"],
                enable_screenshot=True,
            )

            # Click on VirtualMachines tab
            self.do_click(
                locator=self.vm_snapshot_loc["virtualmachines_tab"],
                enable_screenshot=True,
            )

            logger.info("Successfully navigated to VirtualMachines page")
            return True
        except Exception as e:
            logger.error(f"Failed to navigate to VirtualMachines: {e}")
            raise

    def select_vm_by_name(self, vm_name):
        """
        Select a VM by name to open the VirtualMachine details page

        Args:
            vm_name (str): Name of the virtual machine

        Returns:
            bool: True if VM selected successfully
        """
        logger.info(f"Selecting VM: {vm_name}")
        try:
            # Find and click on the VM name link
            vm_link_locator = (
                f"a[data-test-id='vm-name-link'][text()='{vm_name}']",
                self.vm_snapshot_loc["vm_name_link"][1],
            )
            self.do_click(locator=vm_link_locator, enable_screenshot=True)

            logger.info(f"Successfully selected VM: {vm_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to select VM {vm_name}: {e}")
            raise

    def navigate_to_snapshots_tab(self):
        """
        Navigate to the Snapshots tab in VM details page

        Returns:
            bool: True if navigation successful
        """
        logger.info("Navigating to Snapshots tab")
        try:
            self.do_click(
                locator=self.vm_snapshot_loc["snapshots_tab"], enable_screenshot=True
            )
            logger.info("Successfully navigated to Snapshots tab")
            return True
        except Exception as e:
            logger.error(f"Failed to navigate to Snapshots tab: {e}")
            raise

    def take_snapshot(self, snapshot_name, accept_disk_warning=False):
        """
        Take a snapshot of the VM via UI

        Args:
            snapshot_name (str): Name for the snapshot
            accept_disk_warning (bool): Whether to accept disk warning if present

        Returns:
            bool: True if snapshot creation initiated successfully
        """
        logger.info(f"Taking snapshot: {snapshot_name}")

        try:
            # Click Take Snapshot button
            logger.info("Clicking Take Snapshot button")
            self.do_click(
                locator=self.vm_snapshot_loc["take_snapshot_button"],
                enable_screenshot=True,
            )

            # Enter snapshot name
            logger.info(f"Entering snapshot name: {snapshot_name}")
            self.do_send_keys(
                locator=self.vm_snapshot_loc["snapshot_name_input"], text=snapshot_name
            )

            # Expand disks section to review
            logger.info("Expanding disks section")
            try:
                self.do_click(
                    locator=self.vm_snapshot_loc["expand_disks_section"],
                    enable_screenshot=True,
                )
            except Exception:
                logger.info("Disks section already expanded or not present")

            # Handle disk warning if needed
            if accept_disk_warning:
                logger.info("Accepting disk warning")
                try:
                    self.do_click(
                        locator=self.vm_snapshot_loc["disk_warning_checkbox"],
                        enable_screenshot=True,
                    )
                except Exception:
                    logger.info("No disk warning checkbox found")

            # Click Save button
            logger.info("Clicking Save button")
            self.do_click(
                locator=self.vm_snapshot_loc["save_snapshot_button"],
                enable_screenshot=True,
            )

            logger.info(f"Snapshot creation initiated: {snapshot_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to take snapshot {snapshot_name}: {e}")
            raise

    def wait_for_snapshot_ready(self, snapshot_name, timeout=300):
        """
        Wait for snapshot to reach Ready state

        Args:
            snapshot_name (str): Name of the snapshot
            timeout (int): Timeout in seconds

        Returns:
            bool: True if snapshot is ready

        Raises:
            ResourceWrongStatusException: If snapshot doesn't reach Ready state
        """
        logger.info(f"Waiting for snapshot {snapshot_name} to be ready")

        try:
            for sample in TimeoutSampler(
                timeout=timeout,
                sleep=10,
                func=self.get_snapshot_status,
                snapshot_name=snapshot_name,
            ):
                if sample == constants.STATUS_READY or sample == "Ready":
                    logger.info(f"Snapshot {snapshot_name} is ready")
                    return True
        except Exception:
            raise ResourceWrongStatusException(
                f"Snapshot {snapshot_name} did not reach Ready state within {timeout}s"
            )

    def get_snapshot_status(self, snapshot_name):
        """
        Get the status of a snapshot

        Args:
            snapshot_name (str): Name of the snapshot

        Returns:
            str: Status of the snapshot
        """
        try:
            # Find snapshot row by name and get status
            snapshot_status_locator = (
                f"tr[data-test-id='snapshot-row'][data-snapshot-name='{snapshot_name}'] "
                f"span[data-test-id='snapshot-status']",
                self.vm_snapshot_loc["snapshot_status"][1],
            )
            status = self.get_element_text(snapshot_status_locator)
            logger.info(f"Snapshot {snapshot_name} status: {status}")
            return status
        except Exception as e:
            logger.warning(f"Could not get status for snapshot {snapshot_name}: {e}")
            return None

    def stop_vm(self):
        """
        Stop the VM via UI

        Returns:
            bool: True if VM stop initiated successfully
        """
        logger.info("Stopping VM")
        try:
            # Click VM actions menu
            self.do_click(
                locator=self.vm_snapshot_loc["vm_actions_menu"], enable_screenshot=True
            )

            # Click Stop VM option
            self.do_click(
                locator=self.vm_snapshot_loc["stop_vm_option"], enable_screenshot=True
            )

            logger.info("VM stop initiated")
            return True
        except Exception as e:
            logger.error(f"Failed to stop VM: {e}")
            raise

    def is_vm_running(self):
        """
        Check if VM is in running state

        Returns:
            bool: True if VM is running, False otherwise
        """
        try:
            status = self.get_element_text(self.vm_snapshot_loc["vm_status"])
            is_running = status.lower() == "running"
            logger.info(f"VM running status: {is_running} (status: {status})")
            return is_running
        except Exception as e:
            logger.warning(f"Could not determine VM status: {e}")
            return False

    def wait_for_vm_stopped(self, timeout=300):
        """
        Wait for VM to stop

        Args:
            timeout (int): Timeout in seconds

        Returns:
            bool: True if VM stopped successfully
        """
        logger.info("Waiting for VM to stop")
        try:
            for sample in TimeoutSampler(
                timeout=timeout, sleep=10, func=self.get_vm_status
            ):
                if sample and sample.lower() in ["stopped", "off"]:
                    logger.info("VM stopped successfully")
                    return True
        except Exception:
            raise ResourceWrongStatusException(f"VM did not stop within {timeout}s")

    def get_vm_status(self):
        """
        Get the current VM status

        Returns:
            str: VM status
        """
        try:
            status = self.get_element_text(self.vm_snapshot_loc["vm_status"])
            return status
        except Exception:
            return None

    def restore_vm_from_snapshot(self, snapshot_name):
        """
        Restore a VM from a snapshot via UI

        Args:
            snapshot_name (str): Name of the snapshot to restore from

        Returns:
            bool: True if restore initiated successfully
        """
        logger.info(f"Restoring VM from snapshot: {snapshot_name}")

        try:
            # Select snapshot row
            snapshot_row_locator = (
                f"tr[data-test-id='snapshot-row'][data-snapshot-name='{snapshot_name}']",
                self.vm_snapshot_loc["snapshot_row"][1],
            )
            self.do_click(locator=snapshot_row_locator, enable_screenshot=True)

            # Click kebab menu
            self.do_click(
                locator=self.vm_snapshot_loc["snapshot_kebab_menu"],
                enable_screenshot=True,
            )

            # Click Restore option
            self.do_click(
                locator=self.vm_snapshot_loc["restore_vm_option"],
                enable_screenshot=True,
            )

            # Confirm restore
            self.do_click(
                locator=self.vm_snapshot_loc["restore_confirm_button"],
                enable_screenshot=True,
            )

            logger.info(f"VM restore initiated from snapshot: {snapshot_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to restore VM from snapshot {snapshot_name}: {e}")
            raise

    def create_vm_from_snapshot(self, snapshot_name, new_vm_name):
        """
        Create a new VM from a snapshot via UI

        Args:
            snapshot_name (str): Name of the snapshot
            new_vm_name (str): Name for the new VM

        Returns:
            bool: True if VM creation initiated successfully
        """
        logger.info(f"Creating new VM {new_vm_name} from snapshot {snapshot_name}")

        try:
            # Select snapshot row
            snapshot_row_locator = (
                f"tr[data-test-id='snapshot-row'][data-snapshot-name='{snapshot_name}']",
                self.vm_snapshot_loc["snapshot_row"][1],
            )
            self.do_click(locator=snapshot_row_locator, enable_screenshot=True)

            # Click kebab menu
            self.do_click(
                locator=self.vm_snapshot_loc["snapshot_kebab_menu"],
                enable_screenshot=True,
            )

            # Click Create VM option
            self.do_click(
                locator=self.vm_snapshot_loc["create_vm_option"], enable_screenshot=True
            )

            # Enter new VM name
            self.do_send_keys(
                locator=self.vm_snapshot_loc["new_vm_name_input"], text=new_vm_name
            )

            # Click Create button
            self.do_click(
                locator=self.vm_snapshot_loc["create_vm_button"], enable_screenshot=True
            )

            logger.info(f"New VM creation initiated: {new_vm_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create VM from snapshot: {e}")
            raise

    def delete_snapshot(self, snapshot_name):
        """
        Delete a snapshot via UI

        Args:
            snapshot_name (str): Name of the snapshot to delete

        Returns:
            bool: True if deletion initiated successfully
        """
        logger.info(f"Deleting snapshot: {snapshot_name}")

        try:
            # Select snapshot row
            snapshot_row_locator = (
                f"tr[data-test-id='snapshot-row'][data-snapshot-name='{snapshot_name}']",
                self.vm_snapshot_loc["snapshot_row"][1],
            )
            self.do_click(locator=snapshot_row_locator, enable_screenshot=True)

            # Click kebab menu
            self.do_click(
                locator=self.vm_snapshot_loc["snapshot_kebab_menu"],
                enable_screenshot=True,
            )

            # Click Delete option
            self.do_click(
                locator=self.vm_snapshot_loc["delete_snapshot_option"],
                enable_screenshot=True,
            )

            # Confirm deletion
            self.do_click(
                locator=self.vm_snapshot_loc["confirm_delete_button"],
                enable_screenshot=True,
            )

            logger.info(f"Snapshot deletion initiated: {snapshot_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete snapshot {snapshot_name}: {e}")
            raise

    def verify_snapshot_exists(self, snapshot_name):
        """
        Verify that a snapshot exists in the list

        Args:
            snapshot_name (str): Name of the snapshot

        Returns:
            bool: True if snapshot exists, False otherwise
        """
        try:
            snapshot_row_locator = (
                f"tr[data-test-id='snapshot-row'][data-snapshot-name='{snapshot_name}']",
                self.vm_snapshot_loc["snapshot_row"][1],
            )
            element = self.wait_until_expected_text_is_found(
                locator=snapshot_row_locator, expected_text=snapshot_name, timeout=30
            )
            exists = element is not None
            logger.info(f"Snapshot {snapshot_name} exists: {exists}")
            return exists
        except Exception:
            logger.info(f"Snapshot {snapshot_name} not found")
            return False


# Made with Bob
