"""
Test VirtualMachine Clone Lifecycle - Clone and Verify via UI

This test automates the cloning of a VirtualMachine in OpenShift Virtualization:
1. Find an existing centos VM in Running state
2. Clone the VM with "Start VirtualMachine once created" option
3. Wait for clone to be created and running
4. Login to the cloned VM and verify old files exist
5. Delete the cloned VM as teardown
"""

import logging
import pytest
import time

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.framework.testlib import (
    tier1,
    ui,
    ManageTest,
)
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
)
from ocs_ci.ocs.exceptions import ResourceNotFoundError

# Import the VirtualMachineUI class
from ocs_ci.ocs.ui.page_objects.virtualmachine_ui import VirtualMachineUI

logger = logging.getLogger(__name__)


@ui
@green_squad
class TestVirtualMachineClone(ManageTest):
    """
    Test class for VirtualMachine clone lifecycle UI automation.

    This class contains test cases for:
    1. Finding an existing centos VM in Running state
    2. Cloning the VM with auto-start option
    3. Verifying clone creation and Running status
    4. Logging into cloned VM and verifying files
    5. Deleting the cloned VM as cleanup
    """

    @pytest.fixture(autouse=True)
    def setup_ui(self, setup_ui_class_factory):
        """
        Setup UI session for the test class.

        Args:
            setup_ui_class_factory: Factory fixture to setup UI session
        """
        setup_ui_class_factory()
        self.page_nav = PageNavigator()
        self.base_ui = BaseUI()
        self.vm_ui = VirtualMachineUI()
        self.cloned_vm_name = None  # Store for teardown

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")  # Update with actual Polarion ID
    def test_clone_centos_vm_and_verify_files(self):
        """
        Test to clone a centos VM and verify old files exist in the clone.

        Test Steps:
        1. Log into the OpenShift console and go to Virtualization > VirtualMachines
        2. On the VirtualMachines page check if any centos-*** virtualmachine is
           already there and the status is "Running", if yes then click the 3 dots
           and select option clone from the dropdown option
        3. From the "Clone VirtualMachine" page, Check the checkbox
           "Start VirtualMachine once created" and select the button "Clone"
        4. Wait until clone created
        5. Check if the VM created from the clone is running if not wait until
           its in Running status
        6. Login to the VM and check for the old files in /etc/ in the name of sample.txt
        7. If the file is not there raise an error
        8. Delete the clone VM as the teardown

        Expected Results:
        - A running centos VM is found
        - VM is cloned successfully with auto-start option
        - Cloned VM reaches Running state
        - Old files (sample.txt) exist in the cloned VM
        - Cloned VM is deleted successfully
        """
        logger.info("=" * 80)
        logger.info("Starting VirtualMachine Clone Test")
        logger.info("=" * 80)

        # Step 1: Navigate to Virtualization > VirtualMachines
        logger.info("\nStep 1: Navigate to Virtualization > VirtualMachines")
        logger.info("-" * 80)
        self.vm_ui.navigate_to_virtualmachines_page()
        self.base_ui.take_screenshot("virtualmachines_page")
        time.sleep(3)

        # Step 2: Find running centos VM and click clone option
        logger.info("\nStep 2: Find running centos-*** VM and initiate clone")
        logger.info("-" * 80)

        try:
            original_vm_name = self.vm_ui.find_running_centos_vm()
            logger.info(f"Found running centos VM: {original_vm_name}")
            self.base_ui.take_screenshot("centos_vm_found")
        except ResourceNotFoundError as e:
            logger.error(f"No running centos VM found: {e}")
            pytest.skip("No running centos-*** VM found to clone")

        # Click the 3-dot kebab menu
        self.vm_ui.click_vm_kebab_menu(original_vm_name)
        self.base_ui.take_screenshot("vm_kebab_menu_opened")
        time.sleep(1)

        # Click Clone option
        self.vm_ui.click_clone_option()
        self.base_ui.take_screenshot("clone_page_opened")
        time.sleep(2)

        # Step 3: Check "Start VirtualMachine once created" and click Clone
        logger.info("\nStep 3: Configure clone options and initiate cloning")
        logger.info("-" * 80)

        self.vm_ui.check_start_vm_on_clone_checkbox()
        self.base_ui.take_screenshot("start_vm_checkbox_checked")
        time.sleep(1)

        self.vm_ui.click_clone_button()
        self.base_ui.take_screenshot("clone_initiated")
        time.sleep(5)

        # Step 4: Wait until clone is created
        logger.info("\nStep 4: Wait for clone creation")
        logger.info("-" * 80)

        # Get the cloned VM name
        self.cloned_vm_name = self.vm_ui.get_cloned_vm_name(original_vm_name)
        logger.info(f"Cloned VM name: {self.cloned_vm_name}")
        time.sleep(3)

        # Step 5: Check if cloned VM is Running
        logger.info("\nStep 5: Wait for cloned VM to reach Running status")
        logger.info("-" * 80)

        # Navigate back to VM list if needed
        try:
            self.vm_ui.navigate_back_to_virtualmachines_list()
            time.sleep(3)
        except Exception as e:
            logger.debug(f"Already on VM list page: {e}")

        # Wait for Running status
        logger.info(f"Waiting for cloned VM '{self.cloned_vm_name}' to be Running...")
        self.vm_ui.wait_for_vm_status("Running", vm_name=self.cloned_vm_name)
        self.base_ui.take_screenshot("cloned_vm_running")
        logger.info(f"✓ Cloned VM '{self.cloned_vm_name}' is now Running")

        # Step 6 & 7: Login to VM and check for sample.txt file
        logger.info("\nStep 6-7: Login to cloned VM and verify file existence")
        logger.info("-" * 80)

        try:
            file_exists = self.vm_ui.check_file_in_vm(
                self.cloned_vm_name, file_path="/etc/sample.txt"
            )

            if not file_exists:
                error_msg = f"File /etc/sample.txt not found in cloned VM '{self.cloned_vm_name}'"
                logger.error(error_msg)
                self.base_ui.take_screenshot("file_not_found_error")
                raise AssertionError(error_msg)

            logger.info("✓ File /etc/sample.txt exists in cloned VM")
            self.base_ui.take_screenshot("file_verification_success")

        except Exception as e:
            logger.error(f"Error during file verification: {e}")
            self.base_ui.take_screenshot("file_verification_error")
            raise

        # Final summary
        logger.info("\n" + "=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("-" * 80)
        logger.info(f"Original VM: {original_vm_name}")
        logger.info(f"Cloned VM: {self.cloned_vm_name}")
        logger.info("Clone Created: ✓ PASS")
        logger.info("Clone Running: ✓ PASS")
        logger.info("File Verified: ✓ PASS")
        logger.info("=" * 80)

    def teardown_method(self, method):
        """
        Teardown method to clean up cloned VM

        Args:
            method: Test method that was executed
        """
        # Step 8: Delete the cloned VM
        if self.cloned_vm_name:
            logger.info("\n" + "=" * 80)
            logger.info("TEARDOWN: Deleting cloned VM")
            logger.info("-" * 80)

            try:
                # Navigate to VM list
                self.vm_ui.navigate_back_to_virtualmachines_list()
                time.sleep(3)

                # Select the cloned VM
                self.vm_ui.select_vm_checkbox(self.cloned_vm_name)
                self.base_ui.take_screenshot("cloned_vm_selected_for_deletion")
                time.sleep(1)

                # Click Actions menu
                self.vm_ui.click_actions_menu()
                time.sleep(1)

                # Click Delete option
                self.vm_ui.click_delete_option()
                self.base_ui.take_screenshot("delete_modal_opened")
                time.sleep(2)

                # Check delete options and confirm
                self.vm_ui.check_delete_options_and_confirm()
                self.base_ui.take_screenshot("cloned_vm_deletion_confirmed")
                time.sleep(5)

                # Verify deletion
                deletion_verified = self.vm_ui.verify_vm_deleted(self.cloned_vm_name)

                if deletion_verified:
                    logger.info(
                        f"✓ Cloned VM '{self.cloned_vm_name}' successfully deleted"
                    )
                else:
                    logger.warning(
                        f"⚠ Cloned VM '{self.cloned_vm_name}' deletion could not be verified"
                    )

            except Exception as e:
                logger.error(f"Error during teardown: {e}")
                self.base_ui.take_screenshot("teardown_error")

            logger.info("=" * 80)


# Made with Bob
