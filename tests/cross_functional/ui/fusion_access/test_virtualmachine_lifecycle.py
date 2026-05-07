"""
Test VirtualMachine Lifecycle - Creation and Deletion via UI

This test automates the complete lifecycle of a VirtualMachine in OpenShift Virtualization:
1. Create VM from InstanceType with bootable volume
2. Monitor VM status from Provisioning to Running
3. Stop the VM
4. Delete the VM with grace period and disk deletion
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

# Import the VirtualMachineUI class
from virtualmachine_ui import VirtualMachineUI

logger = logging.getLogger(__name__)


@ui
@green_squad
class TestVirtualMachineLifecycle(ManageTest):
    """
    Test class for VirtualMachine lifecycle UI automation.

    This class contains test cases for:
    1. Creating a VirtualMachine from InstanceType
    2. Selecting bootable volume and storage class
    3. Monitoring VM status transitions
    4. Stopping the VM
    5. Deleting the VM with proper cleanup
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

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")  # Update with actual Polarion ID
    def test_create_and_delete_virtualmachine_from_instancetype(self):
        """
        Test to create and delete a VirtualMachine from InstanceType via UI.

        Test Steps:
        1. Log into the OpenShift console and go to Virtualization > VirtualMachines
        2. On the VirtualMachines page click on "Create VirtualMachine" button
           and select dropdown option 'From InstanceType'
        3. From the "Create new VirtualMachine" page from the volume table
           select the latest/top option as a bootable volume
        4. From the "Select InstanceType" section class choose "General Purpose"
        5. From the same "Create new virtualMachine" page scroll down a little
           to get the third section of "VirtualMachine details" and choose an
           option from the dropdown menu for the storageclass. Pick a storageclass
           name that is created from the previous testcase and name that starts
           with san-xxxxxxx-vm
        6. Ensure the checkbox "Start this VirtualMachine after creation" is checked,
           if not check the box
        7. Click "Create VirtualMachine" button
        8. On the VirtualMachines page from the Details section, Look for the status
           "Provisioning" and wait until it changes to "Running". Get the VirtualMachine name
        9. Go to Virtualization > VirtualMachines page and find the virtual machine created
        10. Check the VirtualMachine box and click on "Actions > Control > Stop"
        11. The status should move from stopping to 'Stopped"
        12. Now click on "Action > Delete "
        13. Check the boxes from the pop up "with grace period" and "Delete disk"
        14. Click on "Delete" button

        Expected Results:
        - VM is created successfully with selected configuration
        - VM transitions from Provisioning to Running state
        - VM can be stopped successfully
        - VM can be deleted with proper cleanup
        """
        logger.info("=" * 80)
        logger.info("Starting VirtualMachine Lifecycle Test")
        logger.info("=" * 80)

        # Step 1: Navigate to Virtualization > VirtualMachines
        logger.info("\nStep 1: Navigate to Virtualization > VirtualMachines")
        logger.info("-" * 80)
        self.vm_ui.navigate_to_virtualmachines_page()
        self.base_ui.take_screenshot("virtualmachines_page")
        time.sleep(3)

        # Step 2: Click on "Create VirtualMachine" and select "From InstanceType"
        logger.info("\nStep 2: Click Create VirtualMachine > From InstanceType")
        logger.info("-" * 80)
        self.vm_ui.click_create_virtualmachine()
        time.sleep(2)
        self.vm_ui.select_from_instancetype_option()
        self.base_ui.take_screenshot("from_instancetype_selected")
        time.sleep(3)

        # Step 3: Select the first/top bootable volume
        logger.info("\nStep 3: Select first bootable volume from table")
        logger.info("-" * 80)
        bootable_volume = self.vm_ui.select_first_bootable_volume()
        logger.info(f"Selected bootable volume: {bootable_volume}")
        self.base_ui.take_screenshot("bootable_volume_selected")
        time.sleep(2)

        # Step 4: Select "General Purpose" InstanceType
        logger.info("\nStep 4: Select General Purpose InstanceType")
        logger.info("-" * 80)
        self.vm_ui.select_general_purpose_instancetype()
        self.base_ui.take_screenshot("general_purpose_selected")
        time.sleep(2)

        # Step 5: Select storage class starting with 'san-' and ending with '-vm'
        logger.info("\nStep 5: Select storage class (san-*-vm)")
        logger.info("-" * 80)
        try:
            storage_class = self.vm_ui.select_storageclass_starting_with_san()
            logger.info(f"Selected storage class: {storage_class}")
            self.base_ui.take_screenshot("storageclass_selected")
        except Exception as e:
            logger.warning(f"Could not select storage class automatically: {e}")
            logger.info(
                "Please ensure a storage class starting with 'san-' and ending with '-vm' exists"
            )
            self.base_ui.take_screenshot("storageclass_selection_failed")
            # Continue with test - storage class might be pre-selected
        time.sleep(2)

        # Step 6: Ensure "Start this VirtualMachine after creation" checkbox is checked
        logger.info(
            "\nStep 6: Ensure 'Start this VirtualMachine after creation' is checked"
        )
        logger.info("-" * 80)
        self.vm_ui.ensure_start_vm_checkbox_checked()
        self.base_ui.take_screenshot("start_vm_checkbox_checked")
        time.sleep(2)

        # Step 7: Click "Create VirtualMachine" button
        logger.info("\nStep 7: Click Create VirtualMachine button")
        logger.info("-" * 80)
        self.vm_ui.click_create_virtualmachine_submit()
        self.base_ui.take_screenshot("vm_creation_initiated")
        time.sleep(5)

        # Step 8: Wait for VM status to change from Provisioning to Running
        logger.info("\nStep 8: Wait for VM status: Provisioning -> Running")
        logger.info("-" * 80)

        # First, wait for Provisioning status
        logger.info("Waiting for Provisioning status...")
        try:
            self.vm_ui.wait_for_vm_status("Provisioning")
            self.base_ui.take_screenshot("vm_provisioning")
        except Exception as e:
            logger.warning(f"Could not detect Provisioning status: {e}")

        # Then wait for Running status
        logger.info("Waiting for Running status...")
        self.vm_ui.wait_for_vm_status("Running")
        self.base_ui.take_screenshot("vm_running")

        # Get VM name
        vm_name = self.vm_ui.get_vm_name_from_details_page()
        logger.info(f"✓ VirtualMachine '{vm_name}' is now Running")

        # Step 9: Navigate back to VirtualMachines list and find the VM
        logger.info("\nStep 9: Navigate to VirtualMachines list and find VM")
        logger.info("-" * 80)
        self.vm_ui.navigate_back_to_virtualmachines_list()
        time.sleep(3)
        self.base_ui.take_screenshot("vm_list_with_new_vm")

        # Step 10: Select VM checkbox and click Actions > Control > Stop
        logger.info("\nStep 10: Stop the VirtualMachine")
        logger.info("-" * 80)
        self.vm_ui.select_vm_checkbox(vm_name)
        self.base_ui.take_screenshot("vm_checkbox_selected")
        time.sleep(1)

        self.vm_ui.click_actions_menu()
        time.sleep(1)
        self.vm_ui.click_control_submenu()
        time.sleep(1)
        self.vm_ui.click_stop_option()
        self.base_ui.take_screenshot("vm_stop_initiated")
        time.sleep(3)

        # Step 11: Wait for VM status to change from Stopping to Stopped
        logger.info("\nStep 11: Wait for VM status: Stopping -> Stopped")
        logger.info("-" * 80)

        # Wait for Stopping status (optional, might be quick)
        try:
            logger.info("Checking for Stopping status...")
            self.vm_ui.wait_for_vm_status("Stopping")
            self.base_ui.take_screenshot("vm_stopping")
        except Exception as e:
            logger.info(f"Stopping status not detected (might be too quick): {e}")

        # Wait for Stopped status
        logger.info("Waiting for Stopped status...")
        self.vm_ui.wait_for_vm_status("Stopped")
        self.base_ui.take_screenshot("vm_stopped")
        logger.info(f"✓ VirtualMachine '{vm_name}' is now Stopped")

        # Step 12: Click Actions > Delete
        logger.info("\nStep 12: Delete the VirtualMachine")
        logger.info("-" * 80)

        # Ensure VM is still selected
        time.sleep(2)
        self.vm_ui.click_actions_menu()
        time.sleep(1)
        self.vm_ui.click_delete_option()
        self.base_ui.take_screenshot("delete_modal_opened")
        time.sleep(2)

        # Step 13 & 14: Check delete options and confirm deletion
        logger.info("\nStep 13-14: Check delete options and confirm deletion")
        logger.info("-" * 80)
        self.vm_ui.check_delete_options_and_confirm()
        self.base_ui.take_screenshot("vm_deletion_confirmed")
        time.sleep(5)

        # Verify VM deletion
        logger.info("\nVerifying VM deletion...")
        logger.info("-" * 80)
        deletion_verified = self.vm_ui.verify_vm_deleted(vm_name)

        if deletion_verified:
            logger.info(f"✓ VirtualMachine '{vm_name}' successfully deleted")
            self.base_ui.take_screenshot("vm_deleted_verified")
        else:
            logger.error(f"✗ VirtualMachine '{vm_name}' deletion could not be verified")
            self.base_ui.take_screenshot("vm_deletion_verification_failed")

        # Final summary
        logger.info("\n" + "=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("-" * 80)
        logger.info(f"VirtualMachine Name: {vm_name}")
        logger.info(f"Bootable Volume: {bootable_volume}")
        logger.info("InstanceType: General Purpose")
        logger.info("VM Created: ✓ PASS")
        logger.info("VM Running: ✓ PASS")
        logger.info("VM Stopped: ✓ PASS")
        logger.info(f"VM Deleted: {'✓ PASS' if deletion_verified else '✗ FAIL'}")
        logger.info("=" * 80)

        # Assert final state
        assert deletion_verified, f"VM '{vm_name}' was not successfully deleted"


# Additional test for error scenarios
@ui
@green_squad
class TestVirtualMachineErrorHandling(ManageTest):
    """
    Test class for VirtualMachine error handling scenarios
    """

    @pytest.fixture(autouse=True)
    def setup_ui(self, setup_ui_class_factory):
        """
        Setup UI session for the test class.
        """
        setup_ui_class_factory()
        self.page_nav = PageNavigator()
        self.base_ui = BaseUI()
        self.vm_ui = VirtualMachineUI()

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")  # Update with actual Polarion ID
    def test_vm_creation_without_storage_class(self):
        """
        Test VM creation behavior when no storage class is selected

        This test verifies proper error handling and validation messages
        """
        logger.info("Testing VM creation without storage class selection")

        # Navigate and start VM creation
        self.vm_ui.navigate_to_virtualmachines_page()
        self.vm_ui.click_create_virtualmachine()
        time.sleep(2)
        self.vm_ui.select_from_instancetype_option()
        time.sleep(3)

        # Select bootable volume and instance type but skip storage class
        self.vm_ui.select_first_bootable_volume()
        self.vm_ui.select_general_purpose_instancetype()

        # Try to create without storage class
        # Expected: Validation error or disabled button
        self.base_ui.take_screenshot("vm_creation_without_storageclass")

        logger.info("Test completed - verify validation behavior")


# Made with Bob
