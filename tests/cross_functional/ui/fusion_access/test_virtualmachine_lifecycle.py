"""
Test VirtualMachine Lifecycle - Creation via UI

This test automates the creation of a VirtualMachine in OpenShift Virtualization
using the new multi-step creation wizard
"""

import logging
import pytest

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.framework.testlib import (
    ui,
    ManageTest,
)
from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    ignore_leftovers,
)
from ocs_ci.helpers.helpers import create_unique_resource_name, create_project
from ocs_ci.ocs.ui.page_objects.virtualmachine_ui import VirtualMachineUI

logger = logging.getLogger(__name__)


@ui
@magenta_squad
@ignore_leftovers
class TestVirtualMachineLifecycle(ManageTest):
    """
    Test class for VirtualMachine lifecycle UI automation.
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

    @pytest.mark.polarion_id("OCS-8066")
    def test_create_virtualmachine_from_instancetype(self):
        """
        Test to create a VirtualMachine via the new UI wizard.

        Test Steps:
        1. Create a test namespace and navigate to Workloads > Pods in left nav.
           Open "All Projects" dropdown at the top and select the test namespace.
        2. Navigate to Virtualization > VirtualMachines, dismiss welcome modal.
        3. Click Create, enter a unique VM name, click Next (Deployment details).
        4. Guest OS page: select "Other Linux" (3rd card), open Guest operating
           system type dropdown and pick the latest centos.stream* version, click Next.
        5. Boot source page: click on the latest centos-stream* volume, click Next.
        6. Compute resources page: select small size, click Next.
        7. Customization page: click Storage tab, click the kebab menu on the
           rootdisk row, select Edit, change StorageClass to option ending with
           -vm, click Save, click Next.
        8. Review and create page: click Create VirtualMachine.
        9. Wait for VM status: Provisioning → Running.
        """
        logger.info("=" * 80)
        logger.info("Starting VirtualMachine Creation Test")
        logger.info("=" * 80)

        logger.info("\nStep 1: Create new project")
        logger.info("-" * 80)
        project_obj = create_project()
        namespace = project_obj.namespace

        self.vm_ui.navigate_to_workloads_pods()
        self.base_ui.take_screenshot("workloads_pods_page")

        logger.info(f"Selecting namespace '{namespace}' from All Projects dropdown")
        self.vm_ui.select_project_from_all_projects(namespace)
        self.base_ui.take_screenshot("namespace_selected")

        logger.info("\nStep 2: Navigate to Virtualization > VirtualMachines")
        logger.info("-" * 80)
        self.vm_ui.navigate_to_virtualmachines_page()
        self.base_ui.page_has_loaded()
        self.base_ui.take_screenshot("virtualmachines_page")

        logger.info("\nStep 3: Click Create, enter VM name, click Next")
        logger.info("-" * 80)
        self.vm_ui.dismiss_welcome_modal_if_present(wait_for_modal=True, timeout=15)
        vm_name = create_unique_resource_name("test", "vm")
        logger.info(f"Generated VM name: {vm_name}")
        self.vm_ui.click_create_virtualmachine()
        self.base_ui.take_screenshot("creation_wizard_opened")
        self.vm_ui.enter_vm_name(vm_name)
        self.base_ui.take_screenshot("vm_name_entered")
        self.vm_ui.click_next_button()
        self.base_ui.take_screenshot("deployment_details_next_clicked")

        logger.info(
            "\nStep 4: Guest OS — select Other Linux, pick latest centos.stream"
        )
        logger.info("-" * 80)
        self.vm_ui.select_guest_os_other_linux()
        self.base_ui.take_screenshot("other_linux_selected")

        guest_os_type = self.vm_ui.select_guest_os()
        logger.info(f"Selected Guest OS type: {guest_os_type}")
        self.base_ui.take_screenshot("guest_os_type_selected")

        self.vm_ui.click_next_button()
        self.base_ui.take_screenshot("guest_os_next_clicked")

        logger.info("\nStep 5: Boot source — select latest centos-stream, click Next")
        logger.info("-" * 80)
        self.vm_ui.select_boot_volume_centos_stream_latest()
        self.base_ui.take_screenshot("centos_stream_latest_selected")

        self.vm_ui.click_next_button()
        self.base_ui.take_screenshot("boot_source_next_clicked")

        logger.info("\nStep 6: Compute resources — select small: 1 CPUs, 2 GiB Memory")
        logger.info("-" * 80)
        self.base_ui.take_screenshot("compute_resources_page")
        self.vm_ui.select_compute_size_small()
        self.base_ui.take_screenshot("compute_size_selected")
        self.vm_ui.click_next_button()
        self.base_ui.take_screenshot("compute_resources_next_clicked")

        logger.info("\nStep 7: Customization — Storage tab, edit rootdisk StorageClass")
        logger.info("-" * 80)
        self.base_ui.take_screenshot("customization_page")

        self.vm_ui.click_customization_storage_tab()
        self.base_ui.take_screenshot("customization_storage_tab")

        self.vm_ui.click_rootdisk_kebab_and_edit()
        self.base_ui.take_screenshot("edit_disk_popup_opened")

        storage_class = self.vm_ui.change_storageclass_to_vm_option()
        assert storage_class.endswith(
            "-vm"
        ), f"Expected StorageClass ending with '-vm', got: {storage_class}"
        logger.info(f"Changed StorageClass to: {storage_class}")
        self.base_ui.take_screenshot("storageclass_vm_selected")

        self.vm_ui.click_edit_disk_save()
        self.base_ui.take_screenshot("edit_disk_saved")

        self.vm_ui.click_next_button()
        self.base_ui.take_screenshot("customization_next_clicked")

        logger.info("\nStep 8: Review and create — click Create VirtualMachine")
        logger.info("-" * 80)
        self.base_ui.take_screenshot("review_and_create_page")
        self.vm_ui.click_create_virtualmachine_submit()
        self.base_ui.take_screenshot("vm_creation_initiated")
        self.vm_ui.dismiss_welcome_modal_if_present(wait_for_modal=True, timeout=20)
        self.base_ui.take_screenshot("post_creation_welcome_modal_closed")

        logger.info("\nStep 9: Wait for VM status: Provisioning -> Running")
        logger.info("-" * 80)
        self.base_ui.page_has_loaded()
        logger.info("Waiting for Running status ...")
        self.vm_ui.wait_for_vm_running()
        self.base_ui.take_screenshot("vm_running")
        logger.info(f"VirtualMachine '{vm_name}' is now Running")
        logger.info("VM Created: PASS")
        logger.info("VM Running: PASS")
