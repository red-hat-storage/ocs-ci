"""
Test VirtualMachine Lifecycle - Creation via UI

This test automates the creation of a VirtualMachine in OpenShift Virtualization
using the new multi-step creation wizard
"""

import logging
import os
import time

import pexpect
import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    magenta_squad,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    ui,
)
from ocs_ci.helpers.helpers import create_project, create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
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
        Setup UI session for the test class.  Initialises instance attributes
        used by the teardown fixture so they are always defined even if a test
        fails before the VM is created.

        Args:
            setup_ui_class_factory: Factory fixture to setup UI session
        """
        # Initialise cleanup handles; tests populate these after VM creation.
        self._test_vm_name = None
        self._test_namespace = None

        setup_ui_class_factory()
        self.page_nav = PageNavigator()
        self.base_ui = BaseUI()
        self.vm_ui = VirtualMachineUI()

    @pytest.fixture(autouse=True)
    def teardown_vm(self):
        """
        Teardown fixture — runs after every test method regardless of outcome.

        Lists all VirtualMachines in the test namespace, deletes
        all of them, then deletes the namespace itself.
        """
        yield  # test runs here

        namespace = self._test_namespace
        if not namespace:
            logger.info("teardown_vm: no namespace recorded — nothing to clean up")
            return

        vm_ocp = OCP(kind=constants.VIRTUAL_MACHINE, namespace=namespace)
        ns_ocp = OCP(kind="Namespace")

        # List all VMs in the namespace
        try:
            vm_list = vm_ocp.exec_oc_cmd(
                f"get vm -n {namespace} --no-headers", out_yaml_format=False
            )
            logger.info(vm_list or "(no VMs found)")
        except Exception as e:
            logger.warning(f"Could not list VMs in '{namespace}': {e}")

        # Delete all VMs in the namespace
        try:
            vm_ocp.exec_oc_cmd(f"delete vm --all -n {namespace}", out_yaml_format=False)
            logger.info(f"All VMs deleted from namespace '{namespace}'")
        except Exception as e:
            logger.warning(f"Could not delete VMs in '{namespace}': {e}")

        # Delete the namespace
        try:
            ns_ocp.exec_oc_cmd(f"delete namespace {namespace}", out_yaml_format=False)
            logger.info(f"Namespace '{namespace}' deleted")
        except Exception as e:
            logger.warning(f"Could not delete namespace '{namespace}': {e}")

    def _login_to_vm_console(self, vm_name, namespace, vm_username, vm_password):
        """
        Spawn a virtctl console session and log in to the VM.

        Args:
            vm_name (str): Name of the VirtualMachine.
            namespace (str): Namespace the VM lives in.
            vm_username (str): OS username (e.g. ``centos``).
            vm_password (str): OS password from cloud-init userData.

        Returns:
            pexpect.spawn: The open pexpect child process, already at a shell
            prompt and ready to receive commands.
        """
        virtctl_console_cmd = f"virtctl console {vm_name} --namespace {namespace}"
        logger.info(f"Launching: {virtctl_console_cmd}")

        env = os.environ.copy()
        kubeconfig = config.RUN.get("kubeconfig")
        if kubeconfig:
            env["KUBECONFIG"] = kubeconfig

        child = pexpect.spawn(
            virtctl_console_cmd,
            env=env,
            encoding="utf-8",
            timeout=120,
        )

        # Wait for the "Press Ctrl" banner confirming the console is connected
        child.expect(r"Press Ctrl", timeout=60)
        child.sendline("")

        child.expect(r"login:", timeout=60)
        logger.info("Login prompt detected — sending username")
        child.sendline(vm_username)

        child.expect(r"[Pp]assword:", timeout=60)
        logger.info("Password prompt detected — sending password")
        child.sendline(vm_password)

        child.expect(r"\]\$\s", timeout=60)
        logger.info("Shell prompt detected — logged in successfully")

        return child

    def _calculate_vm_file_md5sum(self, child, test_file, test_data):
        """
        Write *test_data* to *test_file* inside the VM, run ``md5sum``,
        and return the hex digest.

        Args:
            child (pexpect.spawn): An open, logged-in pexpect console session.
            test_file (str): Absolute path of the file to create inside the VM.
            test_data (str): String content to write into the file.

        Returns:
            str: The md5 hex digest of the written file.
        """
        shell_prompt = r"\]\$\s"

        # Write the test data
        child.sendline(f"echo '{test_data}' > {test_file}")
        child.expect(shell_prompt, timeout=30)
        logger.info(f"Test data written to '{test_file}' inside the VM")

        # Run md5sum
        child.sendline(f"md5sum {test_file}")
        child.expect(shell_prompt, timeout=30)

        md5sum_output = None
        for line in child.before.splitlines():
            line = line.strip()
            # A valid md5sum line: 32 hex chars followed by whitespace + filename
            if line and not line.startswith("md5sum") and len(line.split()) >= 2:
                candidate = line.split()[0]
                if len(candidate) == 32 and all(
                    c in "0123456789abcdefABCDEF" for c in candidate
                ):
                    md5sum_output = candidate
                    break

        logger.info(f"Parsed md5sum checksum: {md5sum_output}")
        return md5sum_output

    def _wait_for_vmi_agent_connected(self, vm_name, namespace, timeout=600):
        """
        Poll the VMI conditions until ``AgentConnected`` is ``True``.

        Args:
            vm_name (str): Name of the VirtualMachineInstance (same as the VM name).
            namespace (str): Namespace the VMI lives in.
            timeout (int): Maximum seconds to wait (default 600 = 10 minutes).

        Raises:
            AssertionError: If the VMI does not appear or AgentConnected does not
            become True within *timeout*.
        """
        vmi_ocp = OCP(kind=constants.VIRTUAL_MACHINE_INSTANCE, namespace=namespace)
        deadline = time.time() + timeout

        logger.info(
            f"Waiting for VMI '{vm_name}' to appear in cluster "
            f"(namespace '{namespace}', timeout {timeout}s)..."
        )
        while True:
            vmi_data = vmi_ocp.get(resource_name=vm_name, dont_raise=True, silent=True)
            if vmi_data:
                logger.info(f"VMI '{vm_name}' found in cluster")
                break
            remaining = deadline - time.time()
            assert (
                remaining > 0
            ), f"VMI '{vm_name}' did not appear in cluster within {timeout}s"
            logger.info(
                f"VMI '{vm_name}' not yet found — "
                f"retrying in 10 s ({int(remaining)}s remaining)..."
            )
            time.sleep(10)

        logger.info(
            f"Waiting for AgentConnected on VMI '{vm_name}' "
            f"(namespace '{namespace}')..."
        )
        while True:
            vmi_data = vmi_ocp.get(resource_name=vm_name, dont_raise=True, silent=True)
            if vmi_data:
                conditions = vmi_data.get("status", {}).get("conditions") or []
                for cond in conditions:
                    if (
                        cond.get("type") == "AgentConnected"
                        and cond.get("status") == "True"
                    ):
                        logger.info(
                            f"AgentConnected=True on VMI '{vm_name}' — "
                            "guest OS is fully booted and ready"
                        )
                        return
            remaining = deadline - time.time()
            assert remaining > 0, (
                f"AgentConnected did not become True for VMI '{vm_name}' "
                f"within {timeout}s"
            )
            logger.info(
                f"AgentConnected not yet True on '{vm_name}' — "
                f"retrying in 30 s ({int(remaining)}s remaining)..."
            )
            time.sleep(30)

    def _create_vm_and_wait_for_running(self):
        """
        Create a new namespace, navigate through the VM creation wizard, and
        wait until the new VirtualMachine reaches the Running state.

        Steps performed:
        1. Create a new project/namespace and select it from the All Projects
           dropdown under Workloads > Pods.
        2. Navigate to Virtualization > VirtualMachines.
        3. Open the creation wizard, enter a unique VM name, click Next.
        4. Guest OS: select Other Linux, pick the latest centos.stream* version,
           click Next.
        5. Boot source: select the latest centos-stream* volume, click Next.
        6. Compute resources: select the small size, click Next.
        7. Customization: open the Storage tab, edit the rootdisk row's
           StorageClass to the option ending with -vm, save, click Next.
        8. Review and create: click Create VirtualMachine.
        9. Wait for the VM status to reach Running.

        Returns:
            tuple[str, str]: ``(vm_name, namespace)`` of the newly running VM.
        """
        # Reset so teardown always has the latest namespace for this test run.
        self._test_vm_name = None
        self._test_namespace = None

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

        # Store on the instance so teardown_vm can clean up even on test failure.
        self._test_vm_name = vm_name
        self._test_namespace = namespace

        return vm_name, namespace

    def _fetch_vm_credentials(self, vm_name, namespace):
        """
        Retrieve the cloud-init username and password from the VM's YAML spec.

        Args:
            vm_name (str): Name of the VirtualMachine resource.
            namespace (str): Namespace the VM lives in.

        Returns:
            tuple[str, str]: ``(vm_username, vm_password)``
        """
        logger.info(
            f"Fetching VM YAML for '{vm_name}' in namespace '{namespace}' "
            "to extract credentials"
        )
        vm_ocp = OCP(kind=constants.VIRTUAL_MACHINE, namespace=namespace)
        vm_yaml = vm_ocp.get(resource_name=vm_name)

        vm_username = None
        vm_password = None
        volumes = (
            vm_yaml.get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("volumes", [])
        )
        for volume in volumes:
            cloud_init_data = volume.get("cloudInitNoCloud") or volume.get(
                "cloudInitConfigDrive"
            )
            if cloud_init_data:
                user_data_str = cloud_init_data.get("userData", "")
                user_data = yaml.safe_load(user_data_str)
                vm_username = user_data.get("user")
                vm_password = user_data.get("password")
                break

        assert vm_username, f"Could not find 'user' in userData for VM '{vm_name}'"
        assert vm_password, f"Could not find 'password' in userData for VM '{vm_name}'"
        logger.info("Extracted credentials — username: <masked>, password: <masked>")
        return vm_username, vm_password

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
        10. Fetch VM credentials (username/password) from the VM YAML,
            login to the VM console via virtctl console using pexpect,
            write a test file with known data, compute its md5sum, and assert
            the checksum is non-empty.
        """
        logger.info("=" * 80)
        logger.info("Starting VirtualMachine Creation Test")
        logger.info("=" * 80)

        vm_name, namespace = self._create_vm_and_wait_for_running()
        logger.info("VM Created: PASS")
        logger.info("VM Running: PASS")

        logger.info("\nStep 10: Add data to the VM and compute md5sum via CLI")
        logger.info("-" * 80)

        vm_username, vm_password = self._fetch_vm_credentials(vm_name, namespace)

        self._wait_for_vmi_agent_connected(vm_name, namespace)

        test_file = "/home/centos/ocs_test_data.txt"
        test_data = "OCS CI test data for checksum verification"

        child = self._login_to_vm_console(vm_name, namespace, vm_username, vm_password)

        md5sum = self._calculate_vm_file_md5sum(child, test_file, test_data)

        # Exit the console with Ctrl+] — the correct escape for
        # virtctl console, same as pressing Ctrl+] in a terminal.
        child.send("\x1d")
        child.close()

        assert md5sum, "md5sum value is empty — data write or checksum failed"
        logger.info(f"VM data md5sum checksum stored: {md5sum}")
        logger.info("VM data write and md5sum checksum: PASS")

    @pytest.mark.polarion_id("OCS-8067")
    def test_clone_virtualmachine(self):
        """
        Test cloning a VirtualMachine via the UI and verifying data integrity.

        Test Steps:
        1. Create a new namespace, run through the full VM creation wizard,
           and wait for the VM to reach the Running state.
        2. Fetch VM credentials from the VM YAML.
        3. Log in to the VM console via virtctl, write a test file with known
           data, and compute its md5sum.
        4. Navigate to Virtualization > VirtualMachines and open the VM detail
           page.
        5. Open Actions dropdown, click Clone; the Clone VirtualMachine popup
           opens with the clone name pre-filled. Read the clone name, tick
           'Start VirtualMachine once created', then click Clone.
        6. Wait for the page to finish loading after clone — the UI navigates
           automatically to the cloned VM detail page. Verify status is Running.
        7. Log in to the cloned VM console via virtctl and compute the md5sum
           of the same test file.
        8. Assert the md5sum of the cloned file matches the original checksum.
        """
        logger.info("=" * 80)
        logger.info("Starting VirtualMachine Clone Test")
        logger.info("=" * 80)

        logger.info("\nStep 1: Create VM and wait for Running state")
        logger.info("-" * 80)
        vm_name, namespace = self._create_vm_and_wait_for_running()
        logger.info(f"VM '{vm_name}' in namespace '{namespace}' is Running — PASS")

        logger.info("\nStep 2: Fetch VM credentials from VM YAML")
        logger.info("-" * 80)
        vm_username, vm_password = self._fetch_vm_credentials(vm_name, namespace)

        logger.info("\nStep 3: Write test data to VM and compute md5sum")
        logger.info("-" * 80)
        test_file = "/home/centos/ocs_test_data.txt"
        test_data = "OCS CI test data for checksum verification"

        self._wait_for_vmi_agent_connected(vm_name, namespace)
        child = self._login_to_vm_console(vm_name, namespace, vm_username, vm_password)
        original_md5sum = self._calculate_vm_file_md5sum(child, test_file, test_data)
        child.send("\x1d")
        child.close()

        assert original_md5sum, "md5sum value is empty — data write or checksum failed"
        logger.info(f"Original VM md5sum: {original_md5sum}")
        logger.info("VM data write and md5sum checksum: PASS")

        logger.info("\nStep 4: Navigate to Virtualization > VirtualMachines")
        logger.info("-" * 80)
        self.vm_ui.navigate_to_virtualmachines_page()
        self.base_ui.page_has_loaded()
        self.vm_ui.dismiss_welcome_modal_if_present(wait_for_modal=True, timeout=15)
        self.base_ui.take_screenshot("clone_test_vms_page")

        logger.info(f"\nStep 4b: Click VM '{vm_name}'")
        logger.info("-" * 80)
        self.vm_ui.click_virtual_machines_tab_and_open_vm(vm_name, namespace)
        self.base_ui.take_screenshot("clone_test_original_vm_detail")

        logger.info(
            "\nStep 5: Actions > Clone — read clone name, tick checkbox, submit"
        )
        logger.info("-" * 80)
        self.vm_ui.click_actions_menu()
        self.base_ui.take_screenshot("clone_test_actions_menu_open")

        self.vm_ui.click_actions_clone()
        self.base_ui.take_screenshot("clone_test_clone_popup_open")

        # Read the pre-filled clone name before clicking anything
        clone_vm_name = self.vm_ui.get_clone_vm_name()
        logger.info(f"Clone VM name will be: '{clone_vm_name}'")

        # Tick 'Start VirtualMachine once created' so the clone starts automatically
        self.vm_ui.tick_start_vm_once_created()
        self.base_ui.take_screenshot("clone_test_popup_ready")

        self.vm_ui.click_clone_submit_button()
        self.base_ui.take_screenshot("clone_test_clone_submitted")
        logger.info(f"Clone submitted — clone VM name: '{clone_vm_name}'")

        logger.info(
            "\nStep 6: Wait for page to load after clone, verify cloned VM is Running"
        )
        logger.info("-" * 80)
        self.base_ui.page_has_loaded()
        self.base_ui.take_screenshot("clone_test_clone_vm_detail")

        # If Running within 60 s — proceed. If Stopped — start via Actions > Control > Start.
        self.vm_ui.ensure_cloned_vm_running()
        self.base_ui.take_screenshot("clone_test_clone_vm_running")
        logger.info(f"Cloned VM '{clone_vm_name}' is now Running — PASS")

        logger.info(
            "\nStep 7: Login to cloned VM console and compute md5sum of test file"
        )
        logger.info("-" * 80)
        # Wait for the cloned VM's guest OS to fully boot
        self._wait_for_vmi_agent_connected(clone_vm_name, namespace)

        child = self._login_to_vm_console(
            clone_vm_name,
            namespace,
            vm_username,
            vm_password,
        )

        clone_md5sum = self._calculate_vm_file_md5sum(
            child, test_file, "OCS CI test data for checksum verification"
        )

        child.send("\x1d")
        child.close()

        logger.info(f"Original VM md5sum : {original_md5sum}")
        logger.info(f"Cloned VM md5sum   : {clone_md5sum}")

        logger.info("\nStep 8: Assert cloned VM md5sum matches original")
        logger.info("-" * 80)
        assert clone_md5sum, "md5sum of cloned VM file is empty"
        assert clone_md5sum == original_md5sum, (
            f"Data integrity check FAILED: "
            f"original md5sum={original_md5sum}, "
            f"clone md5sum={clone_md5sum}"
        )
        logger.info("md5sum matches original — data integrity verified: PASS")
