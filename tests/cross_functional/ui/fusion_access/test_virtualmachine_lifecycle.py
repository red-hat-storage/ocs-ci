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
from selenium.common.exceptions import WebDriverException

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
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.page_objects.virtualmachine_ui import VirtualMachineUI
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@ui
@magenta_squad
@ignore_leftovers
class TestVirtualMachineLifecycle(ManageTest):
    """
    Test class for VirtualMachine lifecycle UI automation.
    """

    @pytest.fixture(autouse=True)
    def setup_ui(self, setup_ui_class):
        """
        Setup UI session for the test class.  Initialises instance attributes
        used by the teardown fixture so they are always defined even if a test
        fails before the VM is created.

        Args:
            setup_ui_class: Fixture that logs in and registers browser close on teardown
        """
        self._test_vm_name = None
        self._test_namespace = None

        self.page_nav = PageNavigator()
        self.base_ui = BaseUI()
        self.vm_ui = VirtualMachineUI()

    @pytest.fixture(autouse=True, scope="class")
    def teardown_lungroup(self, request):
        """
        Class-scoped teardown that runs once after all tests in this class.

        Steps:
        1. Check via CLI if any filesystem exists in ibm-spectrum-scale.
           If none found, skip to step 2.  If found, delete it via CLI.
        2. Wait 60 s then delete the LocalDisk associated with the LUN group
           from the CLI.
        3. Wait 60 s then delete the IBM Spectrum Scale cluster resource.
        """

        def cleanup():

            logger.info("teardown_lungroup: starting class-level cleanup")

            # Step 1 — Check filesystem exists and delete via CLI
            lungroup_name = None
            try:
                ocp_fs = OCP(
                    kind=constants.IBM_STORAGE_SCALE_FILESYSTEM,
                    namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
                )
                fs_out = ocp_fs.exec_oc_cmd(
                    f"get {constants.IBM_STORAGE_SCALE_FILESYSTEM}"
                    f" -n {constants.IBM_STORAGE_SCALE_NAMESPACE} --no-headers",
                    out_yaml_format=False,
                )
                if not fs_out or not fs_out.strip():
                    logger.info(
                        f"No {constants.IBM_STORAGE_SCALE_FILESYSTEM} found in "
                        f"{constants.IBM_STORAGE_SCALE_NAMESPACE} — skipping LUN group deletion"
                    )
                else:
                    for line in fs_out.splitlines():
                        line = line.strip()
                        if line:
                            lungroup_name = line.split()[0]
                            break
                    if lungroup_name:
                        ocp_fs.exec_oc_cmd(
                            f"delete {constants.IBM_STORAGE_SCALE_FILESYSTEM} {lungroup_name}"
                            f" -n {constants.IBM_STORAGE_SCALE_NAMESPACE}",
                            out_yaml_format=False,
                        )
                        logger.info(f"Deleted filesystem '{lungroup_name}' via CLI")
                        logger.info("Waiting 60 s after filesystem deletion...")
                        time.sleep(60)
            except CommandFailed as e:
                logger.warning(f"Could not delete filesystem via CLI: {e}")

            # Step 2 — Delete LocalDisk from CLI
            if lungroup_name:
                try:
                    ocp = OCP(
                        kind=constants.IBM_STORAGE_SCALE_LOCALDISK,
                        namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
                    )
                    ld_out = ocp.exec_oc_cmd(
                        f"get localdisks -n {constants.IBM_STORAGE_SCALE_NAMESPACE}"
                        " --no-headers",
                        out_yaml_format=False,
                    )
                    localdisk_name = None
                    for line in ld_out.splitlines():
                        if lungroup_name in line:
                            localdisk_name = line.split()[0]
                            break
                    if localdisk_name:
                        ocp.exec_oc_cmd(
                            f"delete localdisk {localdisk_name}"
                            f" -n {constants.IBM_STORAGE_SCALE_NAMESPACE}",
                            out_yaml_format=False,
                        )
                        logger.info(f"Deleted LocalDisk '{localdisk_name}'")
                    else:
                        logger.warning(
                            f"No LocalDisk found for LUN group '{lungroup_name}'"
                        )
                except CommandFailed as e:
                    logger.warning(f"Could not delete LocalDisk: {e}")

            logger.info("Waiting 60 s before deleting IBM Spectrum Scale cluster...")
            time.sleep(60)

            # Step 3 — Delete IBM Spectrum Scale cluster resource
            try:
                ocp = OCP(
                    kind=constants.IBM_STORAGE_SCALE_CLUSTER_KIND,
                    namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
                )
                ocp.exec_oc_cmd(
                    f"delete {constants.IBM_STORAGE_SCALE_CLUSTER_KIND}"
                    f" ibm-spectrum-scale"
                    f" -n {constants.IBM_STORAGE_SCALE_NAMESPACE}",
                    out_yaml_format=False,
                )
                logger.info("Deleted IBM Spectrum Scale cluster resource")
            except CommandFailed as e:
                logger.warning(
                    f"Could not delete IBM Spectrum Scale cluster resource: {e}"
                )

            logger.info("teardown_lungroup: complete")

        request.addfinalizer(cleanup)

    @pytest.fixture(autouse=True)
    def teardown_vm(self, request):
        """
        Lists all VirtualMachines in the test namespace, deletes
        all of them, then deletes the namespace itself.
        """

        def cleanup():
            namespace = self._test_namespace
            if not namespace:
                logger.info("teardown_vm: no namespace recorded — nothing to clean up")
                return

            vm_ocp = OCP(kind=constants.VIRTUAL_MACHINE, namespace=namespace)
            ns_ocp = OCP(kind="Namespace")

            # List all VMs in the namespace for diagnostics
            try:
                vm_list = vm_ocp.exec_oc_cmd(
                    f"get vm -n {namespace} --no-headers", out_yaml_format=False
                )
                logger.info(vm_list or "(no VMs found)")
            except Exception as e:
                logger.warning(f"Could not list VMs in '{namespace}': {e}")

            # Delete all VMs in the namespace
            try:
                vm_ocp.exec_oc_cmd(
                    f"delete vm --all -n {namespace}", out_yaml_format=False
                )
                logger.info(f"All VMs deleted from namespace '{namespace}'")
            except Exception as e:
                logger.warning(f"Could not delete VMs in '{namespace}': {e}")

            # Delete the namespace
            try:
                ns_ocp.exec_oc_cmd(
                    f"delete namespace {namespace}", out_yaml_format=False
                )
                logger.info(f"Namespace '{namespace}' deleted")
            except Exception as e:
                logger.warning(f"Could not delete namespace '{namespace}': {e}")

        request.addfinalizer(cleanup)

    def _login_to_vm_console(self, vm_name, namespace, vm_username, vm_password):
        """
        Spawn a virtctl console session and log in to the VM.

        Handles two cases:
        - Fresh session: console shows ``login:`` prompt → send username/password.
        - Already-logged-in session: console shows shell prompt directly

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
        # Send a newline to nudge the console past any deprecation warnings or
        # buffered output before the login/shell prompt appears
        child.sendline("")

        # The console may already be at a shell prompt (prior session still
        # active) or at a login: prompt — handle both.
        index = child.expect([r"\]\$\s*", r"[Ll]ogin:\s*"], timeout=120)
        if index == 0:
            logger.info("Shell prompt detected directly — already logged in")
        else:
            logger.info("Login prompt detected — sending username")
            child.sendline(vm_username)

            child.expect(r"[Pp]assword:", timeout=60)
            logger.info("Password prompt detected — sending password")
            child.sendline(vm_password)

            child.expect(r"\]\$\s*", timeout=120)
            logger.info("Shell prompt detected — logged in successfully")

        return child

    def _calculate_vm_file_md5sum(self, child, test_file, test_data=None):
        """
        Args:
            child (pexpect.spawn): An open, logged-in pexpect console session.
            test_file (str): Absolute path of the file to checksum inside the VM.
            test_data (str | None): String content to write into the file before
                checksumming.  Pass ``None`` (default) to checksum without writing.

        Returns:
            str: The md5 hex digest of the file.
        """
        shell_prompt = r"\]\$\s"
        filename = test_file.split("/")[-1]

        if test_data is not None:
            child.sendline(f"echo '{test_data}' > {test_file}")
            child.expect(shell_prompt, timeout=30)
            logger.info(f"Test data written to '{test_file}' inside the VM")
        else:
            # Check the file is present on the cloned disk by listing the
            # parent directory and confirming the filename appears in the output.
            parent_dir = "/".join(test_file.split("/")[:-1]) or "/"
            logger.info(f"Checking for '{filename}' in '{parent_dir}' via ls...")
            child.sendline(f"ls {parent_dir}")
            child.expect(shell_prompt, timeout=30)
            ls_output = child.before
            logger.info(f"ls output: {ls_output.strip()}")
            assert filename in ls_output, (
                f"Expected file '{test_file}' not found on cloned VM — "
                f"'ls {parent_dir}' output:\n{ls_output.strip()}"
            )
            logger.info(f"Confirmed '{filename}' is present on the cloned disk")

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

        if md5sum_output is None:
            logger.warning(f"Could not parse md5sum from output:\n{child.before}")
        else:
            logger.info(f"Parsed md5sum checksum: {md5sum_output}")
        return md5sum_output

    def _wait_for_vmi_agent_connected(self, vm_name, namespace, timeout=1200):
        """
        Wait until the VMI exists in the cluster and its ``AgentConnected``
        condition is ``True``.

        Args:
            vm_name (str): Name of the VirtualMachineInstance (same as the VM).
            namespace (str): Namespace the VMI lives in.
            timeout (int): Maximum seconds to wait across both phases
                (default 600 = 10 minutes).

        Raises:
            ocs_ci.ocs.exceptions.TimeoutExpiredError: If the VMI does not
                appear or AgentConnected does not become True within *timeout*.
        """
        vmi_ocp = OCP(kind=constants.VIRTUAL_MACHINE_INSTANCE, namespace=namespace)

        logger.info(
            f"Waiting for VMI '{vm_name}' to appear in cluster "
            f"(namespace '{namespace}', timeout {timeout}s)..."
        )
        for vmi_data in TimeoutSampler(
            timeout=timeout,
            sleep=10,
            func=vmi_ocp.get,
            resource_name=vm_name,
            dont_raise=True,
            silent=True,
        ):
            if vmi_data:
                logger.info(f"VMI '{vm_name}' found in cluster")
                break

        logger.info(
            f"Waiting for AgentConnected on VMI '{vm_name}' "
            f"(namespace '{namespace}')..."
        )
        for vmi_data in TimeoutSampler(
            timeout=timeout,
            sleep=30,
            func=vmi_ocp.get,
            resource_name=vm_name,
            dont_raise=True,
            silent=True,
        ):
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
        5. Boot source: no volumes are present — click 'Add volume', fill the
           dialog, and save. Wait up to 15 minutes for the 'Clone in progress'
           badge to disappear, then click the volume row and click Next.
        6. Compute resources: select the small size, click Next.
        7. Customization: no changes needed — click Next.
        8. Review and create: click Create VirtualMachine.
        9. Wait for the VM status to reach Running.

        Returns:
            tuple[str, str]: ``(vm_name, namespace)`` of the newly running VM.
        """
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

        logger.info(
            "\nStep 5: Boot source — Add volume dialog, wait for clone, select volume"
        )
        logger.info("-" * 80)
        self.base_ui.take_screenshot("boot_source_page_no_volumes")

        dest_volume_name = self.vm_ui.add_boot_volume_via_dialog(vm_name)
        self.base_ui.take_screenshot("add_volume_dialog_saved")

        logger.info("Waiting for boot volume clone to finish ")
        self.vm_ui.wait_for_clone_in_progress_to_finish(timeout=1200)
        self.base_ui.take_screenshot("clone_finished")

        logger.info(
            f"Selecting the cloned destination volume row: '{dest_volume_name}'"
        )
        self.vm_ui.select_boot_volume_by_name(dest_volume_name)
        self.base_ui.take_screenshot("boot_volume_selected")

        self.vm_ui.click_next_button()
        self.base_ui.take_screenshot("boot_source_next_clicked")

        logger.info("\nStep 6: Compute resources — select small: 1 CPUs, 2 GiB Memory")
        logger.info("-" * 80)
        self.base_ui.take_screenshot("compute_resources_page")
        self.vm_ui.select_compute_size_small()
        self.base_ui.take_screenshot("compute_size_selected")
        self.vm_ui.click_next_button()
        self.base_ui.take_screenshot("compute_resources_next_clicked")

        logger.info("\nStep 7: Customization — no changes, click Next")
        logger.info("-" * 80)
        self.base_ui.take_screenshot("customization_page")
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
                if not isinstance(user_data, dict):
                    logger.warning(
                        "cloud-init userData is not a mapping — skipping volume"
                    )
                    continue
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
        5. Boot source page: click 'Add volume', fill the Add Volume dialog
           (source type Volume, project openshift-virtualization-os-images, latest
           centos-stream PVC, destination volume name), and save. Wait for the
           'Clone in progress' badge to disappear, then click the volume row and
           click Next.
        6. Compute resources page: select small size, click Next.
        7. Customization page: no changes needed — click Next.
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

        child.send("\x1d")
        child.close()

        assert md5sum, "md5sum value is empty — data write or checksum failed"
        logger.info(f"VM data md5sum checksum stored: {md5sum}")
        logger.info("VM data write and md5sum checksum: PASS")

    @pytest.mark.polarion_id("OCS-8223")
    def test_virtualmachine_snapshot_and_restore(self):
        """
        Test VM snapshot creation and restore via the UI.

        Test Steps:
        1. Create a new namespace, run through the full VM creation wizard,
           and wait for the VM to reach the Running state.
        2. Fetch VM credentials from the VM YAML.
        3. Log in to the VM console via virtctl, write a test file with known
           data, and compute its md5sum.
        4. Open Actions dropdown, click Take snapshot; a popup opens with the
           snapshot name auto-filled — click Save.
        5. Navigate to the Snapshots tab, wait for the snapshot
           status to reach Succeeded, then sleep 30 s.  Modify existing data on vm
           by appending to the existing test file via the VM console.
        6. Power off the VM via Actions > Control > Stop and wait for the VM
           status to change to Stopped in the overview page.
        7. Navigate back to the Snapshots tab, click the kebab menu for the
           snapshot, select 'Restore VirtualMachine from snapshot', then click
           Restore in the confirmation popup.
        8. Navigate to the Overview page and wait up to 10 minutes for the VM
           status to reach Stopped, then start the VM via Actions > Control >
           Start and wait for Running.
        9. SSH to the VM and verify the file modification made in step 5 is
           absent — the file content should match the original md5sum from
           step 3.
        """
        logger.info("=" * 80)
        logger.info("Starting VirtualMachine Snapshot and Restore Test")
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
        test_data = "OCS CI test data for snapshot restore verification"

        self._wait_for_vmi_agent_connected(vm_name, namespace)
        child = self._login_to_vm_console(vm_name, namespace, vm_username, vm_password)
        original_md5sum = self._calculate_vm_file_md5sum(child, test_file, test_data)
        child.send("\x1d")
        child.close()

        assert original_md5sum, "md5sum value is empty — data write or checksum failed"
        logger.info(f"Original VM md5sum: {original_md5sum}")
        logger.info("VM data write and md5sum checksum calculation is successful")

        logger.info("\nStep 4: Take snapshot of the vm")
        logger.info("-" * 80)
        self.vm_ui.click_actions_menu()
        self.base_ui.take_screenshot("snapshot_test_actions_menu_open")

        self.vm_ui.click_actions_take_snapshot()
        self.base_ui.take_screenshot("snapshot_test_take_snapshot_popup")

        self.vm_ui.click_take_snapshot_save()
        self.base_ui.take_screenshot("snapshot_test_snapshot_save_clicked")
        logger.info("Snapshot creation initiated successfully")

        logger.info(
            "\nStep 5: Navigate to Snapshots tab, wait for Succeeded, later modify existing data in vm"
        )
        logger.info("-" * 80)
        self.vm_ui.click_vm_detail_snapshots_tab()
        self.base_ui.take_screenshot("snapshot_test_snapshots_tab")

        self.vm_ui.wait_for_snapshot_succeeded()
        self.base_ui.take_screenshot("snapshot_test_snapshot_succeeded")
        logger.info("Snapshot status reached: Succeeded ")

        logger.info("Sleeping 30 s after snapshot succeeded before modifying data...")
        time.sleep(30)

        logger.info("Modifying VM data — appending to test file via console")
        child = self._login_to_vm_console(vm_name, namespace, vm_username, vm_password)
        child.sendline(f'echo "Data modified after snapshot" >> {test_file}')
        child.expect(r"\]\$\s", timeout=30)
        logger.info("Data modification appended to test file")
        modified_md5sum = self._calculate_vm_file_md5sum(child, test_file)
        child.send("\x1d")
        child.close()

        assert modified_md5sum, "md5sum after modification is empty"
        assert modified_md5sum != original_md5sum, (
            "The post-snapshot modification did not change the file: "
            f"md5sum is still {original_md5sum}. The restore check would pass "
            "without exercising the restore."
        )
        logger.info(f"Modified VM md5sum: {modified_md5sum}")

        logger.info("\nStep 6: Power off VM via Actions > Control > Stop")
        logger.info("-" * 80)
        self.vm_ui.click_vm_detail_overview_tab()
        self.base_ui.take_screenshot("snapshot_test_overview_before_stop")
        self.vm_ui.click_actions_menu()
        self.base_ui.take_screenshot("snapshot_test_actions_stop_menu")
        self.vm_ui.click_actions_control_then_stop()
        self.base_ui.take_screenshot("snapshot_test_stop_clicked")

        self.vm_ui.wait_for_vm_stopped()
        self.base_ui.take_screenshot("snapshot_test_vm_stopped")
        logger.info("VM status reached: Stopped")

        logger.info(
            "\nStep 7: Snapshots tab > kebab > Restore VirtualMachine from snapshot"
        )
        logger.info("-" * 80)
        self.vm_ui.click_vm_detail_snapshots_tab()
        self.base_ui.take_screenshot("snapshot_test_snapshots_tab_before_restore")

        self.vm_ui.click_snapshot_kebab_and_restore()
        self.base_ui.take_screenshot("snapshot_test_restore_popup")

        self.vm_ui.click_restore_snapshot_confirm()
        self.base_ui.take_screenshot("snapshot_test_restore_confirmed")
        logger.info("Restore initiated from snapshot")

        logger.info("\nStep 8: Navigate to Overview; wait for Stopped, then Start")
        logger.info("-" * 80)
        self.vm_ui.click_vm_detail_overview_tab()
        self.base_ui.take_screenshot("snapshot_test_overview_after_restore")

        self.vm_ui.wait_for_vm_stopped_long()
        self.base_ui.take_screenshot("snapshot_test_vm_stopped_after_restore")
        logger.info("VM status after restore: Stopped — PASS")

        logger.info("Starting VM via Actions > Control > Start")
        self.vm_ui.click_actions_menu()
        self.vm_ui.click_actions_control_then_start()
        self.base_ui.take_screenshot("snapshot_test_start_clicked")

        self.vm_ui.wait_for_vm_running()
        self.base_ui.take_screenshot("snapshot_test_vm_running_after_restore")
        logger.info("VM status after restore and start: Running — PASS")

        logger.info("\nStep 9: Validate restore — verify file content matches original")
        logger.info("-" * 80)
        self._wait_for_vmi_agent_connected(vm_name, namespace)

        child = self._login_to_vm_console(vm_name, namespace, vm_username, vm_password)
        restored_md5sum = self._calculate_vm_file_md5sum(child, test_file)
        child.send("\x1d")
        child.close()

        logger.info(f"Original md5sum : {original_md5sum}")
        logger.info(f"Restored md5sum : {restored_md5sum}")

        assert restored_md5sum, "md5sum of restored VM file is empty"
        assert restored_md5sum == original_md5sum, (
            f"Snapshot restore data integrity check FAILED: "
            f"original md5sum={original_md5sum}, "
            f"restored md5sum={restored_md5sum}. "
            "The modification made after the snapshot is still present."
        )
        logger.info(
            "Restored md5sum matches original — snapshot restore verified: PASS"
        )

    @pytest.mark.polarion_id("OCS-8091")
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
        6. Wait for the page to finish loading after clone — Verify status is Running.
        7. Log in to the cloned VM console, confirm the file exists, compute
           its md5sum without writing anything.
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
        self.vm_ui.click_virtual_machines_tab_and_open_vm(vm_name)
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
            "\nStep 6: Wait for clone detail page to load, verify cloned VM is Running"
        )
        logger.info("-" * 80)
        self.base_ui.page_has_loaded()
        logger.info("Waiting 30 s for cloned VM detail page to fully render...")
        time.sleep(30)
        self.base_ui.page_has_loaded()
        self.base_ui.take_screenshot("clone_test_clone_vm_detail")

        # If Running within 4 min — proceed. If Stopped — start via Actions > Control > Start.
        self.vm_ui.ensure_cloned_vm_running()
        self.base_ui.take_screenshot("clone_test_clone_vm_running")
        logger.info(f"Cloned VM '{clone_vm_name}' is now Running — PASS")

        logger.info(
            "\nStep 7: Login to cloned VM console, verify file exists, compute md5sum"
        )
        logger.info("-" * 80)
        # Wait for the cloned VM's guest OS to fully boot
        self._wait_for_vmi_agent_connected(clone_vm_name, namespace, timeout=1200)

        child = self._login_to_vm_console(
            clone_vm_name,
            namespace,
            vm_username,
            vm_password,
        )

        # Pass no test_data — the file was written to the parent VM's disk and
        # should be present on the clone unchanged.  The helper first asserts
        # the file exists, then reads its md5sum without overwriting it.
        clone_md5sum = self._calculate_vm_file_md5sum(child, test_file)

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

        # Initiating Ui deletion for lungroup and verification of lungorup deletion
        # will be taken care in teardown part

        logger.info("\nStep 9: Delete LUN group via UI")
        logger.info("-" * 80)
        try:
            lungroup_name = self.vm_ui.delete_lungroup_via_ui()
            logger.info(f"LUN group '{lungroup_name}' deletion initiated via UI")
        except WebDriverException as e:
            logger.warning(f"Could not delete LUN group via UI (browser error): {e}")
