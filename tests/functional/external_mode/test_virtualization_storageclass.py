import logging
import sys
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier3,
    external_mode_required,
    skipif_ocs_version,
    brown_squad,
)
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.cnv.virtual_machine import VirtualMachine
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.vmware import enable_hardware_virtualization
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@tier3
@brown_squad
@external_mode_required
@skipif_ocs_version("<4.22")
class TestVirtSCAutoProvisioning:

    @pytest.fixture(autouse=True)
    def setup_and_teardown_cnv(self):
        """
        Setup: Ensure CNV is installed and Hardware Virtualization is checked.
        Teardown: Uninstall CNV after the test completion ONLY if it wasn't pre-installed.
        """
        log.info("Checking OpenShift Virtualization CNV status...")
        self.virt_sc_name = constants.EXTERNAL_VIRT_SC_NAME
        self.sc_handler = ocp.OCP(kind=constants.STORAGECLASS)
        cnv_installer = CNVInstaller()
        ns_handler = ocp.OCP(kind=constants.NAMESPACE)

        if (
            config.ENV_DATA.get("enable_hw_virtualization")
            or config.ENV_DATA.get("platform") == constants.VSPHERE_PLATFORM
        ):
            log.info("Verifying if Hardware Virtualization is available on nodes...")
            try:
                enable_hardware_virtualization()
            except (OSError, CommandFailed) as e:
                log.warning(
                    "Hardware virtualization check failed: %s. Emulation will be used.",
                    str(e),
                )

        ns_exists = ns_handler.is_exist(resource_name=constants.CNV_NAMESPACE)
        self.cnv_pre_installed = False

        if ns_exists and cnv_installer.cnv_hyperconverged_installed():
            log.info("CNV namespace and operator detected. Verifying health...")
            if cnv_installer.post_install_verification(raise_exception=False):
                self.cnv_pre_installed = True
            else:
                log.warning("CNV is unhealthy. Forcing redeployment to stabilize...")
                cnv_installer.deploy_cnv(check_cnv_deployed=False)
        else:
            log.info(
                "CNV not found or namespace missing. Initiating full deployment..."
            )
            cnv_installer.deploy_cnv(check_cnv_deployed=False)

        cnv_installer.enable_software_emulation()
        self.is_virt_enabled_by_default = cnv_installer.cnv_hyperconverged_installed()

        yield

        if not self.cnv_pre_installed:
            log.info("Test finished. Initiating CNV Uninstallation (Teardown)...")
            try:
                cnv_installer.uninstall_cnv(check_cnv_installed=True)
                log.info("CNV uninstallation completed successfully.")
            except CommandFailed as e:
                log.error("Failed to uninstall CNV during teardown: %s", str(e))
                raise e
        else:
            log.info(
                "Skipping CNV uninstallation because it was pre-existing on the test cluster."
            )

    def test_virt_sc_and_vm_deployment(self):
        """
        Validate automatic provisioning behavior based on platform configuration.
        """
        if self.is_virt_enabled_by_default:
            log.info("Validating automatic provisioning with Virtualization Enabled.")
            sampler = TimeoutSampler(
                timeout=420,
                sleep=15,
                func=self.sc_handler.is_exist,
                resource_name=self.virt_sc_name,
            )
            if not sampler.wait_for_func_status(result=True):
                pytest.fail(
                    f"Failure: {self.virt_sc_name} was not automatically provisioned."
                )

            vm_name = create_unique_resource_name("test-virt-sc", "vm")
            vm_namespace = create_unique_resource_name("virt-sc-verify", "ns")
            vm_obj = VirtualMachine(vm_name=vm_name, namespace=vm_namespace)

            try:
                log.info(
                    f"Step 2: Creating VM '{vm_name}' using SC '{self.virt_sc_name}'."
                )
                vm_obj.create_vm_workload(
                    sc_name=self.virt_sc_name, ssh=False, verify=False
                )
                log.info(f"Waiting for VM {vm_name} to reach Running status...")
                vm_obj.wait_for_vm_status(status=constants.VM_RUNNING, timeout=300)
            except CommandFailed as e:
                log.error(
                    "VM Deployment testcase failed during creation phase: %s", str(e)
                )
                raise e
            finally:
                log.info("Cleaning up VM resources in namespace: %s", vm_namespace)

                # Check if a primary test exception is currently propagating
                primary_exception_active = sys.exc_info()[0] is not None
                cleanup_exception = None

                try:
                    vm_obj.delete()
                except CommandFailed as e:
                    # Specific scenario handling: catch expected cluster API failures on resource dropping
                    if primary_exception_active:
                        log.warning(
                            "VM workload deletion failed during teardown, but suppressing "
                            "to prioritize the primary test failure. Cleanup error: %s",
                            str(e),
                        )
                    else:
                        log.error(
                            "VM workload deletion resource trace failed: %s", str(e)
                        )
                        cleanup_exception = e

                ns_handler = ocp.OCP(kind=constants.NAMESPACE)
                if ns_handler.is_exist(resource_name=vm_namespace):
                    try:
                        ns_handler.delete(resource_name=vm_namespace)
                        ns_handler.wait_for_delete(
                            resource_name=vm_namespace, timeout=300
                        )
                    except CommandFailed as e:
                        if primary_exception_active:
                            log.warning(
                                "Namespace deletion tracking failed during active error bubble up: %s",
                                str(e),
                            )
                        else:
                            log.error(
                                "Namespace deletion failed explicitly: %s", str(e)
                            )
                            if not cleanup_exception:
                                cleanup_exception = e

                # Only raise the cleanup error if the test body itself ran cleanly
                if cleanup_exception and not primary_exception_active:
                    raise cleanup_exception
        else:
            log.info(
                "Validating that Virt SC does not exist when Virtualization is disabled."
            )
            assert not self.sc_handler.is_exist(
                resource_name=self.virt_sc_name
            ), f"Testcase Failed: {self.virt_sc_name} exists even though OpenShift Virtualization is disabled."

    def test_self_healing_of_deleted_virt_sc(self):
        """
        Validate self-healing behavior of deleted Virt StorageClass.
        """
        log.info(
            "Validating self-healing loop by dropping the Virt StorageClass resource."
        )
        if not self.is_virt_enabled_by_default:
            pytest.skip(
                "Virtualization not enabled on this deployment template. Skipping testcase."
            )

        pre_sampler = TimeoutSampler(
            timeout=420,
            sleep=15,
            func=self.sc_handler.is_exist,
            resource_name=self.virt_sc_name,
        )
        if not pre_sampler.wait_for_func_status(result=True):
            pytest.fail(
                f"Precondition Failed: {self.virt_sc_name} did not appear during initial stabilization window."
            )

        self.sc_handler.delete(resource_name=self.virt_sc_name)

        post_sampler = TimeoutSampler(
            timeout=180,
            sleep=15,
            func=self.sc_handler.is_exist,
            resource_name=self.virt_sc_name,
        )
        assert post_sampler.wait_for_func_status(
            result=True
        ), f"Testcase Failed: The operator did not re-create {self.virt_sc_name} after manual deletion."

        log.info(
            f"Self-healing verified successfully: {self.virt_sc_name} was restored automatically."
        )
