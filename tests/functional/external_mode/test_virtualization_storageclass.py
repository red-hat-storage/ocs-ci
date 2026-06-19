import logging
import pytest
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier3,
    external_mode_required,
    skipif_ocs_version,
    brown_squad,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.cnv.virtual_machine import VirtualMachine
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.deployment.vmware import enable_hardware_virtualization

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
        log.info("Checking for OpenShift Virtualization (CNV) status...")
        cnv_installer = CNVInstaller()
        ns_handler = ocp.OCP(kind=constants.NAMESPACE)

        # 1. Hardware Virtualization Check
        if (
            config.ENV_DATA.get("enable_hw_virtualization")
            or config.ENV_DATA.get("platform") == constants.VSPHERE_PLATFORM
        ):
            log.info("Verifying if Hardware Virtualization is available on nodes...")
            try:
                enable_hardware_virtualization()
            except Exception as e:
                log.warning(
                    f"Hardware virtualization check failed: {e}. Emulation will be used."
                )

        # 2. Robust Installation & Pre-existing State Tracking Check
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

        # 3. Force Software Emulation (Safety for vSphere/Cloud labs)
        cnv_installer.enable_software_emulation()

        yield  # Execute the test

        # --- TEARDOWN ---
        # FIX: Only run uninstallation if CNV was not already active prior to execution
        if not self.cnv_pre_installed:
            log.info("Test finished. Initiating CNV Uninstallation (Teardown)...")
            try:
                cnv_installer.uninstall_cnv(check_cnv_installed=True)
                log.info("CNV uninstallation completed successfully.")
            except Exception as e:
                log.error(f"Failed to uninstall CNV during teardown: {str(e)}")
                raise e
        else:
            log.info(
                "Skipping CNV uninstallation because it was pre-existing on the test cluster."
            )

    def test_virt_sc_and_vm_deployment(self):
        """
        1. Verify the ODF Virtualization StorageClass exists.
        2. Deploy a VM using that specific StorageClass.
        """
        virt_sc_name = constants.EXTERNAL_VIRT_SC_NAME
        vm_name = create_unique_resource_name("test-virt-sc", "vm")
        vm_namespace = create_unique_resource_name("virt-sc-verify", "ns")

        # Initialize the VM object
        vm_obj = VirtualMachine(vm_name=vm_name, namespace=vm_namespace)

        try:
            # 1. Verify Virt StorageClass existence
            log.info(f"Step 1: Verifying {virt_sc_name} presence on the cluster.")
            sc_handler = ocp.OCP(kind=constants.STORAGECLASS)

            sampler = TimeoutSampler(
                timeout=420,
                sleep=15,
                func=sc_handler.is_exist,
                resource_name=virt_sc_name,
            )
            if not sampler.wait_for_func_status(True):
                pytest.fail(
                    f"Failure: {virt_sc_name} was not provisioned. Check if ODF virtualization pool is enabled."
                )

            # 2. Deploy VM Workload
            log.info(f"Step 2: Creating VM '{vm_name}' using SC '{virt_sc_name}'.")
            vm_obj.create_vm_workload(sc_name=virt_sc_name, ssh=False, verify=False)

            # 2. Manually wait for ONLY the "Running" status
            log.info(f"Waiting for VM {vm_name} to reach Running status...")
            vm_obj.wait_for_vm_status(status=constants.VM_RUNNING, timeout=300)

        except Exception as e:
            log.error(f"Test logic failed: {str(e)}")
            raise e

        finally:
            log.info(f"Cleaning up VM resources in {vm_namespace}...")
            try:
                vm_obj.delete()
            except Exception as e:
                log.warning(f"VM deletion failed (might already be gone): {e}")

            ns_handler = ocp.OCP(kind=constants.NAMESPACE)
            if ns_handler.is_exist(resource_name=vm_namespace):
                ns_handler.delete(resource_name=vm_namespace)
                ns_handler.wait_for_delete(resource_name=vm_namespace, timeout=300)
