"""
This module contains terraform specific methods and classes needed
for deployment on vSphere platform
"""
import os
import logging

from ocs_ci.framework import config
from ocs_ci.utility import version
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


class Terraform(object):
    """
    Wrapper for terraform
    """

    def __init__(self, path, bin_path=None, state_file_path=None):
        """
        Initialize required variables needed for terraform

        Args:
            path (str): Path to the vSphere modules
            bin_path (str): Path to terraform binary installer
            state_file_path (str): Path to terraform tfstate file

        """
        self.path = path
        self.terraform_installer = bin_path or os.path.expanduser(
            config.ENV_DATA["terraform_installer"]
        )
        self.is_directory_path_supported = False
        self.terraform_version = Terraform.get_terraform_version()
        if state_file_path:
            self.state_file_path = state_file_path
        else:
            self.state_file_path = os.path.join(
                config.ENV_DATA["cluster_path"], "terraform_data", "terraform.tfstate"
            )
        self.state_file_param = ""
        config.ENV_DATA["terraform_state_file"] = self.state_file_path
        if version.get_semantic_version(
            self.terraform_version
        ) <= version.get_semantic_version("0.15"):
            self.is_directory_path_supported = True
        else:
            self.state_file_param = f"-state={self.state_file_path}"

    @staticmethod
    def get_terraform_version():
        terraform_log_path = os.path.join(
            config.ENV_DATA.get("cluster_path"), config.ENV_DATA.get("TF_LOG_FILE")
        )
        terraform_version = config.DEPLOYMENT["terraform_version"]
        try:
            with open(terraform_log_path, "r") as fd:
                logger.debug(f"Reading terraform version from {terraform_log_path}")
                for each_line in fd:
                    if "Terraform version:" in each_line:
                        terraform_version = each_line.split()[-1]
                        logger.debug(
                            f"Terraform version which will be use: {terraform_version}"
                        )
                        return terraform_version
        except FileNotFoundError:
            logger.debug(f"{terraform_log_path} file not found")
        finally:
            return terraform_version

    def initialize(self, upgrade=False):
        """
        Initialize a working directory containing Terraform configuration files

        Args:
            upgrade (bool): True in case installing modules needs upgrade from
                previously-downloaded objects, False otherwise

        """
        logger.info("Initializing terraform work directory")
        if upgrade:
            if self.is_directory_path_supported:
                cmd = f"{self.terraform_installer} init -upgrade {self.path}"
            else:
                cmd = f"{self.terraform_installer} -chdir={self.path} init -upgrade"
        elif self.is_directory_path_supported:
            cmd = f"{self.terraform_installer} init {self.path}"
        else:
            cmd = f"{self.terraform_installer} -chdir={self.path} init"
        run_cmd(cmd, timeout=1200)

    def apply(self, tfvars, bootstrap_complete=False, module=None, refresh=True):
        """
        Apply the changes required to reach the desired state of the configuration

        Args:
            tfvars (str): path to terraform.tfvars file
            bootstrap_complete (bool): Removes bootstrap node if True
            module (str): Module to apply
                e.g: constants.COMPUTE_MODULE
            refresh (bool): If True, updates the state for each resource prior to
                planning and applying

        """
        bootstrap_complete_param = (
            "-var bootstrap_complete=true" if bootstrap_complete else ""
        )
        module_param = f"-target={module}" if module else ""
        refresh_param = "-refresh=false" if not refresh else ""
        if self.is_directory_path_supported:
            chdir_param = ""
            dir_path = self.path
        else:
            chdir_param = f"-chdir={self.path}"
            dir_path = ""
        cmd = (
            f"{self.terraform_installer} {chdir_param} apply {module_param} {refresh_param}"
            f" {self.state_file_param} '-var-file={tfvars}'"
            f" -auto-approve {bootstrap_complete_param} {dir_path}"
        )

        run_cmd(cmd, timeout=1500)

    def destroy(self, tfvars, refresh=True):
        """
        Destroys the cluster

        Args:
            tfvars (str): path to terraform.tfvars file

        """
        logger.info("Destroying the cluster")
        refresh_param = "-refresh=false" if not refresh else ""
        if self.is_directory_path_supported:
            chdir_param = ""
            dir_path = self.path
        else:
            chdir_param = f"-chdir={self.path}"
            dir_path = ""
        cmd = (
            f"{self.terraform_installer} {chdir_param} destroy {refresh_param}"
            f" {self.state_file_param} '-var-file={tfvars}' -auto-approve {dir_path}"
        )
        run_cmd(cmd, timeout=1200)

    def output(self, tfstate, module, json_format=True):
        """
        Extracts the value of an output variable from the state file

        Args:
            tfstate (str): path to terraform.tfstate file
            module (str): module to extract
            json_format (bool): True if format output as json

        Returns:
            str: output from tfstate

        """
        if json_format:
            cmd = (
                f"{self.terraform_installer} output -json -state={tfstate}" f" {module}"
            )
        else:
            cmd = f"{self.terraform_installer} output -state={tfstate} {module}"
        return run_cmd(cmd)

    def destroy_module(self, tfvars, module):
        """
        Destroys the particular module/node

        Args:
            tfvars (str): path to terraform.tfvars file
            module (str): Module to destroy
                e.g: constants.BOOTSTRAP_MODULE

        """
        logger.info(f"Destroying the module: {module}")
        if self.is_directory_path_supported:
            chdir_param = ""
            dir_path = self.path
        else:
            chdir_param = f"-chdir={self.path}"
            dir_path = ""
        cmd = (
            f"{self.terraform_installer} {chdir_param} destroy -auto-approve "
            f" -target={module} {self.state_file_param} '-var-file={tfvars}' {dir_path}"
        )
        run_cmd(cmd, timeout=1200)

    def change_statefile(self, module, resource_type, resource_name, instance):
        """
        Remove the records from the state file so that terraform will no longer be
        tracking the corresponding remote objects.

        Note: terraform state file should be present in the directory from where the
        commands are initiated

        Args:
            module (str): Name of the module
                e.g: compute_vm, module.control_plane_vm etc.
            resource_type (str): Resource type
                e.g: vsphere_virtual_machine, vsphere_compute_cluster etc.
            resource_name (str): Name of the resource
                e.g: vm
            instance (str): Name of the instance
                e.g: compute-0.j-056vu1cs33l-a.qe.rh-ocs.com

        Examples::

            terraform = Terraform(os.path.join(upi_repo_path, "upi/vsphere/"))
            terraform.change_statefile(
                module="compute_vm", resource_type="vsphere_virtual_machine",
                resource_name="vm", instance="compute-0.j-056vu1cs33l-a.qe.rh-ocs.com"
            )

        """
        logger.info("Modifying terraform state file")
        cmd = (
            f"{self.terraform_installer} state rm {self.state_file_param} "
            f"'module.{module}.{resource_type}.{resource_name}[\"{instance}\"]'"
        )
        run_cmd(cmd)
