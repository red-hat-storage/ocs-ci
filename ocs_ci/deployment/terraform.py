"""
This module contains terraform specific methods and classes needed
for deployment on vSphere platform
"""
import os
import logging

from ocs_ci.framework import config
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


class Terraform(object):
    """
    Wrapper for terraform
    """
    def __init__(self, path, bin_path=None):
        """
        Initialize required variables needed for terraform

        Args:
            path (str): Path to the vSphere modules
            bin_path (str): Path to the terraform binary installer

        """
        self.path = path
        self.terraform_installer = bin_path or os.path.expanduser(
            config.ENV_DATA['terraform_installer']
        )

    def initialize(self, upgrade=False):
        """
        Initialize a working directory containing Terraform configuration files

        Args:
            upgrade (bool): True in case installing modules needs upgrade from
                previously-downloaded objects, False otherwise

        """
        logger.info("Initializing terraform work directory")
        if upgrade:
            cmd = f"{self.terraform_installer} init -upgrade {self.path}"
        else:
            cmd = f"{self.terraform_installer} init {self.path}"
        run_cmd(cmd, timeout=1200)

    def apply(self, tfvars, bootstrap_complete=False):
        """
        Apply the changes required to reach the desired state of the configuration

        Args:
            tfvars (str): path to terraform.tfvars file
            bootstrap_complete (bool): Removes bootstrap node if True

        """
        bootstrap_complete_param = (
            "-var bootstrap_complete=true" if bootstrap_complete else ""
        )
        cmd = (
            f"{self.terraform_installer} apply '-var-file={tfvars}'"
            f" -auto-approve {bootstrap_complete_param} '{self.path}'"
        )

        run_cmd(cmd, timeout=1500)

    def destroy(self, tfvars):
        """
        Destroys the cluster

        Args:
            tfvars (str): path to terraform.tfvars file

        """
        logger.info("Destroying the cluster")
        run_cmd(
            f"{self.terraform_installer} destroy '-var-file={tfvars}'"
            f" -auto-approve {self.path}",
            timeout=1200,
        )

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
                f"{self.terraform_installer} output -json -state={tfstate}"
                f" {module}"
            )
        else:
            cmd = (
                f"{self.terraform_installer} output -state={tfstate} {module}"
            )
        return run_cmd(cmd)
