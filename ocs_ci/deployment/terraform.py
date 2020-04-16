"""
This module contains terraform specific methods and classes needed
for deployment on vSphere platform
"""
import logging

from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


class Terraform(object):
    """
    Wrapper for terraform
    """
    def __init__(self, path):
        self.path = path

    def initialize(self, upgrade=False):
        """
        Initialize a working directory containing Terraform configuration files

        Args:
            upgrade (bool): True in case installing modules needs upgrade from
                previously-downloaded objects, False otherwise

        """
        logger.info("Initializing terraform work directory")
        if upgrade:
            cmd = f"terraform init -upgrade {self.path}"
        else:
            cmd = f"terraform init {self.path}"
        run_cmd(cmd, timeout=1200)

    def apply(self, tfvars, bootstrap_complete=False):
        """
        Apply the changes required to reach the desired state of the configuration

        Args:
            tfvars (str): path to terraform.tfvars file
            bootstrap_complete (bool): Removes bootstrap node if True

        """
        if bootstrap_complete:
            cmd = f"terraform apply '-var-file={tfvars}' -auto-approve -var bootstrap_complete=true '{self.path}'"
        else:
            cmd = f"terraform apply '-var-file={tfvars}' -auto-approve '{self.path}'"
        run_cmd(cmd, timeout=1500)

    def destroy(self, tfvars):
        """
        Destroys the cluster

        Args:
            tfvars (str): path to terraform.tfvars file

        """
        logger.info("Destroying the cluster")
        run_cmd(
            f"terraform destroy '-var-file={tfvars}' -auto-approve {self.path}",
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
            cmd = f"terraform output -json -state={tfstate} {module}"
        else:
            cmd = f"terraform output -state={tfstate} {module}"
        return run_cmd(cmd)
