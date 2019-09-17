"""
This module contains terraform specific methods and classes needed
for deployment on vSphere platform
"""
import logging

from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


class Terraform(object):
    def __init__(self, path):
        self.path = path

    def initialize(self):
        """
        Initialize a working directory containing Terraform configuration files
        """
        logger.info("Initializing terraform work directory")
        run_cmd(f"terraform init {self.path}")

    def apply(self, tfvars, bootstrap_complete=False):
        """
        Apply the changes required to reach the desired state of the configuration
        """
        if bootstrap_complete:
            cmd = f"terraform apply '-var-file={tfvars}' -auto-approve -var bootstrap_complete=true '{self.path}'"
        else:
            cmd = f"terraform apply '-var-file={tfvars}' -auto-approve '{self.path}'"
        run_cmd(cmd)

    def destroy(self, tfvars):
        """
        Destroys the cluster
        """
        logger.info("Destroying the cluster")
        run_cmd(f"terraform destroy '-var-file={tfvars}' -auto-approve {self.path}")
