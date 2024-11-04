"""
This module contains helper classes related to Managed Service ROSA Clusters in different environments.
"""

import logging
import os
import re
import time

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import ROSAAdminLoginFailedException
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


class ROSAEnvCluster:
    """
    A helper class for Managed Service ROSA Clusters in any environment.
    """

    def __init__(self, cluster, env_prefix):
        """
        Initialize required variables.

        Args:
            cluster (str): Name of the cluster in the environment.
            env_prefix (str): Prefix for environment-specific config keys.

        """
        self.cluster = cluster
        self.env_prefix = env_prefix
        self.username_key = f"{self.env_prefix}_cluster_admin_username"
        self.password_key = f"{self.env_prefix}_cluster_admin_password"
        self.kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        self.kubeadmin_password_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["password_location"]
        )
        self.username_path = os.path.join(
            config.ENV_DATA["cluster_path"], "auth", "admin-user"
        )

        # Create "auth" folder if it doesn't exist.
        abs_path = os.path.expanduser(self.kubeconfig_path)
        base_path = os.path.dirname(abs_path)
        os.makedirs(base_path, exist_ok=True)

    def generate_kubeadmin_password_file(self, path=None):
        """
        Creates a kubeadmin password file for the cluster.

        Args:
            path (str): Path to create the kubeadmin password file.

        """
        path = path or self.kubeadmin_password_path
        with open(path, "w+") as fd:
            fd.write(config.ENV_DATA[self.password_key])

    def cluster_admin_login(self, skip_tls_verify=False):
        """
        Logs in to the cluster as admin.

        Args:
            skip_tls_verify (bool): If True, bypasses the certificate check and uses insecure connections.

        Returns:
            str: Output of the `oc login` command.

        """
        cmd = config.ENV_DATA["oc_login_cmd"]
        if skip_tls_verify:
            cmd += " --insecure-skip-tls-verify"
        out = exec_cmd(cmd, ignore_error=True)
        out_str = out.stdout.decode("UTF-8")
        return out_str

    def wait_for_cluster_admin_login_successful(self):
        """
        Waits for the admin login to be successful.

        Raises:
            ROSAAdminLoginFailedException: If admin fails to log in to the cluster.

        """
        for sample in TimeoutSampler(
            600, 10, self.cluster_admin_login, skip_tls_verify=True
        ):
            if "Login successful" in sample:
                logger.info(sample)
                return
            logger.warning("Login failed")
        raise ROSAAdminLoginFailedException

    def generate_kubeconfig_file(self, path=None, skip_tls_verify=False):
        """
        Creates a kubeconfig file for the cluster.

        Args:
            path (str): Path to create the kubeconfig file.
            skip_tls_verify (bool): If True, bypasses the certificate check and uses insecure connections.

        """
        path = path or self.kubeconfig_path
        cmd = f"{config.ENV_DATA['oc_login_cmd']} --kubeconfig {path}"
        if skip_tls_verify:
            cmd += " --insecure-skip-tls-verify"
        exec_cmd(cmd)

    def create_admin_and_login(self):
        """
        Creates an admin account for the cluster and logs in.
        """
        logger.info(f"Creating admin account for cluster {self.cluster}")
        cmd = f"rosa create admin --cluster={self.cluster}"
        out = exec_cmd(cmd)
        out_str = out.stdout.decode("UTF-8")
        for line in out_str.splitlines():
            if "oc login" in line:
                config.ENV_DATA["oc_login_cmd"] = line
                res = re.search(r"oc login (.*) --username (.*) --password (.*)", line)
                config.ENV_DATA[self.username_key] = res.group(2)
                config.ENV_DATA[self.password_key] = res.group(3)
                break
        else:
            logger.error("Failed to parse 'oc login' command from output")
            raise ROSAAdminLoginFailedException(
                "Could not find 'oc login' command in output"
            )

        logger.info("It may take up to a minute for the account to become active")
        time.sleep(10)
        self.wait_for_cluster_admin_login_successful()
        self.create_username_file()

    def create_username_file(self):
        """
        Creates a file with the username for the cluster.

        """

        with open(self.username_path, "w+") as fd:
            fd.write(config.ENV_DATA[self.username_key])

    def get_admin_password(self):
        """
        Get the admin password for the cluster.

        Returns:
            str: Admin password for the cluster.
        """
        return config.ENV_DATA[self.password_key]

    def get_admin_name(self):
        """
        Get the admin username for the cluster.

        Returns:
            str: Admin username for the cluster.
        """
        return config.ENV_DATA[self.username_key]


class ROSAProdEnvCluster(ROSAEnvCluster):
    """
    A helper class for Managed Service ROSA Clusters in the Production Environment.
    """

    def __init__(self, cluster):
        """
        Initialize required variables.

        Args:
            cluster (str): Name of the cluster in the production environment.

        """
        super().__init__(cluster, env_prefix="ms_prod")


class ROSAStageEnvCluster(ROSAEnvCluster):
    """
    A helper class for Managed Service ROSA Clusters in the Stage Environment.
    """

    def __init__(self, cluster):
        """
        Initialize required variables.

        Args:
            cluster (str): Name of the cluster in the stage environment.

        """
        super().__init__(cluster, env_prefix="ms_stage")
