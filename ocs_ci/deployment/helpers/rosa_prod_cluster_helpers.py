"""
This module contains helpers class related to Managed Service ROSA
Clusters in production environment
"""
import logging
import os
import re
import time

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import ROSAProdAdminLoginFailedException
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


class ROSAProdEnvCluster(object):
    """
    A helper class for Managed Service ROSA Cluster in Production Environment
    """

    def __init__(self, cluster):
        """
        Initialize required variables

        Args:
            cluster (str): Name of the cluster in production environment

        """
        self.cluster = cluster
        self.kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        self.kubeadmin_password_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["password_location"]
        )

        # create "auth" folder
        abs_path = os.path.expanduser(self.kubeconfig_path)
        base_path = os.path.dirname(abs_path)
        os.makedirs(base_path, exist_ok=True)

    def create_admin_and_login(self):
        """
        creates admin account for cluster and login
        """
        logger.info(f"creating admin account to cluster {self.cluster}")
        cmd = f"rosa create admin --cluster={self.cluster}"
        out = exec_cmd(cmd)
        out_str = str(out.stdout, "UTF-8")
        for line in out_str.splitlines():
            if "oc login" in line:
                config.ENV_DATA["ms_prod_oc_login"] = line
                res = re.search(r"oc login (.*) --username (.*) --password (.*)", line)
                config.ENV_DATA["ms_prod_cluster_admin_username"] = res.group(2)
                config.ENV_DATA["ms_prod_cluster_admin_password"] = res.group(3)
                break
        logger.info("It may take up to a minute for the account to become active")
        time.sleep(10)
        self.wait_for_cluster_admin_login_successful()

    def cluster_admin_login(self, skip_tls_verify=False):
        """
        Login to production cluster

        Args:
            skip_tls_verify (bool): True to bypass the certificate check
               and use insecure connections

        Returns:
            str: output of oc login command

        """
        cmd = config.ENV_DATA["ms_prod_oc_login"]
        if skip_tls_verify:
            cmd = f"{cmd} --insecure-skip-tls-verify"
        out = exec_cmd(cmd, ignore_error=True)
        out_str = str(out.stdout, "UTF-8")
        return out_str

    def wait_for_cluster_admin_login_successful(self):
        """
        Waits for the admin to login successfully

        Raises:
            ROSAProdAdminLoginFailedException: in case of admin failed to log in cluster

        """
        for sample in TimeoutSampler(
            600, 10, self.cluster_admin_login, skip_tls_verify=True
        ):
            if "Login successful" in sample:
                logger.info(sample)
                return
            logger.warning("Login failed")
        raise ROSAProdAdminLoginFailedException

    def generate_kubeconfig_file(self, path=None, skip_tls_verify=False):
        """
        creates kubeconfig file for the cluster

        Args:
            path (str): Path to create kubeconfig file
            skip_tls_verify (bool): True to bypass the certificate check
               and use insecure connections

        """
        path = path or self.kubeconfig_path
        cmd = f"{config.ENV_DATA['ms_prod_oc_login']} --kubeconfig {path}"
        if skip_tls_verify:
            cmd = f"{cmd} --insecure-skip-tls-verify"
        exec_cmd(cmd)

    def generate_kubeadmin_password_file(self, path=None):
        """
        creates kubeadmin password file for cluster

        Args:
            path (str): Path to create kubeadmin password file

        """
        path = path or self.kubeadmin_password_path
        with open(path, "w+") as fd:
            fd.write(config.ENV_DATA["ms_prod_cluster_admin_password"])
