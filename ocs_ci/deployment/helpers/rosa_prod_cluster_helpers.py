"""
This module contains helpers function related to Managed Service ROSA
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
    A base class for Managed Service ROSA Cluster in Production Environment
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

        # login to cluster using admin account
        self._login()

    def _login(self):
        """
        Login to cluster
        """
        self.create_admin()
        self.wait_for_cluster_admin_login_successful()

    def create_admin(self):
        """
        creates admin account for cluster
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

    def cluster_admin_login(self):
        """
        Login to production cluster

        Returns:
            str: output of oc login command

        """
        cmd = config.ENV_DATA["ms_prod_oc_login"]
        out = exec_cmd(cmd, ignore_error=True)
        out_str = str(out.stdout, "UTF-8")
        return out_str

    def wait_for_cluster_admin_login_successful(self):
        """
        Waits for the admin to login successfully

        Raises:
            ROSAProdAdminLoginFailedException: in case of admin failed to log in cluster

        """
        for sample in TimeoutSampler(600, 10, self.cluster_admin_login):
            if "Login successful" in sample:
                logger.info(sample)
                return
            logger.warning(f"Login failed")
        raise ROSAProdAdminLoginFailedException
