"""
This module contains common code and a base class for any on-premise platform
deployment.
"""

import logging
import os
import subprocess

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions
from ocs_ci.utility.bootstrap import gather_bootstrap
from ocs_ci.utility.utils import get_cluster_name, run_cmd


logger = logging.getLogger(__name__)


class OnPremDeploymentBase(Deployment):
    """
    Base class for deployment in on-premise platforms
    """

    def __init__(self):
        super(OnPremDeploymentBase, self).__init__()
        if config.ENV_DATA.get("cluster_name"):
            self.cluster_name = config.ENV_DATA["cluster_name"]
        else:
            self.cluster_name = get_cluster_name(self.cluster_path)

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence according to cluster name prefix

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        raise NotImplementedError()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster in on-premise platform

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        cluster_name_parts = config.ENV_DATA.get("cluster_name").split("-")
        prefix = cluster_name_parts[0]
        if not (
            prefix.startswith(tuple(constants.PRODUCTION_JOBS_PREFIX))
            or config.DEPLOYMENT.get("force_deploy_multiple_clusters")
        ):
            if self.check_cluster_existence(prefix):
                raise exceptions.SameNamePrefixClusterAlreadyExistsException(
                    f"Cluster with name prefix {prefix} already exists. "
                    f"Please destroy the existing cluster for a new cluster "
                    f"deployment"
                )
        super(OnPremDeploymentBase, self).deploy_ocp(log_cli_level)


class IPIOCPDeployment(BaseOCPDeployment):
    """
    Common implementation of IPI OCP deployments for on-premise platforms
    """

    def __init__(self):
        super(IPIOCPDeployment, self).__init__()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for on-premise IPI here
        """
        super(IPIOCPDeployment, self).deploy_prereq()
        if config.DEPLOYMENT["preserve_bootstrap_node"]:
            logger.info("Setting ENV VAR to preserve bootstrap node")
            os.environ["OPENSHIFT_INSTALL_PRESERVE_BOOTSTRAP"] = "True"
            assert os.getenv("OPENSHIFT_INSTALL_PRESERVE_BOOTSTRAP") == "True"

    def deploy(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster for on-prem platform

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        logger.info("Deploying OCP cluster")
        install_timeout = config.DEPLOYMENT.get("openshift_install_timeout")
        logger.info(
            f"Running openshift-install with '{log_cli_level}' log level "
            f"and {install_timeout} second timeout"
        )
        try:
            run_cmd(
                f"{self.installer} create cluster "
                f"--dir {self.cluster_path} "
                f"--log-level {log_cli_level}",
                timeout=install_timeout,
            )
        except (exceptions.CommandFailed, subprocess.TimeoutExpired) as e:
            if constants.GATHER_BOOTSTRAP_PATTERN in str(e):
                try:
                    gather_bootstrap()
                except Exception as ex:
                    logger.error(ex)
            raise e
        self.test_cluster()
