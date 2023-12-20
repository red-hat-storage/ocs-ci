# -*- coding: utf8 -*-
"""
This module contains common code and a base class for any cloud platform
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
from ocs_ci.utility.deployment import get_cluster_prefix
from ocs_ci.utility.ibmcloud import label_worker_nodes_region
from ocs_ci.utility.utils import get_cluster_name, run_cmd

logger = logging.getLogger(__name__)


class CloudDeploymentBase(Deployment):
    """
    Base class for deployment on a cloud platform (such as AWS, Azure, ...).
    """

    def __init__(self):
        """
        Any cloud platform deployment requires region and cluster name.
        """
        super(CloudDeploymentBase, self).__init__()
        self.region = config.ENV_DATA["region"]
        if config.ENV_DATA.get("cluster_name"):
            self.cluster_name = config.ENV_DATA["cluster_name"]
        else:
            self.cluster_name = get_cluster_name(self.cluster_path)
        # dict of cluster prefixes with special handling rules (for existence
        # check or during a cluster cleanup)
        self.cluster_prefixes_special_rules = {}

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
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        if not config.DEPLOYMENT.get("force_deploy_multiple_clusters"):
            prefix = get_cluster_prefix(
                self.cluster_name, self.cluster_prefixes_special_rules
            )
            if self.check_cluster_existence(prefix):
                raise exceptions.SameNamePrefixClusterAlreadyExistsException(
                    f"Cluster with name prefix {prefix} already exists. "
                    f"Please destroy the existing cluster for a new cluster "
                    f"deployment"
                )
        super(CloudDeploymentBase, self).deploy_ocp(log_cli_level)


class IPIOCPDeployment(BaseOCPDeployment):
    """
    Common implementation of IPI OCP deployments for cloud platforms.
    """

    def __init__(self):
        super(IPIOCPDeployment, self).__init__()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for cloud IPI here.
        """
        super(IPIOCPDeployment, self).deploy_prereq()
        if config.DEPLOYMENT["preserve_bootstrap_node"]:
            logger.info("Setting ENV VAR to preserve bootstrap node")
            os.environ["OPENSHIFT_INSTALL_PRESERVE_BOOTSTRAP"] = "True"
            assert os.getenv("OPENSHIFT_INSTALL_PRESERVE_BOOTSTRAP") == "True"

    def deploy(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")
        """
        logger.info("Deploying OCP cluster")
        install_timeout = config.DEPLOYMENT.get("openshift_install_timeout")
        logger.info(
            f"running openshift-install with '{log_cli_level}' log level "
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
        label_worker_nodes_region()
