# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on IBM Cloud Platform.
"""

import logging
import os

from ocs_ci.framework import config
from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.utility import ibmcloud


logger = logging.getLogger(__name__)


__all__ = ["IBMCloud"]


class IBMCloudOCPDeployment(BaseOCPDeployment):
    """
    IBM Cloud deployment class.

    """

    def __init__(self):
        super(IBMCloudOCPDeployment, self).__init__()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for IBM cloud deployment.
        """
        super(IBMCloudOCPDeployment, self).deploy_prereq()

    def deploy(self, log_level=""):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level

        """
        # TODO: Add log level to ibmcloud command
        ibmcloud.create_cluster(self.cluster_name)
        kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        ibmcloud.get_kubeconfig(self.cluster_name, kubeconfig_path)
        self.test_cluster()

    def destroy(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        # TODO: Add log level to ibmcloud command
        ibmcloud.destroy_cluster(self.cluster_name)


class IBMCloud(CloudDeploymentBase):
    """
    Deployment class for IBM Cloud
    """

    DEFAULT_STORAGECLASS = "ibmc-vpc-block-10iops-tier"

    OCPDeployment = IBMCloudOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(IBMCloud, self).__init__()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        ibmcloud.login()
        super(IBMCloud, self).deploy_ocp(log_cli_level)

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence based on a cluster name prefix.

        Args:
            cluster_name_prefix (str): name prefix which identifies a cluster

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        logger.info(
            "checking existence of IBM Cloud cluster with prefix %s",
            cluster_name_prefix,
        )
        all_clusters = ibmcloud.list_clusters(provider=config.ENV_DATA["provider"])
        non_term_clusters_with_prefix = [
            cl
            for cl in all_clusters
            if cl["state"] != "deleting" and cl["name"].startswith(cluster_name_prefix)
        ]
        return bool(non_term_clusters_with_prefix)
