# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Azure platform.
"""

import logging
import json

from ocs_ci.framework import config
from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.cloud import IPIOCPDeployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.utility import version
from ocs_ci.utility.azure_utils import AZURE as AzureUtil, AzureAroUtil
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


__all__ = ["AZUREIPI"]


class AZUREBase(CloudDeploymentBase):
    """
    Azure deployment base class, with code common to both IPI and UPI.

    Having this base class separate from AZUREIPI even when we have implemented
    IPI only makes adding UPI class later easier, moreover code structure is
    comparable with other platforms.
    """

    # default storage class for StorageCluster CRD on Azure platform
    # From OCP 4.11, default storage class is managed-csi
    if version.get_semantic_ocp_version_from_config() >= version.VERSION_4_11:
        DEFAULT_STORAGECLASS = "managed-csi"
    else:
        DEFAULT_STORAGECLASS = "managed-premium"

    def __init__(self):
        super(AZUREBase, self).__init__()
        self.azure_util = AzureUtil()

    def add_node(self):
        # TODO: implement later
        super(AZUREBase, self).add_node()

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence based on a cluster name prefix.

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise
        """
        logger.info("checking existence of cluster with prefix %s", cluster_name_prefix)
        # ask about all azure resource groups, and filter it by cluster name
        # prefix (there is azure resource group for each cluster, which
        # contains all other azure resources of the cluster)
        resource_groups = self.azure_util.resource_client.resource_groups.list()
        for rg in resource_groups:
            if rg.name.startswith(cluster_name_prefix):
                logger.info(
                    "For given cluster name prefix %s, there is a resource group %s already.",
                    cluster_name_prefix,
                    rg.name,
                )
                return True
        logger.info(
            "For given cluster name prefix %s, there is no resource group.",
            cluster_name_prefix,
        )
        return False


class AZUREIPI(AZUREBase):
    """
    A class to handle Azure IPI specific deployment.
    """

    OCPDeployment = IPIOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(AZUREIPI, self).__init__()

    # For Azure IPI there is no need to implement custom:
    # - deploy_ocp() method (as long as we don't tweak host network)


class AzureCloudAroOCPDeployment(BaseOCPDeployment):
    """
    Azure ARO Managed deployment class.

    """

    def __init__(self):
        super(AzureCloudAroOCPDeployment, self).__init__()
        self.azure_util = AzureAroUtil()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for Azure ARO cloud deployment.
        """
        super(AzureCloudAroOCPDeployment, self).deploy_prereq()

    def deploy(self, log_level=""):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level

        """
        # TODO: Add log level to az command

        self.azure_util.create_cluster(self.cluster_name)
        self.test_cluster()

    def destroy(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        # TODO: Add log level to az command
        self.azure_util.destroy_cluster(
            self.cluster_name,
            config.ENV_DATA["azure_base_domain_resource_group_name"],
        )


class AZUREAroManaged(AZUREBase):
    """
    Deployment class for Azure Aro
    """

    OCPDeployment = AzureCloudAroOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        self.azure_util = AzureAroUtil()
        super(AZUREAroManaged, self).__init__()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        super(AZUREAroManaged, self).deploy_ocp(log_cli_level)

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
            "checking existence of Azure cluster with prefix %s",
            cluster_name_prefix,
        )
        data = json.loads(exec_cmd("az aro list -o json").stdout)
        for cluster in data:
            if cluster_name_prefix in cluster["name"]:
                return True
        return False
