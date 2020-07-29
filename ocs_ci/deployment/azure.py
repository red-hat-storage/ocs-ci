# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Azure platform.
"""

import logging

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.cloud import IPIOCPDeployment
from ocs_ci.utility.azure_utils import AZURE as AzureUtil


logger = logging.getLogger(__name__)


__all__ = ['AZUREIPI']


class AZUREBase(CloudDeploymentBase):
    """
    Azure deployment base class, with code common to both IPI and UPI.

    Having this base class separate from AZUREIPI even when we have implemented
    IPI only makes adding UPI class later easier, moreover code structure is
    comparable with other platforms.
    """

    # default storage class for StorageCluster CRD on Azure platform
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
        logger.info(
            "checking existence of cluster with prefix %s", cluster_name_prefix)
        # ask about all azure resource groups, and filter it by cluster name
        # prefix (there is azure resource group for each cluster, which
        # contains all other azure resources of the cluster)
        resource_groups = self.azure_util.resource_client.resource_groups.list()
        for rg in resource_groups:
            if rg.name.startswith(cluster_name_prefix):
                logger.info(
                    "For given cluster name prefix %s, there is a resource group %s already.",
                    cluster_name_prefix,
                    rg.name)
                return True
        logger.info(
            "For given cluster name prefix %s, there is no resource group.",
            cluster_name_prefix
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
