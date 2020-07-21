# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Azure platform.
"""

import logging

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.cloud import CloudIPIOCPDeployment
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

    def __init__(self):
        super(AZUREBase, self).__init__()
        self.azure_util = AzureUtil()

    def add_node(self):
        # TODO: implement later
        super(AZUREBase, self).add_node()

    def check_cluster_existence(self, cluster_name_prefix):
        # TODO: implement now
        pass


class AZUREIPI(AZUREBase):
    """
    A class to handle Azure IPI specific deployment.
    """

    OCPDeployment = CloudIPIOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(AZUREIPI, self).__init__()

    # For Azure IPI there is no need to implement custom:
    # - deploy_ocp() method (as long as we don't tweak host network)
