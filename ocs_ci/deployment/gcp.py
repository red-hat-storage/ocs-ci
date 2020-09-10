# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Google Cloud Platform (aka GCP).
"""

import logging

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.cloud import IPIOCPDeployment
# TODO: import GCP util


logger = logging.getLogger(__name__)


__all__ = ['GCPIPI']


class GCPBase(CloudDeploymentBase):
    """
    Google Cloud deployment base class, with code common to both IPI and UPI.

    Having this base class separate from GCPIPI even when we have implemented
    IPI only makes adding UPI class later easier, moreover code structure is
    comparable with other platforms.
    """

    def __init__(self):
        super(GCPBase, self).__init__()
        # TODO: self.gcp_util = GCPUtil()

    def add_node(self):
        # TODO: implement later
        super(GCPBase, self).add_node()

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence based on a cluster name prefix.

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        # TODO: actual check is happening here
        return False


class GCPIPI(GCPBase):
    """
    A class to handle GCP IPI specific deployment
    """

    # default storage class for StorageCluster CRD on Google Cloud platform
    DEFAULT_STORAGECLASS = "managed-premium"

    OCPDeployment = IPIOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(GCPIPI, self).__init__()
