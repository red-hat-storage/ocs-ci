# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Google Cloud Platform (aka GCP).
"""

import logging

from libcloud.compute.types import NodeState

from ocs_ci.deployment.cloud import CloudDeploymentBase, IPIOCPDeployment
from ocs_ci.utility.gcp import GoogleCloudUtil


logger = logging.getLogger(__name__)


__all__ = ["GCPIPI"]


class GCPBase(CloudDeploymentBase):
    """
    Google Cloud deployment base class, with code common to both IPI and UPI.

    Having this base class separate from GCPIPI even when we have implemented
    IPI only makes adding UPI class later easier, moreover code structure is
    comparable with other platforms.
    """

    def __init__(self):
        super(GCPBase, self).__init__()
        self.util = GoogleCloudUtil()

    def add_node(self):
        # TODO: implement later
        super(GCPBase, self).add_node()

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
            "checking existence of GCP cluster with prefix %s", cluster_name_prefix
        )
        non_term_cluster_nodes = []
        for node in self.util.compute_driver.list_nodes():
            if (
                node.name.startswith(cluster_name_prefix)
                and node.state != NodeState.TERMINATED
            ):
                non_term_cluster_nodes.append(node)
        if len(non_term_cluster_nodes) > 0:
            logger.warning(
                "Non terminated nodes with the same name prefix were found: %s",
                non_term_cluster_nodes,
            )
            return True
        return False


class GCPIPI(GCPBase):
    """
    A class to handle GCP IPI specific deployment
    """

    OCPDeployment = IPIOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(GCPIPI, self).__init__()
