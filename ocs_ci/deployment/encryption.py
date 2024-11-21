"""
This module provides functions for encryption configuration during deployment
"""

import logging

from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def add_in_transit_encryption_to_cluster_data(cluster_data):
    """
    Update storage cluster YAML data with in-transit encryption configuration if required.

    Args:
        cluster_data (dict): storage cluster YAML data

    Returns:
        dict: updated storage storage cluster yaml
    """
    if config.ENV_DATA.get("in_transit_encryption"):
        logger.info("Configuring in-transit encryption for the storage cluster")
        if "network" not in cluster_data["spec"]:
            cluster_data["spec"]["network"] = {}

        if "connections" not in cluster_data["spec"]["network"]:
            cluster_data["spec"]["network"]["connections"] = {}

        cluster_data["spec"]["network"]["connections"] = {
            "encryption": {"enabled": True}
        }
    return cluster_data


def add_encryption_details_to_cluster_data(cluster_data):
    """
    Update storage cluster YAML data with encryption information from
    configuration.

    Args:
        cluster_data (dict): storage cluster YAML data

    Returns:
        dict: updated storage storage cluster yaml
    """
    if config.ENV_DATA.get("encryption_at_rest"):
        logger.info("Enabling encryption at REST!")
        cluster_data["spec"]["encryption"] = {
            "enable": True,
        }
        cluster_data["spec"]["encryption"] = {
            "clusterWide": True,
        }
    if config.DEPLOYMENT.get("kms_deployment"):
        cluster_data["spec"]["encryption"]["kms"] = {
            "enable": True,
        }
    return cluster_data
