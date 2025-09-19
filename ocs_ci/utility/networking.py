"""
Module that contains network related functions
"""

import ipaddress
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    DeploymentPlatformNotSupported,
    UnavailableResourceException,
)


logger = logging.getLogger(__name__)


def label_worker_nodes_with_mon_ip(self):
    """
    Label worker nodes with label 'network.rook.io/mon-ip: <IPAddress>'
    """
    if not config.ENV_DATA["platform"].lower() in constants.BAREMETAL_PLATFORMS:
        raise DeploymentPlatformNotSupported(
            "Labeling nodes with mon ip is not implemented for current platform"
        )

    nodes = OCP(kind="node").get().get("items", [])
    worker_nodes = [
        node for node in nodes if constants.WORKER_LABEL in node["metadata"]["labels"]
    ]
    if not worker_nodes:
        raise UnavailableResourceException("No worker node found!")
    for worker in worker_nodes.keys():
        network_data = (
            config.ENV_DATA.get("baremetal", {}).get("servers", {}).get(worker)
        )
        label_cmd = [
            (
                f"label nodes {worker} "
                f"network.rook.io/mon-ip: \"{network_data['private_ip']}\" --overwrite"
            )
        ]

        nodes.exec_oc_cmd(command=label_cmd)


def add_data_replication_separation_to_cluster_data(cluster_data):
    """
    Update storage cluster YAML data with data replication separation
    if required.

    Args:
        cluster_data (dict): storage cluster YAML data

    Returns:
        dict: updated storage storage cluster yaml
    """
    if config.ENV_DATA.get("enable_data_separation_replication"):
        nodes = OCP(kind="node").get().get("items", [])
        worker_nodes = [
            node
            for node in nodes
            if constants.WORKER_LABEL in node["metadata"]["labels"]
        ]
        network_data = (
            config.ENV_DATA.get("baremetal", {}).get("servers", {}).get(worker_nodes[0])
        )
        ip_network = ipaddress.IPv4Network(
            f"{network_data['private_ip']}/{network_data['private_ip']}", strict=False
        )
        str_network = f"{ip_network.network_address}/{ip_network.prefixlen}"
        logger.info("Configuring data replication separation for the storage cluster")
        if "network" not in cluster_data["spec"]:
            cluster_data["spec"]["network"] = {}
        if "addressRanges" not in cluster_data["spec"]["network"]:
            cluster_data["spec"]["network"]["addressRanges"] = {}
        if "public" not in cluster_data["spec"]["network"]["addressRanges"]:
            cluster_data["spec"]["network"]["addressRanges"]["public"] = []

        cluster_data["spec"]["network"]["addressRanges"]["public"] = [str_network]
        return cluster_data
