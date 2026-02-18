"""
Module that contains network related functions
"""

import ipaddress
import logging
import re

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    UnavailableResourceException,
    CommandFailed,
)


logger = logging.getLogger(__name__)


def get_node_private_ip(node_name):
    """
    Get the private IP address of a node using ip addr command.

    This function executes 'ip addr' command on the node to retrieve
    the first non-loopback IPv4 address, which is typically the private IP.

    Args:
        node_name (str): Name of the node

    Returns:
        tuple: (private_ip (str), prefix_length (str)) - IP address and network prefix length

    Raises:
        CommandFailed: If unable to retrieve IP address from the node
    """
    nodes_obj = OCP(kind="node")

    # Run 'ip -o addr show' to get all IP addresses in one-line format
    # Filter for IPv4 addresses (inet), exclude loopback (127.0.0.1)
    cmd = "ip -o addr show | grep 'inet ' | grep -v '127.0.0.1' | head -1 | awk '{print $4}'"

    try:
        output = nodes_obj.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd])
        # Output should be in format: "IP/PREFIX" e.g., "192.168.1.10/24"
        output = output.strip()

        if not output or "/" not in output:
            raise CommandFailed(
                f"Unable to get valid IP address from node {node_name}. Output: {output}"
            )

        # Parse IP and prefix
        ip_with_prefix = output.splitlines()[
            -1
        ].strip()  # Get last line in case of multiple lines
        match = re.search(r"(\d+\.\d+\.\d+\.\d+)/(\d+)", ip_with_prefix)

        if not match:
            raise CommandFailed(
                f"Unable to parse IP address from node {node_name}. Output: {output}"
            )

        private_ip = match.group(1)
        prefix_length = match.group(2)

        logger.info(
            f"Retrieved private IP {private_ip}/{prefix_length} from node {node_name}"
        )
        return private_ip, prefix_length

    except Exception as e:
        logger.error(f"Failed to get private IP from node {node_name}: {e}")
        raise


def annotate_worker_nodes_with_mon_ip():
    """
    Annotate worker nodes with annotation 'network.rook.io/mon-ip: <IPAddress>'

    This function retrieves the private IP address from each worker node using
    the 'ip addr' command and annotates the node with that IP address.
    """
    nodes_obj = OCP(kind="node")
    nodes = nodes_obj.get().get("items", [])
    worker_nodes = [
        node["metadata"]["name"]
        for node in nodes
        if constants.WORKER_LABEL in node["metadata"]["labels"]
    ]
    if not worker_nodes:
        raise UnavailableResourceException("No worker node found!")

    for worker in worker_nodes:
        # Get private IP from the node using ip addr command
        private_ip, _ = get_node_private_ip(worker)

        annotate_cmd = (
            f"annotate node {worker} "
            f"network.rook.io/mon-ip={private_ip} --overwrite"
        )

        logger.info(
            f"Annotating node {worker} with network.rook.io/mon-ip={private_ip}"
        )
        nodes_obj.exec_oc_cmd(command=annotate_cmd)


def add_data_replication_separation_to_cluster_data(cluster_data):
    """
    Update storage cluster YAML data with data replication separation
    if required.

    This function retrieves the private IP and network information from
    a worker node using the 'ip addr' command and configures the storage
    cluster's public network address range accordingly.

    Args:
        cluster_data (dict): storage cluster YAML data

    Returns:
        dict: updated storage storage cluster yaml
    """
    if config.DEPLOYMENT.get("enable_data_replication_separation"):
        nodes = OCP(kind="node").get().get("items", [])
        worker_nodes = [
            node["metadata"]["name"]
            for node in nodes
            if constants.WORKER_LABEL in node["metadata"]["labels"]
        ]
        if not worker_nodes:
            raise UnavailableResourceException("No worker node found!")

        # Get private IP and prefix from the first worker node using ip addr command
        private_ip, prefix_length = get_node_private_ip(worker_nodes[0])

        ip_network = ipaddress.IPv4Network(
            f"{private_ip}/{prefix_length}",
            strict=False,
        )
        str_network = f"{ip_network.network_address}/{ip_network.prefixlen}"

        logger.info(
            f"Configuring data replication separation for the storage cluster "
            f"with network range: {str_network}"
        )

        if "network" not in cluster_data["spec"]:
            cluster_data["spec"]["network"] = {}
        if "addressRanges" not in cluster_data["spec"]["network"]:
            cluster_data["spec"]["network"]["addressRanges"] = {}
        if "public" not in cluster_data["spec"]["network"]["addressRanges"]:
            cluster_data["spec"]["network"]["addressRanges"]["public"] = []

        cluster_data["spec"]["network"]["addressRanges"]["public"] = [str_network]
    return cluster_data
