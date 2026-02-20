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


def get_cluster_network_cidrs():
    """
    Get the clusterNetwork CIDR ranges from the OCP Network resource.

    Returns:
        list: List of ipaddress.IPv4Network objects representing cluster network CIDRs

    Raises:
        UnavailableResourceException: If unable to retrieve Network resource
    """
    try:
        network_obj = OCP(kind="network.config.openshift.io", resource_name="cluster")
        network_data = network_obj.get()

        cluster_networks = network_data.get("spec", {}).get("clusterNetwork", [])
        cidrs = []

        for network in cluster_networks:
            cidr_str = network.get("cidr")
            if cidr_str:
                try:
                    cidrs.append(ipaddress.IPv4Network(cidr_str))
                    logger.info(f"Found cluster network CIDR: {cidr_str}")
                except (ValueError, ipaddress.AddressValueError) as e:
                    logger.warning(f"Invalid CIDR format '{cidr_str}': {e}")
                    continue

        return cidrs

    except (KeyError, AttributeError, TypeError) as e:
        logger.error(f"Failed to parse cluster network configuration: {e}")
        raise UnavailableResourceException(
            f"Unable to parse cluster network configuration: {e}"
        )
    except CommandFailed as e:
        logger.error(f"Failed to retrieve Network resource: {e}")
        raise UnavailableResourceException(
            f"Unable to retrieve Network resource 'cluster': {e}"
        )


def is_ip_in_cluster_network(ip_addr, cluster_cidrs):
    """
    Check if an IP address falls within any of the cluster network CIDR ranges.

    Args:
        ip_addr (str): IP address to check
        cluster_cidrs (list): List of ipaddress.IPv4Network objects

    Returns:
        bool: True if IP is in any cluster network CIDR, False otherwise
    """
    try:
        ip = ipaddress.IPv4Address(ip_addr)
        for cidr in cluster_cidrs:
            if ip in cidr:
                return True
        return False
    except (ValueError, ipaddress.AddressValueError) as e:
        logger.warning(f"Invalid IP address format '{ip_addr}': {e}")
        return False


def get_node_private_ip(node_name):
    """
    Get the private IP address of a node using ip addr command.

    This function executes 'ip addr' command on the node to retrieve
    the first non-loopback IPv4 address that is not in the cluster network CIDR ranges.

    Args:
        node_name (str): Name of the node

    Returns:
        tuple: (interface_name (str), private_ip (str), prefix_length (str)) -
               Interface name, IP address and network prefix length

    Raises:
        CommandFailed: If unable to retrieve IP address from the node
    """
    nodes_obj = OCP(kind="node")

    # Get cluster network CIDRs to exclude
    cluster_cidrs = get_cluster_network_cidrs()

    # Run 'ip -o addr show' to get all IP addresses in one-line format
    # Filter for IPv4 addresses (inet), exclude loopback (127.0.0.1)
    # Output format: 2: eth0    inet 192.168.1.10/24 ...
    cmd = "ip -o addr show | grep 'inet ' | grep -v '127.0.0.1' | awk '{print $2, $4}'"

    try:
        output = nodes_obj.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd])
    except CommandFailed as e:
        logger.error(f"Failed to execute debug command on node {node_name}: {e}")
        raise

    output = output.strip()

    if not output:
        raise CommandFailed(
            f"Unable to get IP addresses from node {node_name}. Output: {output}"
        )

    # Parse all IP addresses from output
    # Expected format per line: "interface_name ip/prefix"
    ip_addresses = []
    for line in output.splitlines():
        line = line.strip()
        # Match: interface_name followed by IP/prefix
        match = re.search(r"^(\S+)\s+(\d+\.\d+\.\d+\.\d+)/(\d+)", line)
        if match:
            interface = match.group(1)
            ip_addr = match.group(2)
            prefix = match.group(3)
            ip_addresses.append((interface, ip_addr, prefix))

    if not ip_addresses:
        raise CommandFailed(
            f"Unable to parse any IP addresses from node {node_name}. Output: {output}"
        )

    # Find first IP that is not in cluster network
    for interface, ip_addr, prefix in ip_addresses:
        if not is_ip_in_cluster_network(ip_addr, cluster_cidrs):
            logger.info(
                f"Retrieved private IP {ip_addr}/{prefix} on interface {interface} "
                f"from node {node_name} (excluded cluster network IPs)"
            )
            return interface, ip_addr, prefix

    # If all IPs are in cluster network, raise an error
    cluster_cidr_strings = [str(cidr) for cidr in cluster_cidrs]
    raise CommandFailed(
        f"No valid private IP found on node {node_name}. "
        f"All non-loopback IP addresses are in cluster network CIDRs: {cluster_cidr_strings}. "
        f"Found IPs: {[f'{ip[1]}/{ip[2]} on {ip[0]}' for ip in ip_addresses]}"
    )


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
        _, private_ip, _ = get_node_private_ip(worker)

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
        interface, private_ip, prefix_length = get_node_private_ip(worker_nodes[0])

        ip_network = ipaddress.IPv4Network(
            f"{private_ip}/{prefix_length}",
            strict=False,
        )
        str_network = f"{ip_network.network_address}/{ip_network.prefixlen}"

        logger.info(
            f"Configuring data replication separation for the storage cluster "
            f"with network range: {str_network} (interface: {interface})"
        )

        if "network" not in cluster_data["spec"]:
            cluster_data["spec"]["network"] = {}
        if "addressRanges" not in cluster_data["spec"]["network"]:
            cluster_data["spec"]["network"]["addressRanges"] = {}
        if "public" not in cluster_data["spec"]["network"]["addressRanges"]:
            cluster_data["spec"]["network"]["addressRanges"]["public"] = []

        cluster_data["spec"]["network"]["addressRanges"]["public"] = [str_network]
    return cluster_data
