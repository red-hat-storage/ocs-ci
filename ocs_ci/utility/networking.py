"""
Module that contains network related functions
"""

import base64
import ipaddress
import logging
import os
import yaml
import re

from ocs_ci.framework import config
from ocs_ci.utility.templating import load_yaml
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.exceptions import (
    UnavailableResourceException,
    CommandFailed,
)


logger = logging.getLogger(__name__)


def get_excluded_network_cidrs():
    """
    Get network CIDR ranges that should be excluded when selecting node private IPs.

    This includes:
    - clusterNetwork CIDRs from the OCP Network resource (pod network)
    - serviceNetwork CIDRs from the OCP Network resource (service network)

    Returns:
        list: List of ipaddress.IPv4Network objects representing excluded network CIDRs

    Raises:
        UnavailableResourceException: If unable to retrieve Network resource
    """
    try:
        network_obj = OCP(kind="network.config.openshift.io", resource_name="cluster")
        network_data = network_obj.get()

        cidrs = []

        # Get clusterNetwork CIDRs
        cluster_networks = network_data.get("spec", {}).get("clusterNetwork", [])
        for network in cluster_networks:
            cidr_str = network.get("cidr")
            if cidr_str:
                try:
                    cidrs.append(ipaddress.IPv4Network(cidr_str))
                    logger.info(f"Found cluster network CIDR to exclude: {cidr_str}")
                except (ValueError, ipaddress.AddressValueError) as e:
                    logger.warning(f"Invalid CIDR format '{cidr_str}': {e}")
                    continue

        # Get serviceNetwork CIDRs
        service_networks = network_data.get("spec", {}).get("serviceNetwork", [])
        for cidr_str in service_networks:
            if cidr_str:
                try:
                    cidrs.append(ipaddress.IPv4Network(cidr_str))
                    logger.info(f"Found service network CIDR to exclude: {cidr_str}")
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


def is_ip_in_excluded_networks(ip_addr, excluded_cidrs):
    """
    Check if an IP address falls within any of the excluded network CIDR ranges.

    Args:
        ip_addr (str): IP address to check
        excluded_cidrs (list): List of ipaddress.IPv4Network objects to check against

    Returns:
        bool: True if IP is in any excluded network CIDR, False otherwise
    """
    try:
        ip = ipaddress.IPv4Address(ip_addr)
        for cidr in excluded_cidrs:
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
    the first globally routable IPv4 address that is not in the excluded networks
    (cluster network or service network). Link-local and other non-routable addresses
    are automatically filtered by selecting only addresses with "scope global".

    Args:
        node_name (str): Name of the node

    Returns:
        tuple: (interface_name (str), private_ip (str), prefix_length (str)) -
               Interface name, IP address and network prefix length

    Raises:
        CommandFailed: If unable to retrieve IP address from the node
        UnavailableResourceException: If unable to retrieve Network resource
    """
    nodes_obj = OCP(kind="node")

    # Get network CIDRs to exclude (cluster network, service network)
    excluded_cidrs = get_excluded_network_cidrs()

    # Run 'ip -o addr show' to get all IP addresses in one-line format
    # First try to get IPv4 addresses with "scope global" to exclude link-local and other non-routable addresses
    # Output format: 2: eth0    inet 192.168.1.10/24 ...
    cmd_global = (
        "ip -o addr show | grep 'inet ' | grep 'scope global' | awk '{print $2, $4}'"
    )
    cmd_all = (
        "ip -o addr show | grep 'inet ' | grep -v '127.0.0.1' | awk '{print $2, $4}'"
    )

    try:
        output = nodes_obj.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd_global])
        output = output.strip()

        # If no globally-scoped addresses found, fall back to all non-loopback addresses
        if not output:
            logger.info(
                f"No globally-scoped IP addresses found on node {node_name}, "
                f"falling back to all non-loopback addresses"
            )
            output = nodes_obj.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd_all])
            output = output.strip()
    except CommandFailed as e:
        logger.error(f"Failed to execute debug command on node {node_name}: {e}")
        raise

    if not output:
        raise CommandFailed(f"No IP addresses found on node {node_name}")

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
            f"Unable to parse any IP addresses from node {node_name}. "
            f"Command output: {output}"
        )

    # Find first IP that is not in excluded networks
    for interface, ip_addr, prefix in ip_addresses:
        # Additional safety check: skip link-local addresses using ipaddress module
        try:
            ip_obj = ipaddress.IPv4Address(ip_addr)
            if ip_obj.is_link_local:
                logger.debug(
                    f"Skipping link-local address {ip_addr} on interface {interface}"
                )
                continue
        except (ValueError, ipaddress.AddressValueError):
            logger.warning(f"Invalid IP address format: {ip_addr}")
            continue

        if not is_ip_in_excluded_networks(ip_addr, excluded_cidrs):
            logger.info(
                f"Retrieved private IP {ip_addr}/{prefix} on interface {interface} "
                f"from node {node_name} (excluded cluster/service/link-local networks)"
            )
            return interface, ip_addr, prefix

    # If all IPs are in excluded networks, raise an error
    cidr_strings = [str(cidr) for cidr in excluded_cidrs]
    raise CommandFailed(
        f"No valid private IP found on node {node_name}. "
        f"All globally-scoped IP addresses are in excluded network CIDRs: {cidr_strings}. "
        f"Found IPs: {[f'{ip[1]}/{ip[2]} on {ip[0]}' for ip in ip_addresses]}"
    )


def annotate_worker_nodes_with_mon_ip():
    """
    Annotate worker nodes with annotation 'network.rook.io/mon-ip: <IPAddress>'

    This function retrieves the private IP address from each worker node using
    the 'ip addr' command and annotates the node with that IP address.
    Master nodes are also included to ensure all nodes hosting OCS/ODF components
    receive the annotation.
    """
    nodes_obj = OCP(kind="node")
    worker_nodes = get_worker_nodes(skip_master_nodes=False)
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
        worker_nodes = get_worker_nodes()
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


def create_drs_machine_config():
    """
    Create Machine Config that moves the second physical network to a bridge.
    This is done for HCP configuraion of data replication separation.

    If the MachineConfig '99-br-storage-nmstate-worker' already exists,
    this function will skip creation and return early.
    """
    from ocs_ci.ocs.resources.machineconfig import machineconfig_exists

    mc_name = "99-br-storage-nmstate-worker"
    if machineconfig_exists(mc_name):
        logger.info("MachineConfig %s already exists, skipping creation", mc_name)
        return

    interfaces_path = os.path.join(
        constants.TEMPLATE_DEPLOYMENT_DIR, "drs_interfaces.yaml"
    )
    interfaces_yaml = load_yaml(interfaces_path)
    worker = get_worker_nodes()[0]
    interface_name, _, _ = get_node_private_ip(worker)
    interfaces_yaml["interfaces"][0]["bridge"]["port"][0]["name"] = interface_name
    interfaces_yaml["interfaces"][1]["name"] = interface_name
    interfaces_yaml_string = yaml.dump(interfaces_yaml)
    base64_interfaces = base64.b64encode(interfaces_yaml_string.encode()).decode()
    machineconfigurations_path = os.path.join(
        constants.TEMPLATE_DEPLOYMENT_DIR, "drs_machineconfig.yaml"
    )
    machineconfigurations_yaml = load_yaml(machineconfigurations_path)
    machineconfigurations_yaml["spec"]["config"]["storage"]["files"][0]["contents"][
        "source"
    ] = f"data:text/plain;base64,{base64_interfaces}"
    with config.RunWithProviderConfigContextIfAvailable():
        machineconfigurations_obj = OCS(**machineconfigurations_yaml)
        machineconfigurations_obj.apply(**machineconfigurations_yaml)


def create_drs_nad(cluster_name):
    """
    Create NetworkAttachmentDefinition in namespace where the virt-launcher pods exist.
    This is done for HCP configuraion of data replication separation.

    The namespace is constructed as f"clusters-{cluster_name}". If the namespace
    doesn't exist, it will be created automatically.

    Args:
        cluster_name (str): cluster name used to construct the namespace on provider cluster
    """
    namespace = f"clusters-{cluster_name}"

    # Check if namespace exists, create if it doesn't
    ocp_ns = OCP(kind="namespace")
    if not ocp_ns.is_exist(resource_name=namespace):
        logger.info("Namespace %s does not exist, creating it", namespace)
        ocp_ns.new_project(namespace)
    else:
        logger.info("Namespace %s already exists", namespace)

    nad_path = os.path.join(constants.TEMPLATE_DEPLOYMENT_DIR, "drs_nad.yaml")
    nad_yaml = load_yaml(nad_path)
    nad_yaml["metadata"]["namespace"] = namespace
    with config.RunWithProviderConfigContextIfAvailable():
        nad_obj = OCS(**nad_yaml)
        nad_obj.apply(**nad_yaml)
