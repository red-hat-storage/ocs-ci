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


def get_network_interface_by_ip(node, ip):
    """
    Get interface name from a node that has provided ip address liste.

    Args:
        node (str): node name
        ip (str): IP address

    Returns:
        str: name of the interface
    """
    oc_obj = OCP(kind="node")
    network_info = oc_obj.exec_oc_debug_cmd(node, cmd_list=["ip -br -4 a sh"])
    interface_info = [line for line in network_info.split("\n") if ip in line][
        0
    ].split()
    interface_name = interface_info[0]
    return interface_name


def create_drs_machine_config():
    """
    Create Machine Config that moves the second physical network to a bridge.
    This is done for HCP configuraion of data replication separation.
    """
    interfaces_path = os.path.join(
        constants.TEMPLATE_DEPLOYMENT_DIR, "drs_interfaces.yaml"
    )
    interfaces_yaml = load_yaml(interfaces_path)
    worker = get_worker_nodes()[0]
    private_ip, _ = get_node_private_ip(worker)
    interface_name = get_network_interface_by_ip(worker, private_ip)
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


def create_drs_nad(namespace):
    """
    Create NetworkAttachmentDefinition in namespace where the virt-launcher pods exist.
    This is done for HCP configuraion of data replication separation.

    Args:
        namespace (str): namespace on provider cluster where virt-launcher pods exist
    """
    nad_path = os.path.join(constants.TEMPLATE_DEPLOYMENT_DIR, "drs_nad.yaml")
    nad_yaml = load_yaml(nad_path)
    nad_yaml["metadata"]["namespace"] = namespace
    with config.RunWithProviderConfigContextIfAvailable():
        nad_obj = OCS(**nad_yaml)
        nad_obj.apply(**nad_yaml)
