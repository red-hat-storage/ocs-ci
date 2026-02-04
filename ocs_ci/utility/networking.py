"""
Module that contains network related functions
"""

import ipaddress
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    UnexpectedDeploymentConfiguration,
    UnavailableResourceException,
)


logger = logging.getLogger(__name__)


def annotate_worker_nodes_with_mon_ip():
    """
    Annotate worker nodes with annotation 'network.rook.io/mon-ip: <IPAddress>'
    """
    if not config.ENV_DATA["platform"].lower() in constants.BAREMETAL_PLATFORMS:
        raise UnexpectedDeploymentConfiguration(
            "Annotating nodes with mon ip is not implemented for current platform"
        )

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
        network_data = (
            config.ENV_DATA.get("baremetal", {}).get("servers", {}).get(worker)
        )
        annotate_cmd = (
            f"annotate node {worker} "
            f"network.rook.io/mon-ip={network_data['private_ip']} --overwrite"
        )

        nodes_obj.exec_oc_cmd(command=annotate_cmd)


def add_data_replication_separation_to_cluster_data(cluster_data):
    """
    Update storage cluster YAML data with data replication separation
    if required.

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
        network_data = (
            config.ENV_DATA.get("baremetal", {}).get("servers", {}).get(worker_nodes[0])
        )
        ip_network = ipaddress.IPv4Network(
            f"{network_data['private_ip']}/{network_data['private_prefix_length']}",
            strict=False,
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
    interface_info = [line for line in network_info.split('\n') if ip in line][0].split()
    interface_name = interface_info[0]
    return interface_name


def create_drs_machine_config():
    """
    Create Machine Config that moves the second physical network to a bridge.
    This is done for HCP configuraion of data replication separation.
    """
    pass
