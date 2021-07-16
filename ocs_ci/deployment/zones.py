# -*- coding: utf8 -*-


import logging

from ocs_ci.framework import config
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.constants import ZONE_LABEL
from ocs_ci.ocs.node import get_master_nodes, get_worker_nodes
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


def are_zone_labels_missing():
    """
    Check that there are no nodes with zone labels.

    Returns:
        Bool: True if all nodes are missing zone label, False otherwise.
    """
    node_h = OCP(kind="node")
    nodes = node_h.get(selector=ZONE_LABEL)
    node_names = [n["metadata"]["name"] for n in nodes["items"]]
    logger.info("nodes with '%s' zone label: %s", ZONE_LABEL, node_names)
    return len(nodes["items"]) == 0


def are_zone_labels_present():
    """
    Check that there are no nodes without zone labels.

    Returns:
        Bool: True if all nodes have a zone label, False otherwise.
    """
    node_h = OCP(kind="node")
    nodes_all = node_h.get()
    nodes_labeled = node_h.get(selector=ZONE_LABEL)
    node_names = [n["metadata"]["name"] for n in nodes_labeled["items"]]
    logger.info("nodes with '%s' zone label: %s", ZONE_LABEL, node_names)
    return len(nodes_labeled["items"]) == len(nodes_all["items"])


def assign_dummy_zones(zones, nodes, overwrite=False):
    """
    Assign node labels to given nodes based on given zone lists. Zones
    are assigned so that there is the same number of nodes in each zone.

    Args:
        zones (list[str]): list of k8s zone names
        nodes (list[str]): list of node names to label
        overwrite (bool): if True, labeling will not fail on already defined
            zone labels (False by default)

    Raises:
      ValueError: when number of nodes is not divisible by number of zones
    """
    if len(nodes) % len(zones) != 0:
        msg = "number of nodes is not divisible by number of zones"
        logger.error(msg)
        raise ValueError(msg)
    nodes_per_zone = int(len(nodes) / len(zones))
    node_h = OCP(kind="node")
    for node, zone in zip(nodes, zones * nodes_per_zone):
        logger.info("labeling node %s with %s=%s", node, ZONE_LABEL, zone)
        oc_cmd = f"label node {node} {ZONE_LABEL}={zone}"
        if overwrite:
            oc_cmd += " --overwrite"
        node_h.exec_oc_cmd(command=oc_cmd)


def create_dummy_zone_labels():
    """
    Create dummy zone labels on cluster nodes: try to label all master and
    worker nodes based on values of ``worker_availability_zones`` and
    ``master_availability_zones`` options, but only if there are no zone
    labels already defined.

    Raises:
        UnexpectedDeploymentConfiguration: when either cluster or ocs-ci config
            file are in conflict with dummy zone labels.
    """
    logger.info("trying to setup dummy_zone_node_labels")
    if are_zone_labels_missing():
        to_label = [
            ("master_availability_zones", get_master_nodes()),
            ("worker_availability_zones", get_worker_nodes()),
        ]
        for zone_opt, nodes in to_label:
            zones = config.ENV_DATA.get(zone_opt)
            if zones is None:
                msg = f"{zone_opt} is not defined in ENV_DATA conf"
                logger.error(msg)
                raise exceptions.UnexpectedDeploymentConfiguration(msg)
            assign_dummy_zones(zones, nodes)
    else:
        # don't use dummy zone labeling on a cluster with actuall zones
        msg = (
            "Cluster in unexpected state before dummy zone labeling: "
            "at least one node already have a zone label."
        )
        logger.error(msg)
        raise exceptions.UnexpectedDeploymentConfiguration(msg)
