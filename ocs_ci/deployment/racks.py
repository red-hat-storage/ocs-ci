# -*- coding: utf8 -*-

import logging

from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)

MIN_WORKERS_FOR_UNIQUE_RACKS = 5


def assign_unique_rack_labels(nodes, overwrite=True):
    """
    Assign a unique topology.rook.io/rack label to each given node.

    Args:
        nodes (list[str]): Node names to label
        overwrite (bool): If True, overwrite existing rack labels

    """
    node_h = OCP(kind="node")
    for index, node in enumerate(nodes):
        rack = f"rack{index}"
        logger.info("labeling node %s with %s=%s", node, constants.RACK_LABEL, rack)
        oc_cmd = f"label node {node} {constants.RACK_LABEL}={rack}"
        if overwrite:
            oc_cmd += " --overwrite"
        node_h.exec_oc_cmd(command=oc_cmd)


def create_unique_rack_labels():
    """
    Label each worker node with a unique rack for 5-MON deployments.

    Raises:
        UnexpectedDeploymentConfiguration: If fewer than 5 worker nodes exist

    """
    logger.info("trying to setup unique_rack_node_labels")
    workers = get_worker_nodes()
    if len(workers) < MIN_WORKERS_FOR_UNIQUE_RACKS:
        msg = (
            f"unique_rack_node_labels requires at least "
            f"{MIN_WORKERS_FOR_UNIQUE_RACKS} worker nodes, found {len(workers)}"
        )
        logger.error(msg)
        raise exceptions.UnexpectedDeploymentConfiguration(msg)
    assign_unique_rack_labels(workers)
