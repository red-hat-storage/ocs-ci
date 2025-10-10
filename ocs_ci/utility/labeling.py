import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import UnavailableResourceException
from ocs_ci.ocs.node import get_all_nodes, get_node_objs, get_nodes, label_nodes

logger = logging.getLogger(__name__)


def label_and_taint_nodes():
    """
    Label and taint worker nodes to be used by OCS operator
    """

    # TODO: remove this "heuristics", it doesn't belong there, the process
    # should be explicit and simple, this is asking for trouble, bugs and
    # silently invalid deployments ...
    # See https://github.com/red-hat-storage/ocs-ci/issues/4470
    arbiter_deployment = config.DEPLOYMENT.get("arbiter_deployment")

    nodes = ocp.OCP(kind="node").get().get("items", [])

    worker_nodes = [
        node for node in nodes if constants.WORKER_LABEL in node["metadata"]["labels"]
    ]
    if not worker_nodes:
        raise UnavailableResourceException("No worker node found!")
    az_worker_nodes = {}
    for node in worker_nodes:
        az = node["metadata"]["labels"].get(constants.ZONE_LABEL)
        az_node_list = az_worker_nodes.get(az, [])
        az_node_list.append(node["metadata"]["name"])
        az_worker_nodes[az] = az_node_list
    logger.debug(f"Found the worker nodes in AZ: {az_worker_nodes}")

    if arbiter_deployment:
        to_label = config.DEPLOYMENT.get("ocs_operator_nodes_to_label", 4)
    else:
        to_label = config.DEPLOYMENT.get("ocs_operator_nodes_to_label")

    distributed_worker_nodes = []
    if arbiter_deployment and config.DEPLOYMENT.get("arbiter_autodetect"):
        for az in list(az_worker_nodes.keys()):
            az_node_list = az_worker_nodes.get(az)
            if az_node_list and len(az_node_list) > 1:
                node_names = az_node_list[:2]
                distributed_worker_nodes += node_names
    elif arbiter_deployment and not config.DEPLOYMENT.get("arbiter_autodetect"):
        to_label_per_az = int(
            to_label / len(config.ENV_DATA.get("worker_availability_zones"))
        )
        for az in list(config.ENV_DATA.get("worker_availability_zones")):
            az_node_list = az_worker_nodes.get(az)
            if az_node_list and len(az_node_list) > 1:
                node_names = az_node_list[:to_label_per_az]
                distributed_worker_nodes += node_names
            else:
                raise UnavailableResourceException(
                    "Atleast 2 worker nodes required for arbiter cluster in zone %s",
                    az,
                )
    else:
        while az_worker_nodes:
            for az in list(az_worker_nodes.keys()):
                az_node_list = az_worker_nodes.get(az)
                if az_node_list:
                    node_name = az_node_list.pop(0)
                    distributed_worker_nodes.append(node_name)
                else:
                    del az_worker_nodes[az]
    logger.info(f"Distributed worker nodes for AZ: {distributed_worker_nodes}")

    to_taint = config.DEPLOYMENT.get("ocs_operator_nodes_to_taint", 0)

    distributed_worker_count = len(distributed_worker_nodes)
    if distributed_worker_count < to_label or distributed_worker_count < to_taint:
        logger.info(f"All nodes: {nodes}")
        logger.info(f"Distributed worker nodes: {distributed_worker_nodes}")
        raise UnavailableResourceException(
            f"Not enough distributed worker nodes: {distributed_worker_count} to label: "
            f"{to_label} or taint: {to_taint}!"
        )

    _ocp = ocp.OCP(kind="node")
    workers_to_label = " ".join(distributed_worker_nodes[:to_label])
    if workers_to_label:
        logger.info(
            f"Label nodes: {workers_to_label} with label: "
            f"{constants.OPERATOR_NODE_LABEL}"
        )
        label_cmds = [
            (
                f"label nodes {workers_to_label} "
                f"{constants.OPERATOR_NODE_LABEL} --overwrite"
            )
        ]
        if config.DEPLOYMENT.get("infra_nodes") and not config.ENV_DATA.get(
            "infra_replicas"
        ):
            logger.info(
                f"Label nodes: {workers_to_label} with label: "
                f"{constants.INFRA_NODE_LABEL}"
            )
            label_cmds.append(
                f"label nodes {workers_to_label} "
                f"{constants.INFRA_NODE_LABEL} --overwrite"
            )

        for cmd in label_cmds:
            _ocp.exec_oc_cmd(command=cmd)

    workers_to_taint = " ".join(distributed_worker_nodes[:to_taint])
    if workers_to_taint:
        logger.info(
            f"Taint nodes: {workers_to_taint} with taint: "
            f"{constants.OPERATOR_NODE_TAINT}"
        )
        taint_cmd = (
            f"adm taint nodes {workers_to_taint} {constants.OPERATOR_NODE_TAINT}"
        )
        _ocp.exec_oc_cmd(command=taint_cmd)


def label_storage_nodes():
    """
    Label storage nodes.
    """
    # Mark master nodes schedulable if mark_masters_schedulable: True
    if config.ENV_DATA.get("mark_masters_schedulable", False):
        mark_masters_schedulable()
        # Allow ODF to be deployed on all nodes
        logger.info("labeling all nodes as storage nodes")
        nodes = get_all_nodes()
        node_objs = get_node_objs(nodes)
        label_nodes(nodes=node_objs, label=constants.OPERATOR_NODE_LABEL)
    else:
        logger.info("labeling worker nodes as storage nodes")
        worker_node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
        label_nodes(nodes=worker_node_objs, label=constants.OPERATOR_NODE_LABEL)


def mark_masters_schedulable():
    """
    Mark masters schedulable.
    """
    path = "/spec/mastersSchedulable"
    params = f"""[{{"op": "replace", "path": "{path}", "value": true}}]"""
    scheduler_obj = ocp.OCP(
        kind=constants.SCHEDULERS_CONFIG,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    assert scheduler_obj.patch(
        params=params, format_type="json"
    ), "Failed to run patch command to update control nodes as scheduleable"
