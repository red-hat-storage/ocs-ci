from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import UnavailableResourceException


def get_arbiter_location():
    """
    Get arbiter mon location for storage cluster

    Raises:
        UnavailableResourceException: When a zone doesn't exist that can host the arbiter mon.

    Returns:
        str: Arbiter mon location for storage cluster

    """
    if config.DEPLOYMENT.get("arbiter_deployment") and not config.DEPLOYMENT.get(
        "arbiter_autodetect"
    ):
        return config.DEPLOYMENT.get("arbiter_zone")

    # below logic will autodetect arbiter_zone
    nodes = ocp.OCP(kind="node").get().get("items", [])

    worker_nodes_zones = {
        node["metadata"]["labels"].get(constants.ZONE_LABEL)
        for node in nodes
        if constants.WORKER_LABEL in node["metadata"]["labels"]
        and str(constants.OPERATOR_NODE_LABEL)[:-3] in node["metadata"]["labels"]
    }

    master_nodes_zones = {
        node["metadata"]["labels"].get(constants.ZONE_LABEL)
        for node in nodes
        if constants.MASTER_LABEL in node["metadata"]["labels"]
    }

    arbiter_locations = list(master_nodes_zones - worker_nodes_zones)

    if len(arbiter_locations) < 1:
        raise UnavailableResourceException(
            "At least 1 different zone required than storage nodes in master nodes to host arbiter mon"
        )

    return arbiter_locations[0]
