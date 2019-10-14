import logging
import copy
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants, exceptions
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


def get_node_objs(node_names=None):
    """
    Get node objects by node names

    Args:
        node_names (list): The node names to get their objects for.
            If None, will return all cluster nodes

    Returns:
        list: Cluster node OCP objects

    """
    nodes_obj = OCP(kind='node')
    node_dicts = nodes_obj.get()['items']
    if not node_names:
        return [OCS(**node_obj) for node_obj in node_dicts]
    else:
        return [
            OCS(**node_obj) for node_obj in node_dicts if (
                node_obj.get('metadata').get('name') in node_names
            )
        ]


def get_typed_nodes(node_type='worker', num_of_nodes=None):
    """
    Get cluster's nodes according to the node type (e.g. worker, master) and the
    number of requested nodes from that type

    Args:
        node_type (str): The node type (e.g. worker, master)
        num_of_nodes (int): The number of nodes to be returned

    Returns:
        list: The nodes OCP instances

    """
    nodes = get_node_objs()

    typed_nodes = [
        n for n in nodes if node_type in n.get().get('metadata')
        .get('annotations').get('machineconfiguration.openshift.io/currentConfig')
    ]
    if num_of_nodes:
        typed_nodes = typed_nodes[:num_of_nodes]
    return typed_nodes


def wait_for_nodes_status(
    node_names=None, status=constants.NODE_READY, timeout=180
):
    """
    Wait until all nodes are in the given status

    Args:
        node_names (list): The node names to wait for to reached the desired state
            If None, will wait for all cluster nodes
        status (str): The node status to wait for
            (e.g. 'Ready', 'NotReady', 'SchedulingDisabled')
        timeout (int): The number in seconds to wait for the nodes to reach
            the status

    Raises:
        ResourceWrongStatusException: In case one or more nodes haven't
            reached the desired state

    """
    try:
        if not node_names:
            for sample in TimeoutSampler(60, 3, get_node_objs):
                if sample:
                    node_names = [node.name for node in sample]
                    break
        nodes_not_in_state = copy.deepcopy(node_names)
        log.info(f"Waiting for nodes {node_names} to reach status {status}")
        for sample in TimeoutSampler(timeout, 3, get_node_objs, nodes_not_in_state):
            for node in sample:
                if node.ocp.get_resource_status(node.name) == status:
                    nodes_not_in_state.remove(node.name)
            if not nodes_not_in_state:
                break

    except TimeoutExpiredError:
        log.error(
            f"The following nodes haven't reached status {status}: {node_names}"
        )
        raise exceptions.ResourceWrongStatusException(
            node_names, [n.describe() for n in get_node_objs(node_names)]
        )


def unschedule_nodes(node_names):
    """
    Change nodes to be unscheduled

    Args:
        node_names (list): The names of the nodes

    """
    ocp = OCP(kind='node')
    node_names_str = ' '.join(node_names)
    log.info(f"Unscheduling nodes {node_names_str}")
    ocp.exec_oc_cmd(f"adm cordon {node_names_str}")

    wait_for_nodes_status(
        node_names, status=constants.NODE_READY_SCHEDULING_DISABLED
    )


def schedule_nodes(node_names):
    """
    Change nodes to be scheduled

    Args:
        node_names (list): The names of the nodes

    """
    ocp = OCP(kind='node')
    node_names_str = ' '.join(node_names)
    ocp.exec_oc_cmd(f"adm uncordon {node_names_str}")
    log.info(f"Scheduling nodes {node_names_str}")
    wait_for_nodes_status(node_names)


def drain_nodes(node_names):
    """
    Drain nodes

    Args:
        node_names (list): The names of the nodes

    """
    ocp = OCP(kind='node')
    node_names_str = ' '.join(node_names)
    log.info(f'Draining nodes {node_names_str}')
    ocp.exec_oc_cmd(
        f"adm drain {node_names_str} --force=true --ignore-daemonsets "
        f"--delete-local-data"
    )


def get_typed_worker_nodes(os_id="rhcos"):
    """
    Get worker nodes with specific OS

    Args:
        os_id (str): OS type like rhcos, RHEL etc...

    Returns:
        list: list of worker nodes instances having specified os

    """
    worker_nodes = get_typed_nodes(node_type='worker')
    return [
        node for node in worker_nodes
        if node.get().get('metadata').get('labels').get('node.openshift.io/os_id') == os_id
    ]


def remove_nodes(nodes):
    """
    Remove the nodes from cluster

    Args:
        nodes (list): list of node instances to remove from cluster

    """
    ocp = OCP(kind='node')
    node_names = [node.get().get('metadata').get('name') for node in nodes]
    node_names_str = ' '.join(node_names)

    # unschedule node
    unschedule_nodes(node_names)

    # Drain all the pods from the node
    drain_nodes(node_names)

    # delete the nodes
    log.info(f"Deleting nodes {node_names_str}")
    ocp.exec_oc_cmd(f"delete nodes {node_names_str}")
