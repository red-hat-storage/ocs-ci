import logging
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
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
    node_objs = nodes_obj.get()['items']
    if not node_names:
        return node_objs
    else:
        return [
            node_obj for node_obj in node_objs if (
                node_obj.get('metadata').get('name') in node_names
            )
        ]


def get_typed_nodes(node_type='worker', num_of_nodes=None):
    """
    Get cluster nodes according to the node type (e.g. worker, master) and the
    number of requested nodes from that type

    Args:
        node_type (str): The node type (e.g. worker, master)
        num_of_nodes (int): The number of nodes to be returned

    Returns:
        list: The nodes OCP instances

    """
    nodes = get_node_objs()

    typed_nodes = [
        n for n in nodes if node_type in n.get('metadata')
        .get('annotations').get('machine.openshift.io/machine')
    ]
    if num_of_nodes:
        typed_nodes = typed_nodes[:num_of_nodes]
    return typed_nodes


def wait_for_nodes_status(node_names=None, status=constants.NODE_READY, timeout=120):
    """
    Wait until all nodes are in the given status

    Args:
        node_names (list): The node names to wait for to reached the desired state
            If None, will wait for all cluster nodes
        status (str): The node status to wait for
            (e.g. 'Ready', 'NotReady', 'SchedulingDisabled')
        timeout (int): The number in seconds to wait for the nodes to reach
            the status

    Returns:
        bool: True if all nodes reached the status, False otherwise

    """
    if not node_names:
        node_names = [node.get('metadata').get('name') for node in get_node_objs()]

    log.info(f"Waiting for nodes {node_names} to reach status {status}")

    try:
        for sample in TimeoutSampler(timeout, 3, get_node_objs, node_names):
            for node in sample:
                if not node_names:
                    return True
                for status_condition in node.get('status').get('conditions'):
                    if status == constants.NODE_NOT_READY:
                        message = 'Kubelet stopped posting node status'
                        if message in status_condition.get('message'):
                            node_names.remove(node.get('metadata').get('name'))
                            break
                    else:
                        if 'True' in status_condition.get('status'):
                            log.info(
                                f"The following nodes are still not "
                                f"in {status} status: {node_names}"
                            )
                            if status_condition.get('type') == status:
                                node_names.remove(node.get('metadata').get('name'))
                                break
    except TimeoutExpiredError:
        log.error(f"The following nodes haven't reached status {status}: {node_names}")
        return False


def unschedule_nodes(nodes):
    """
    Change nodes to be unscheduled

    Args:
        nodes (list): The OCP objects of the nodes

    """
    for node in nodes:
        node.exec_oc_cmd(f"adm cordon {node.get('metadata').get('name')}")
    assert wait_for_nodes_status(nodes, status=constants.NODE_SCHEDULING_DISABLED), (
        f"Not all nodes reached status {constants.NODE_SCHEDULING_DISABLED}"
    )


def schedule_nodes(nodes):
    """
    Change nodes to be scheduled

    Args:
        nodes (list): The OCP objects of the nodes

    """
    for node in nodes:
        node.exec_oc_cmd(f"adm uncordon {node.get('metadata').get('name')}")
    assert wait_for_nodes_status(nodes), f"Not all nodes reached status {constants.NODE_READY}"


def drain_nodes(node_names):
    """
    Drain nodes

    Args:
        node_names (list): The names of the nodes

    """
    ocp = OCP(kind='node')
    node_names = ' '.join(node_names)
    ocp.exec_oc_cmd(f"adm drain {node_names}")


def maintenance_nodes(nodes):
    """
    Move nodes to maintenance

    Args:
        nodes (list): The OCP objects of the nodes to move to maintenance

    """
    unschedule_nodes(nodes)
    node_names = [node.get('metadata').get('name') for node in nodes]
    drain_nodes(node_names)
