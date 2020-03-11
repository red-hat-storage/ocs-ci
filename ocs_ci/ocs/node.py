import copy
import logging
import re

from subprocess import TimeoutExpired
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants, exceptions
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import machine
import tests.helpers
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources import pod


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
        nodes = [OCS(**node_obj) for node_obj in node_dicts]
    else:
        nodes = [
            OCS(**node_obj) for node_obj in node_dicts if (
                node_obj.get('metadata').get('name') in node_names
            )
        ]
    assert nodes, "Failed to get the nodes OCS objects"
    return nodes


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
    typed_nodes = [
        node for node in get_node_objs() if node
        .ocp.get_resource(resource_name=node.name, column='ROLES') == node_type
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


def get_node_ips(node_type='worker'):
    """
    Gets the node public IP

    Args:
        node_type (str): The node type (e.g. worker, master)

    Returns:
        list: Node IP's

    """
    ocp = OCP(kind=constants.NODE)
    if node_type == 'worker':
        nodes = ocp.get(selector=constants.WORKER_LABEL).get('items')
    if node_type == 'master:':
        nodes = ocp.get(selector=constants.MASTER_LABEL).get('items')

    if config.ENV_DATA['platform'].lower() == constants.AWS_PLATFORM:
        raise NotImplementedError
    elif config.ENV_DATA['platform'].lower() == constants.VSPHERE_PLATFORM:
        return [
            each['address'] for node in nodes
            for each in node['status']['addresses'] if each['type'] == "ExternalIP"
        ]
    else:
        raise NotImplementedError


def add_new_node_and_label_it(machineset_name):
    """
    Add a new node and label it

    Args:
        machineset_name (str): Name of the machine set

    """
    # Get the initial nodes list
    initial_nodes = tests.helpers.get_worker_nodes()
    log.info(f"Current available worker nodes are {initial_nodes}")

    # get machineset replica count
    machineset_replica_count = machine.get_replica_count(machineset_name)

    # Increase its replica count
    machine.add_node(machineset_name, count=machineset_replica_count + 1)
    log.info(
        f"Increased {machineset_name} count "
        f"by {machineset_replica_count + 1}"
    )

    # wait for the new node to come to ready state
    log.info("Waiting for the new node to be in ready state")
    machine.wait_for_new_node_to_be_ready(machineset_name)

    # Get the node name of new spun node
    nodes_after_new_spun_node = tests.helpers.get_worker_nodes()
    new_spun_node = list(
        set(nodes_after_new_spun_node) - set(initial_nodes)
    )
    log.info(f"New spun node is {new_spun_node}")

    # Label it
    node_obj = ocp.OCP(kind='node')
    node_obj.add_label(
        resource_name=new_spun_node[0],
        label=constants.OPERATOR_NODE_LABEL
    )
    log.info(
        f"Successfully labeled {new_spun_node} with OCS storage label"
    )


def get_node_logs(node_name):
    """
    Get logs from a given node

    pod_name (str): Name of the node

    Returns:
        str: Output of 'dmesg' run on node
    """
    node = OCP(kind='node')
    return node.exec_oc_debug_cmd(node_name, ["dmesg"])


def get_node_resource_utilization_from_adm_top(nodename=None, node_type='worker'):
    """
    Gets the node's cpu and memory utilization in percentage using adm top command.

    Args:
        nodename (str) : The node name
        node_type (str) : The node type (e.g. master, worker)

    Returns:
        dict : Node name and its cpu and memory utilization in
               percentage

    """

    node_names = [nodename] if nodename else [
        node.name for node in get_typed_nodes(node_type=node_type)
    ]
    obj = ocp.OCP()
    resource_utilization_all_nodes = obj.exec_oc_cmd(
        command='adm top nodes', out_yaml_format=False
    ).split("\n")
    utilization_dict = {}

    for node in node_names:
        for value in resource_utilization_all_nodes:
            if node in value:
                value = re.findall(r'\d+', value.strip())
                cpu_utilization = value[2]
                log.info("The CPU utilized by the node "
                         f"{node} is {cpu_utilization}%")
                memory_utilization = value[4]
                log.info("The memory utilized of the node "
                         f"{node} is {memory_utilization}%")
                utilization_dict[node] = {
                    'cpu': int(cpu_utilization),
                    'memory': int(memory_utilization)
                }
    return utilization_dict


def get_node_resource_utilization_from_oc_describe(nodename=None, node_type='worker'):
    """
    Gets the node's cpu and memory utilization in percentage using oc describe node

    Args:
        nodename (str) : The node name
        node_type (str) : The node type (e.g. master, worker)

    Returns:
        dict : Node name and its cpu and memory utilization in
               percentage

    """

    node_names = [nodename] if nodename else [
        node.name for node in get_typed_nodes(node_type=node_type)
    ]
    obj = ocp.OCP()
    utilization_dict = {}
    for node in node_names:
        output = obj.exec_oc_cmd(
            command=f"describe node {node}", out_yaml_format=False
        ).split("\n")
        for line in output:
            if 'cpu  ' in line:
                cpu_data = line.split(' ')
                cpu = re.findall(r'\d+', [i for i in cpu_data if i][2])
            if 'memory  ' in line:
                mem_data = line.split(' ')
                mem = re.findall(r'\d+', [i for i in mem_data if i][2])
        utilization_dict[node] = {
            'cpu': int(cpu[0]),
            'memory': int(mem[0])
        }

    return utilization_dict


def node_network_failure(node_names, wait=True):
    """
    Induce node network failure
    Bring node network interface down, making the node unresponsive

    Args:
        node_names (list): The names of the nodes
        wait (bool): True in case wait for status is needed, False otherwise

    Returns:
        bool: True if node network fail is successful
    """
    if not isinstance(node_names, list):
        node_names = [node_names]

    ocp = OCP(kind='node')
    fail_nw_cmd = "ifconfig $(route | grep default | awk '{print $(NF)}') down"

    for node_name in node_names:
        try:
            ocp.exec_oc_debug_cmd(
                node=node_name, cmd_list=[fail_nw_cmd], timeout=15
            )
        except TimeoutExpired:
            pass

    if wait:
        wait_for_nodes_status(
            node_names=node_names, status=constants.NODE_NOT_READY
        )
    return True


def get_osd_running_nodes():
    """
    Gets the osd running node names

    Returns:
        list: OSD node names

    """
    return [
        pod.get_pod_node(osd_node).name for osd_node in pod.get_osd_pods()
    ]


def get_app_pod_running_nodes(pod_obj):
    """
    Gets the app pod running node names

    Args:
        pod_obj (list): List of app pod objects

    Returns:
        list: App pod running node names

    """
    return [pod.get_pod_node(obj_pod).name for obj_pod in pod_obj]


def get_both_osd_and_app_pod_running_node(
    osd_running_nodes, app_pod_running_nodes
):
    """
     Gets both osd and app pod running node names

     Args:
         osd_running_nodes(list): List of osd running node names
         app_pod_running_nodes(list): List of app pod running node names

     Returns:
         list: Both OSD and app pod running node names

     """
    common_nodes = list(set(osd_running_nodes) & set(app_pod_running_nodes))
    log.info(f"Common node is {common_nodes}")
    return common_nodes
