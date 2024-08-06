import copy
import logging
import re
import time
from prettytable import PrettyTable
from collections import defaultdict
from operator import itemgetter
import random

from subprocess import TimeoutExpired
from semantic_version import Version

from ocs_ci.ocs.machine import get_machine_objs

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    NotAllNodesCreated,
    CommandFailed,
    ResourceNotFoundError,
    NotFoundError,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants, exceptions, ocp, defaults
from ocs_ci.utility import version
from ocs_ci.utility.utils import TimeoutSampler, convert_device_size, get_az_count
from ocs_ci.ocs import machine
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import set_selinux_permissions, get_ocp_version
from ocs_ci.ocs.resources.pv import (
    get_pv_objs_in_sc,
    verify_new_pvs_available_in_sc,
    delete_released_pvs_in_sc,
    get_pv_size,
    get_node_pv_objs,
)
from ocs_ci.utility.version import get_semantic_version
from ocs_ci.utility.rosa import (
    is_odf_addon_installed,
    edit_addon_installation,
    wait_for_addon_to_be_ready,
)
from ocs_ci.utility.decorators import switch_to_orig_index_at_last
from ocs_ci.utility.vsphere import VSPHERE


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
    nodes_obj = OCP(kind="node")
    node_dicts = nodes_obj.get()["items"]
    if not node_names:
        nodes = [OCS(**node_obj) for node_obj in node_dicts]
    else:
        nodes = [
            OCS(**node_obj)
            for node_obj in node_dicts
            if (node_obj.get("metadata").get("name") in node_names)
        ]
    assert nodes, "Failed to get the nodes OCS objects"
    return nodes


def get_nodes(node_type=constants.WORKER_MACHINE, num_of_nodes=None):
    """
    Get cluster's nodes according to the node type (e.g. worker, master) and the
    number of requested nodes from that type.
    In case of HCI provider cluster and 'node_type' is worker, it will exclude the master nodes.

    Args:
        node_type (str): The node type (e.g. worker, master)
        num_of_nodes (int): The number of nodes to be returned

    Returns:
        list: The nodes OCP instances

    """
    from ocs_ci.ocs.cluster import is_hci_provider_cluster

    if (
        config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS
        and node_type == constants.WORKER_MACHINE
    ):
        typed_nodes = [
            node
            for node in get_node_objs()
            if node_type
            in node.ocp.get_resource(resource_name=node.name, column="ROLES")
            and constants.INFRA_MACHINE
            not in node.ocp.get_resource(resource_name=node.name, column="ROLES")
        ]
    else:
        typed_nodes = [
            node
            for node in get_node_objs()
            if node_type
            in node.ocp.get_resource(resource_name=node.name, column="ROLES")
        ]
    if is_hci_provider_cluster() and node_type == constants.WORKER_MACHINE:
        typed_nodes = [
            node
            for node in typed_nodes
            if constants.MASTER_MACHINE
            not in node.ocp.get_resource(resource_name=node.name, column="ROLES")
        ]

    if num_of_nodes:
        typed_nodes = typed_nodes[:num_of_nodes]
    return typed_nodes


def get_nodes_having_label(label):
    """
    Gets nodes with particular label

    Args:
        label (str): Label

    Return:
        Dict: Representing nodes info

    """
    ocp_node_obj = OCP(kind=constants.NODE)
    nodes = ocp_node_obj.get(selector=label).get("items")
    return nodes


def get_all_nodes():
    """
    Gets the all nodes in cluster

    Returns:
        list: List of node name

    """
    ocp_node_obj = ocp.OCP(kind=constants.NODE)
    node_items = ocp_node_obj.get().get("items")
    return [node["metadata"]["name"] for node in node_items]


def wait_for_nodes_status(
    node_names=None, status=constants.NODE_READY, timeout=180, sleep=3
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
        sleep (int): Time in seconds to sleep between attempts

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
        for sample in TimeoutSampler(timeout, sleep, get_node_objs, nodes_not_in_state):
            for node in sample:
                try:
                    if node.ocp.get_resource_status(node.name) == status:
                        log.info(f"Node {node.name} reached status {status}")
                        nodes_not_in_state.remove(node.name)
                except CommandFailed as ex:
                    log.info(f"failed to get the node status by error {ex}")
                    continue
            if not nodes_not_in_state:
                break
        log.info(f"The following nodes reached status {status}: {node_names}")
    except TimeoutExpiredError:
        log.error(
            f"The following nodes haven't reached status {status}: "
            f"{nodes_not_in_state}"
        )
        error_message = (
            f"{node_names}, {[n.describe() for n in get_node_objs(node_names)]}"
        )
        raise exceptions.ResourceWrongStatusException(error_message)


def unschedule_nodes(node_names):
    """
    Change nodes to be unscheduled

    Args:
        node_names (list): The names of the nodes

    """
    ocp = OCP(kind="node")
    node_names_str = " ".join(node_names)
    log.info(f"Unscheduling nodes {node_names_str}")
    ocp.exec_oc_cmd(f"adm cordon {node_names_str}")

    wait_for_nodes_status(node_names, status=constants.NODE_READY_SCHEDULING_DISABLED)


def schedule_nodes(node_names):
    """
    Change nodes to be scheduled

    Args:
        node_names (list): The names of the nodes

    """
    ocp = OCP(kind="node")
    node_names_str = " ".join(node_names)
    ocp.exec_oc_cmd(f"adm uncordon {node_names_str}")
    log.info(f"Scheduling nodes {node_names_str}")
    wait_for_nodes_status(node_names)


def drain_nodes(node_names, timeout=1800, disable_eviction=False):
    """
    Drain nodes

    Args:
        node_names (list): The names of the nodes
        timeout (int): Time to wait for the drain nodes 'oc' command
        disable_eviction (bool): On True will delete pod that is protected by PDB, False by default

    Raises:
        TimeoutExpired: in case drain command fails to complete in time

    """
    ocp = OCP(kind="node")
    node_names_str = " ".join(node_names)
    log.info(f"Draining nodes {node_names_str}")
    try:
        drain_deletion_flag = (
            "--delete-emptydir-data"
            if get_semantic_version(get_ocp_version(), only_major_minor=True)
            >= version.VERSION_4_7
            else "--delete-local-data"
        )
        if disable_eviction:
            ocp.exec_oc_cmd(
                f"adm drain {node_names_str} --force=true --ignore-daemonsets "
                f"{drain_deletion_flag} --disable-eviction",
                timeout=timeout,
            )
        else:
            ocp.exec_oc_cmd(
                f"adm drain {node_names_str} --force=true --ignore-daemonsets "
                f"{drain_deletion_flag}",
                timeout=timeout,
            )
    except TimeoutExpired:
        ct_pod = pod.get_ceph_tools_pod()
        ceph_status = ct_pod.exec_cmd_on_pod("ceph status", out_yaml_format=False)
        log.error(f"Drain command failed to complete. Ceph status: {ceph_status}")
        # TODO: Add re-balance status once pull/1679 is merged
        raise


def get_typed_worker_nodes(os_id="rhcos"):
    """
    Get worker nodes with specific OS

    Args:
        os_id (str): OS type like rhcos, RHEL etc...

    Returns:
        list: list of worker nodes instances having specified os

    """
    worker_nodes = get_nodes(node_type="worker")
    return [
        node
        for node in worker_nodes
        if node.get().get("metadata").get("labels").get("node.openshift.io/os_id")
        == os_id
    ]


def remove_nodes(nodes):
    """
    Remove the nodes from cluster

    Args:
        nodes (list): list of node instances to remove from cluster

    """
    ocp = OCP(kind="node")
    node_names = [node.get().get("metadata").get("name") for node in nodes]
    node_names_str = " ".join(node_names)

    # unschedule node
    unschedule_nodes(node_names)

    # Drain all the pods from the node
    drain_nodes(node_names)

    # delete the nodes
    log.info(f"Deleting nodes {node_names_str}")
    ocp.exec_oc_cmd(f"delete nodes {node_names_str}")


def get_node_ips(node_type="worker"):
    """
    Gets the node public IP

    Args:
        node_type (str): The node type (e.g. worker, master)

    Returns:
        list: Node IP's

    """
    ocp = OCP(kind=constants.NODE)
    if node_type == "worker":
        nodes = ocp.get(selector=constants.WORKER_LABEL).get("items")
    if node_type == "master:":
        nodes = ocp.get(selector=constants.MASTER_LABEL).get("items")

    if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
        raise NotImplementedError
    elif config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
        return [
            each["address"]
            for node in nodes
            for each in node["status"]["addresses"]
            if each["type"] == "ExternalIP"
        ]
    else:
        raise NotImplementedError


def get_node_ip_addresses(ipkind):
    """
    Gets a dictionary of required IP addresses for all nodes

    Args:
        ipkind: ExternalIP or InternalIP or Hostname

    Returns:
        dict: Internal or Exteranl IP addresses keyed off of node name

    """
    ocp = OCP(kind=constants.NODE)
    masternodes = ocp.get(selector=constants.MASTER_LABEL).get("items")
    workernodes = ocp.get(selector=constants.WORKER_LABEL).get("items")
    nodes = masternodes + workernodes

    return {
        node["metadata"]["name"]: each["address"]
        for node in nodes
        for each in node["status"]["addresses"]
        if each["type"] == ipkind
    }


def add_new_node_and_label_it(machineset_name, num_nodes=1, mark_for_ocs_label=True):
    """
    Add a new node for ipi and label it

    Args:
        machineset_name (str): Name of the machine set
        num_nodes (int): number of nodes to add
        mark_for_ocs_label (bool): True if label the new node
    eg: add_new_node_and_label_it("new-tdesala-zlqzn-worker-us-east-2a")

    Returns:
        list: new spun node names

    """
    # Get the initial nodes list
    initial_nodes = get_worker_nodes()
    log.info(f"Current available worker nodes are {initial_nodes}")

    # get machineset replica count
    machineset_replica_count = machine.get_replica_count(machineset_name)
    log.info(f"{machineset_name} has replica count: {machineset_replica_count}")

    # Increase its replica count
    log.info(f"Increasing the replica count by {num_nodes}")
    machine.add_node(machineset_name, count=machineset_replica_count + num_nodes)
    log.info(
        f"{machineset_name} now has replica "
        f"count: {machineset_replica_count + num_nodes}"
    )

    # wait for the new node to come to ready state
    log.info("Waiting for the new node to be in ready state")
    machine.wait_for_new_node_to_be_ready(machineset_name, timeout=900)

    # Get the node name of new spun node
    nodes_after_new_spun_node = get_worker_nodes()
    new_spun_nodes = list(set(nodes_after_new_spun_node) - set(initial_nodes))
    log.info(f"New spun nodes: {new_spun_nodes}")

    # Label it
    if mark_for_ocs_label:
        node_obj = ocp.OCP(kind="node")
        for new_spun_node in new_spun_nodes:
            if is_node_labeled(new_spun_node):
                log.info(
                    f"node {new_spun_node} is already labeled with the OCS storage label"
                )
            else:
                node_obj.add_label(
                    resource_name=new_spun_node, label=constants.OPERATOR_NODE_LABEL
                )
                log.info(f"Successfully labeled {new_spun_node} with OCS storage label")

    return new_spun_nodes


def add_new_node_and_label_upi(
    node_type, num_nodes, mark_for_ocs_label=True, node_conf=None
):
    """
    Add a new node for aws/vmware upi platform and label it

    Args:
        node_type (str): Type of node, RHEL or RHCOS
        num_nodes (int): number of nodes to add
        mark_for_ocs_label (bool): True if label the new node
        node_conf (dict): The node configurations.

    Returns:
        list: new spun node names

    """
    node_conf = node_conf or {}
    initial_nodes = get_worker_nodes()
    from ocs_ci.ocs.platform_nodes import PlatformNodesFactory

    plt = PlatformNodesFactory()
    node_util = plt.get_nodes_platform()
    node_util.create_and_attach_nodes_to_cluster(node_conf, node_type, num_nodes)
    for sample in TimeoutSampler(timeout=600, sleep=6, func=get_worker_nodes):
        if len(sample) == len(initial_nodes) + num_nodes:
            break

    nodes_after_exp = get_worker_nodes()
    wait_for_nodes_status(node_names=get_worker_nodes(), status=constants.NODE_READY)

    new_spun_nodes = list(set(nodes_after_exp) - set(initial_nodes))
    log.info(f"New spun nodes: {new_spun_nodes}")
    # For IBM cloud, it takes time to settle down new nodes even after reaching READY state
    if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
        log.info("Sleeping for 300 seconds for new nodes to settle down")
        time.sleep(300)

    if node_type == constants.RHEL_OS:
        set_selinux_permissions(workers=new_spun_nodes)

    if mark_for_ocs_label:
        node_obj = ocp.OCP(kind="node")
        for new_spun_node in new_spun_nodes:
            node_obj.add_label(
                resource_name=new_spun_node, label=constants.OPERATOR_NODE_LABEL
            )
            log.info(f"Successfully labeled {new_spun_node} with OCS storage label")
    return new_spun_nodes


def get_node_logs(node_name):
    """
    Get logs from a given node

    pod_name (str): Name of the node

    Returns:
        str: Output of 'dmesg' run on node
    """
    node = OCP(kind="node")
    return node.exec_oc_debug_cmd(node_name, ["dmesg"])


def get_node_resource_utilization_from_adm_top(
    nodename=None, node_type=constants.WORKER_MACHINE, print_table=False
):
    """
    Gets the node's cpu and memory utilization in percentage using adm top command.

    Args:
        nodename (str) : The node name
        node_type (str) : The node type (e.g. master, worker)

    Returns:
        dict : Node name and its cpu and memory utilization in
               percentage

    """

    node_names = (
        [nodename]
        if nodename
        else [node.name for node in get_nodes(node_type=node_type)]
    )

    # Validate node is in Ready state
    wait_for_nodes_status(node_names, status=constants.NODE_READY, timeout=30)

    obj = ocp.OCP()
    resource_utilization_all_nodes = obj.exec_oc_cmd(
        command="adm top nodes", out_yaml_format=False
    ).split("\n")
    utilization_dict = {}

    for node in node_names:
        for value in resource_utilization_all_nodes:
            if node in value:
                value = re.findall(r"(\d{1,3})%", value.strip())
                cpu_utilization = value[0]
                log.info(
                    "The CPU utilized by the node " f"{node} is {cpu_utilization}%"
                )
                memory_utilization = value[1]
                log.info(
                    "The memory utilized of the node "
                    f"{node} is {memory_utilization}%"
                )
                utilization_dict[node] = {
                    "cpu": int(cpu_utilization),
                    "memory": int(memory_utilization),
                }

    if print_table:
        print_table_node_resource_utilization(
            utilization_dict=utilization_dict,
            field_names=["Node Name", "CPU USAGE adm_top", "Memory USAGE adm_top"],
        )
    return utilization_dict


def get_node_resource_utilization_from_oc_describe(
    nodename=None, node_type=constants.WORKER_MACHINE, print_table=False
):
    """
    Gets the node's cpu and memory utilization in percentage using oc describe node

    Args:
        nodename (str) : The node name
        node_type (str) : The node type (e.g. master, worker)

    Returns:
        dict : Node name and its cpu and memory utilization in
               percentage

    """

    node_names = (
        [nodename]
        if nodename
        else [node.name for node in get_nodes(node_type=node_type)]
    )
    obj = ocp.OCP()
    utilization_dict = {}
    for node in node_names:
        output = obj.exec_oc_cmd(
            command=f"describe node {node}", out_yaml_format=False
        ).split("\n")
        for line in output:
            if "cpu  " in line:
                cpu_data = line.split(" ")
                cpu = re.findall(r"\d+", [i for i in cpu_data if i][2])
            if "memory  " in line:
                mem_data = line.split(" ")
                mem = re.findall(r"\d+", [i for i in mem_data if i][2])
        utilization_dict[node] = {"cpu": int(cpu[0]), "memory": int(mem[0])}

    if print_table:
        print_table_node_resource_utilization(
            utilization_dict=utilization_dict,
            field_names=[
                "Node Name",
                "CPU USAGE oc_describe",
                "Memory USAGE oc_describe",
            ],
        )

    return utilization_dict


def get_running_pod_count_from_node(nodename=None, node_type=constants.WORKER_MACHINE):
    """
    Gets the node running pod count using oc describe node

    Args:
        nodename (str) : The node name
        node_type (str) : The node type (e.g. master, worker)

    Returns:
        dict : Node name and its pod_count

    """

    node_names = (
        [nodename]
        if nodename
        else [node.name for node in get_nodes(node_type=node_type)]
    )
    obj = ocp.OCP()
    pod_count_dict = {}
    for node in node_names:
        output = obj.exec_oc_cmd(
            command=f"describe node {node}", out_yaml_format=False
        ).split("\n")
        for line in output:
            if "Non-terminated Pods:  " in line:
                count_line = line.split(" ")
                pod_count = re.findall(r"\d+", [i for i in count_line if i][2])
        pod_count_dict[node] = int(pod_count[0])

    return pod_count_dict


def print_table_node_resource_utilization(utilization_dict, field_names):
    """
    Print table of node utilization

    Args:
        utilization_dict (dict) : CPU and Memory utilization per Node
        field_names (list) : The field names of the table

    """
    usage_memory_table = PrettyTable()
    usage_memory_table.field_names = field_names
    for node, util_node in utilization_dict.items():
        usage_memory_table.add_row(
            [node, f'{util_node["cpu"]}%', f'{util_node["memory"]}%']
        )
    log.info(f"\n{usage_memory_table}\n")


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

    ocp = OCP(kind="node")
    fail_nw_cmd = "ifconfig $(route | grep default | awk '{print $(NF)}') down"

    for node_name in node_names:
        try:
            ocp.exec_oc_debug_cmd(node=node_name, cmd_list=[fail_nw_cmd], timeout=15)
        except TimeoutExpired:
            pass

    if wait:
        wait_for_nodes_status(node_names=node_names, status=constants.NODE_NOT_READY)
    return True


def get_osd_running_nodes():
    """
    Gets the osd running node names

    Returns:
        list: OSD node names

    """
    return list({pod.get_pod_node(osd_node).name for osd_node in pod.get_osd_pods()})


def get_osds_per_node():
    """
    Gets the osd running pod names per node name

    Returns:
        dict: {"Node name":["osd running pod name running on the node",..,]}

    """
    dic_node_osd = defaultdict(list)
    osd_pods = pod.get_osd_pods()
    for osd_pod in osd_pods:
        dic_node_osd[osd_pod.data["spec"]["nodeName"]].append(osd_pod.name)
    return dic_node_osd


def get_app_pod_running_nodes(pod_obj):
    """
    Gets the app pod running node names

    Args:
        pod_obj (list): List of app pod objects

    Returns:
        list: App pod running node names

    """
    return [pod.get_pod_node(obj_pod).name for obj_pod in pod_obj]


def get_both_osd_and_app_pod_running_node(osd_running_nodes, app_pod_running_nodes):
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


def get_node_from_machine_name(machine_name):
    """
    Get node name from a given machine_name.

    Args:
        machine_name (str): Name of Machine

    Returns:
        str: Name of Node (or None if not found)

    """
    machine_objs = get_machine_objs()
    for machine_obj in machine_objs:
        if machine_obj.name == machine_name:
            machine_dict = machine_obj.get()
            node_name = machine_dict["status"]["nodeRef"]["name"]
            return node_name


def get_provider():
    """
    Return the OCP Provider (Platform)

    Returns:
         str: The Provider that the OCP is running on

    """

    ocp_cluster = OCP(kind="", resource_name="nodes")
    results = ocp_cluster.get("nodes")["items"][0]["spec"]
    if "providerID" in results:
        return results["providerID"].split(":")[0]
    else:
        return "BareMetal"


def get_compute_node_names(no_replace=False):
    """
    Gets the compute node names

    Args:
        no_replace (bool): If False '.' will replaced with '-'

    Returns:
        list: List of compute node names

    """
    platform = config.ENV_DATA.get("platform").lower()
    compute_node_objs = get_nodes()
    if platform in [
        constants.VSPHERE_PLATFORM,
        constants.AWS_PLATFORM,
        constants.RHV_PLATFORM,
    ]:
        return [
            compute_obj.get()["metadata"]["labels"][constants.HOSTNAME_LABEL]
            for compute_obj in compute_node_objs
        ]
    elif platform in [
        constants.BAREMETAL_PLATFORM,
        constants.BAREMETALPSI_PLATFORM,
        constants.IBM_POWER_PLATFORM,
        constants.HCI_BAREMETAL,
    ]:
        if no_replace:
            return [
                compute_obj.get()["metadata"]["labels"][constants.HOSTNAME_LABEL]
                for compute_obj in compute_node_objs
            ]
        else:
            return [
                compute_obj.get()["metadata"]["labels"][
                    constants.HOSTNAME_LABEL
                ].replace(".", "-")
                for compute_obj in compute_node_objs
            ]
    else:
        raise NotImplementedError


def get_ocs_nodes(num_of_nodes=None):
    """
    Gets the ocs nodes

    Args:
        num_of_nodes (int): The number of ocs nodes to return. If not specified,
            it returns all the ocs nodes.

    Returns:
        list: List of the ocs nodes

    """
    # Import inside the function to avoid circular loop
    from ocs_ci.ocs.cluster import is_managed_service_cluster
    from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster_state

    # If we use managed service or external mode the worker nodes are without the OCS label.
    # So in that case, we will get the worker nodes without searching for the OCS label.
    ms_with_odf_addon = is_managed_service_cluster() and is_odf_addon_installed()
    external_mode_with_ocs = (
        config.DEPLOYMENT.get("external_mode")
        and get_storage_cluster_state(constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE)
        == constants.STATUS_READY
    )
    if ms_with_odf_addon or external_mode_with_ocs:
        ocs_node_names = get_worker_nodes()
    else:
        ocs_node_names = machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL)

    assert ocs_node_names, "Didn't find the ocs nodes"

    ocs_nodes = get_node_objs(ocs_node_names)
    num_of_nodes = num_of_nodes or len(ocs_nodes)
    return ocs_nodes[:num_of_nodes]


def get_node_name(node_obj):
    """
    Get oc node's name

    Args:
        node_obj (node_obj): oc node object

    Returns:
        str: node's name

    """
    node_items = node_obj.get("items")
    return node_items["metadata"]["name"]


def check_nodes_specs(min_memory, min_cpu):
    """
    Check that the cluster worker nodes meet the required minimum CPU and memory

    Args:
        min_memory (int): The required minimum memory in bytes
        min_cpu (int): The required minimum number of vCPUs

    Returns:
        bool: True if all nodes meet the required minimum specs, False otherwise

    """
    nodes = get_nodes()
    log.info(
        f"Checking following nodes with worker selector (assuming that "
        f"this is ran in CI and there are no worker nodes without OCS):\n"
        f"{[node.get().get('metadata').get('name') for node in nodes]}"
    )
    for node in nodes:
        real_cpu = int(node.get()["status"]["capacity"]["cpu"])
        real_memory = convert_device_size(
            node.get()["status"]["capacity"]["memory"], "BY"
        )
        if real_cpu < min_cpu or real_memory < min_memory:
            log.warning(
                f"Node {node.get().get('metadata').get('name')} specs don't meet "
                f" the minimum required specs.\n The requirements are: "
                f"{min_cpu} CPUs and {min_memory} Memory\nThe node has: {real_cpu} "
                f"CPUs and {real_memory} Memory"
            )
            return False
    log.info(
        f"Cluster worker nodes meet the minimum requirements of "
        f"{min_cpu} CPUs and {min_memory} Memory"
    )
    return True


def delete_and_create_osd_node_ipi(osd_node_name):
    """
    Unschedule, drain and delete osd node, and creating a new osd node.
    At the end of the function there should be the same number of osd nodes as
    it was in the beginning, and also ceph health should be OK.

    This function is for any IPI platform.

    Args:
        osd_node_name (str): the name of the osd node

    Returns:
        str: The new node name

    """
    log.info("Going to unschedule, drain and delete %s node", osd_node_name)
    # Unscheduling node
    unschedule_nodes([osd_node_name])
    # Draining Node
    drain_nodes([osd_node_name])
    log.info("Getting machine name from specified node name")
    machine_name = machine.get_machine_from_node_name(osd_node_name)
    log.info(f"Node {osd_node_name} associated machine is {machine_name}")
    log.info(f"Deleting machine {machine_name} and waiting for new machine to come up")
    new_machine_name = machine.delete_machine_and_check_state_of_new_spinned_machine(
        machine_name
    )
    machineset_name = machine.get_machineset_from_machine_name(new_machine_name)
    log.info("Waiting for new worker node to be in ready state")
    machine.wait_for_new_node_to_be_ready(machineset_name)
    new_node_name = get_node_from_machine_name(new_machine_name)
    if not is_node_labeled(new_node_name):
        log.info("Adding ocs label to newly created worker node")
        node_obj = ocp.OCP(kind="node")
        node_obj.add_label(
            resource_name=new_node_name, label=constants.OPERATOR_NODE_LABEL
        )
        log.info(f"Successfully labeled {new_node_name} with OCS storage label")

    return new_node_name


def delete_and_create_osd_node_aws_upi(osd_node_name):
    """
    Unschedule, drain and delete osd node, and creating a new osd node.
    At the end of the function there should be the same number of osd nodes as
    it was in the beginning, and also ceph health should be OK.
    This function is for AWS UPI.

    Args:
        osd_node_name (str): the name of the osd node

    Returns:
        str: The new node name

    """

    osd_node = get_node_objs(node_names=[osd_node_name])[0]
    az = get_node_az(osd_node)
    from ocs_ci.ocs.platform_nodes import AWSNodes

    aws_nodes = AWSNodes()
    stack_name_of_deleted_node = aws_nodes.get_stack_name_of_node(osd_node_name)

    remove_nodes([osd_node])

    log.info(f"Waiting for node {osd_node_name} to be deleted")
    osd_node.ocp.wait_for_delete(
        osd_node_name, timeout=600
    ), f"Node {osd_node_name} is not deleted"

    log.info(f"name of deleted node = {osd_node_name}")
    log.info(f"availability zone of deleted node = {az}")
    log.info(f"stack name of deleted node = {stack_name_of_deleted_node}")

    if config.ENV_DATA.get("rhel_workers"):
        node_type = constants.RHEL_OS
    else:
        node_type = constants.RHCOS

    log.info("Preparing to create a new node...")
    node_conf = {"stack_name": stack_name_of_deleted_node}
    new_node_names = add_new_node_and_label_upi(node_type, 1, node_conf=node_conf)

    return new_node_names[0]


def get_node_az(node):
    """
    Get the node availability zone

    Args:
        node (ocs_ci.ocs.resources.ocs.OCS): The node object

    Returns:
        str: The name of the node availability zone

    """
    labels = node.get().get("metadata", {}).get("labels", {})
    return labels.get(constants.ZONE_LABEL)


def delete_and_create_osd_node_vsphere_upi(osd_node_name, use_existing_node=False):
    """
    Unschedule, drain and delete osd node, and creating a new osd node.
    At the end of the function there should be the same number of osd nodes as
    it was in the beginning, and also ceph health should be OK.
    This function is for vSphere UPI.

    Args:
        osd_node_name (str): the name of the osd node
        use_existing_node (bool): If False, create a new node and label it.
            If True, use an existing node to replace the deleted node
            and label it.

    Returns:
        str: The new node name

    """
    from ocs_ci.ocs.platform_nodes import PlatformNodesFactory

    plt = PlatformNodesFactory()
    node_util = plt.get_nodes_platform()

    osd_node = get_node_objs(node_names=[osd_node_name])[0]
    remove_nodes([osd_node])

    log.info(f"Waiting for node {osd_node_name} to be deleted")
    osd_node.ocp.wait_for_delete(
        osd_node_name, timeout=600
    ), f"Node {osd_node_name} is not deleted"

    log.info(f"name of deleted node = {osd_node_name}")
    node_util.terminate_nodes([osd_node])

    if config.ENV_DATA.get("rhel_workers"):
        node_type = constants.RHEL_OS
    else:
        node_type = constants.RHCOS

    if not use_existing_node:
        log.info("Preparing to create a new node...")
        new_node_names = add_new_node_and_label_upi(node_type, 1)
        new_node_name = new_node_names[0]
    else:
        node_not_in_ocs = get_worker_nodes_not_in_ocs()[0]
        log.info(
            f"Preparing to replace the node {osd_node_name} "
            f"with an existing node {node_not_in_ocs.name}"
        )
        if node_type == constants.RHEL_OS:
            set_selinux_permissions(workers=[node_not_in_ocs])
        label_nodes([node_not_in_ocs])
        new_node_name = node_not_in_ocs.name

    return new_node_name


def delete_and_create_osd_node_vsphere_upi_lso(osd_node_name, use_existing_node=False):
    """
    Unschedule, drain and delete osd node, and creating a new osd node.
    At the end of the function there should be the same number of osd nodes as
    it was in the beginning, and also ceph health should be OK.
    This function is for vSphere UPI.

    Args:
        osd_node_name (str): the name of the osd node
        use_existing_node (bool): If False, create a new node and label it.
            If True, use an existing node to replace the deleted node
            and label it.

    Returns:
        str: The new node name

    """
    sc_name = constants.LOCAL_BLOCK_RESOURCE
    old_pv_objs = get_pv_objs_in_sc(sc_name)

    osd_node = get_node_objs(node_names=[osd_node_name])[0]
    osd_ids = get_node_osd_ids(osd_node_name)
    assert osd_ids, f"The node {osd_node_name} does not have osd pods"

    ocs_version = config.ENV_DATA["ocs_version"]
    assert not (
        len(osd_ids) > 1 and Version.coerce(ocs_version) <= Version.coerce("4.6")
    ), (
        f"We have {len(osd_ids)} osd ids, and ocs version is {ocs_version}. "
        f"The ocs-osd-removal job works with multiple ids only from ocs version 4.7"
    )

    osd_id = osd_ids[0]
    log.info(f"osd ids to remove = {osd_ids}")
    # Save the node hostname before deleting the node
    osd_node_hostname_label = get_node_hostname_label(osd_node)

    log.info("Scale down node deployments...")
    scale_down_deployments(osd_node_name)
    log.info("Scale down deployments finished successfully")

    new_node_name = delete_and_create_osd_node_vsphere_upi(
        osd_node_name, use_existing_node
    )
    assert new_node_name, "Failed to create a new node"
    log.info(f"New node created successfully. Node name: {new_node_name}")

    num_of_new_pvs = len(osd_ids)
    log.info(f"Number of the expected new pvs = {num_of_new_pvs}")
    # If we use LSO, we need to create and attach a new disk manually
    new_node = get_node_objs(node_names=[new_node_name])[0]
    for i in range(num_of_new_pvs):
        add_disk_to_node(new_node)

    new_node_hostname_label = get_node_hostname_label(new_node)
    log.info(
        "Replace the old node with the new worker node in localVolumeDiscovery and localVolumeSet"
    )
    res = replace_old_node_in_lvd_and_lvs(
        old_node_name=osd_node_hostname_label,
        new_node_name=new_node_hostname_label,
    )
    assert res, "Failed to add the new node to LVD and LVS"

    log.info("Verify new pvs are available...")
    is_new_pvs_available = verify_new_pvs_available_in_sc(
        old_pv_objs, sc_name, num_of_new_pvs=num_of_new_pvs
    )
    assert is_new_pvs_available, "New pvs are not available"
    log.info("Finished verifying that the new pv is available")

    osd_removal_job = pod.run_osd_removal_job(osd_ids)
    assert osd_removal_job, "ocs-osd-removal failed to create"
    is_completed = (pod.verify_osd_removal_job_completed_successfully(osd_id),)
    assert is_completed, "ocs-osd-removal-job is not in status 'completed'"
    log.info("ocs-osd-removal-job completed successfully")

    expected_num_of_deleted_pvs = [0, num_of_new_pvs]
    num_of_deleted_pvs = delete_released_pvs_in_sc(sc_name)
    assert num_of_deleted_pvs in expected_num_of_deleted_pvs, (
        f"num of deleted PVs is {num_of_deleted_pvs} "
        f"instead of the expected values {expected_num_of_deleted_pvs}"
    )
    log.info(f"num of deleted PVs is {num_of_deleted_pvs}")
    log.info("Successfully deleted old pvs")

    is_deleted = pod.delete_osd_removal_job(osd_id)
    assert is_deleted, "Failed to delete ocs-osd-removal-job"
    log.info("ocs-osd-removal-job deleted successfully")

    return new_node_name


def label_nodes(nodes, label=constants.OPERATOR_NODE_LABEL):
    """
    Label nodes

    Args:
        nodes (list): list of node objects need to label
        label (str): New label to be assigned for these nodes.
            Default value is the OCS label

    """
    node_obj = ocp.OCP(kind="node")
    for new_node_to_label in nodes:
        node_obj.add_label(resource_name=new_node_to_label.name, label=label)
        log.info(
            f"Successfully labeled {new_node_to_label.name} " f"with OCS storage label"
        )


def get_master_nodes():
    """
    Fetches all master nodes.

    Returns:
        list: List of names of master nodes

    """
    label = "node-role.kubernetes.io/master"
    ocp_node_obj = ocp.OCP(kind=constants.NODE)
    nodes = ocp_node_obj.get(selector=label).get("items")
    master_nodes_list = [node.get("metadata").get("name") for node in nodes]
    return master_nodes_list


def get_worker_nodes():
    """
    Fetches all worker nodes.
    In case of HCI provider cluster, it will exclude the master nodes.

    Returns:
        list: List of names of worker nodes

    """
    from ocs_ci.ocs.cluster import is_hci_provider_cluster

    label = "node-role.kubernetes.io/worker"
    ocp_node_obj = ocp.OCP(kind=constants.NODE)
    nodes = ocp_node_obj.get(selector=label).get("items")
    # Eliminate infra nodes from worker nodes in case of openshift dedicated
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        infra_nodes = ocp_node_obj.get(selector=constants.INFRA_NODE_LABEL).get("items")
        infra_node_ids = [
            infra_node.get("metadata").get("name") for infra_node in infra_nodes
        ]
        nodes = [
            node
            for node in nodes
            if node.get("metadata").get("name") not in infra_node_ids
        ]
    worker_nodes_list = [node.get("metadata").get("name") for node in nodes]
    if is_hci_provider_cluster():
        master_node_list = get_master_nodes()
        worker_nodes_list = list(set(worker_nodes_list) - set(master_node_list))
    return worker_nodes_list


def get_worker_nodes_not_in_ocs():
    """
    Get the worker nodes that are not ocs labeled.

    Returns:
          list: list of worker node objects that are not ocs labeled

    """
    ocs_nodes = get_ocs_nodes()
    ocs_node_names = [n.name for n in ocs_nodes]
    worker_nodes = get_nodes(constants.WORKER_MACHINE)
    return [n for n in worker_nodes if n.name not in ocs_node_names]


def node_replacement_verification_steps_user_side(
    old_node_name, new_node_name, new_osd_node_name, old_osd_ids
):
    """
    Check the verification steps that the user should perform after the process
    of node replacement as described in the docs

    Args:
        old_node_name (str): The name of the old node that has been deleted
        new_node_name (str): The name of the new node that has been created
        new_osd_node_name (str): The name of the new node that has been added to osd nodes
        old_osd_ids (list): List of the old osd ids

    Returns:
        bool: True if all the verification steps passed. False otherwise

    """
    ocs_nodes = get_ocs_nodes()
    ocs_node_names = [n.name for n in ocs_nodes]
    if new_node_name not in ocs_node_names:
        log.warning("The new node not found in ocs nodes")
        return False
    if (
        old_node_name in ocs_node_names
        and config.ENV_DATA["platform"].lower() != constants.VSPHERE_PLATFORM
    ):
        log.warning("The old node name found in ocs nodes")
        return False

    csi_cephfsplugin_pods = pod.get_plugin_pods(interface=constants.CEPHFILESYSTEM)
    csi_rbdplugin_pods = pod.get_plugin_pods(interface=constants.CEPHBLOCKPOOL)
    csi_plugin_pods = csi_cephfsplugin_pods + csi_rbdplugin_pods
    if not all([p.status() == constants.STATUS_RUNNING for p in csi_plugin_pods]):
        log.warning("Not all csi rbd and cephfs plugin pods in status running")
        return False

    # It can take some time until all the ocs pods are up and running
    # after the process of node replacement
    if not pod.wait_for_pods_to_be_running():
        log.warning("Not all the pods in running state")
        return False

    if new_osd_node_name:
        new_osd_node_pods = get_node_pods(
            new_osd_node_name, pods_to_search=pod.get_osd_pods()
        )
        if not new_osd_node_pods:
            log.warning("Didn't find any osd pods running on the new node")
            return False
    else:
        log.info(
            "New osd node name is not provided. Continue with the other verification steps..."
        )

    log.info("Search for the old osd ids")
    new_osd_pods = pod.get_osd_pods_having_ids(old_osd_ids)
    if len(new_osd_pods) < len(old_osd_ids):
        log.warning("Didn't find osd pods for all the osd ids")
        return False

    for osd_pod in new_osd_pods:
        osd_id = pod.get_osd_pod_id(osd_pod)
        osd_pod_node = pod.get_pod_node(osd_pod)
        if not osd_pod_node:
            log.warning(
                f"Didn't find osd node for the osd pod '{osd_pod.name}' with id '{osd_id}'"
            )
            return False

        log.info(
            f"Found new osd pod '{osd_pod.name}' with id '{osd_id}' on the node '{osd_pod_node.name}'"
        )

    log.info("Verification steps from the user side finish successfully")
    return True


def node_replacement_verification_steps_ceph_side(
    old_node_name, new_node_name, new_osd_node_name=None
):
    """
    Check the verification steps from the Ceph side, after the process
    of node replacement as described in the docs

    Args:
        old_node_name (str): The name of the old node that has been deleted
        new_node_name (str): The name of the new node that has been created
        new_osd_node_name (str): The name of the new node that has been added to osd nodes

    Returns:
        bool: True if all the verification steps passed. False otherwise

    """
    if (
        old_node_name == new_node_name
        and config.ENV_DATA["platform"].lower() != constants.VSPHERE_PLATFORM
    ):
        log.warning("Hostname didn't change")
        return False

    wait_for_nodes_status([new_node_name])
    # It can take some time until all the ocs pods are up and running
    # after the process of node replacement
    if not pod.check_pods_after_node_replacement():
        return False

    ct_pod = pod.get_ceph_tools_pod()
    ceph_osd_status = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd status")
    log.info(f"Ceph osd status: {ceph_osd_status}")
    osd_node_names = get_osd_running_nodes()
    log.info(f"osd node names: {osd_node_names}")

    if new_osd_node_name:
        wait_for_nodes_status([new_osd_node_name])
        log.info(f"New osd node name is: {new_osd_node_name}")
        node_names = [osd["host name"] for osd in ceph_osd_status["OSDs"]]
        log.info(f"Node names from ceph osd status: {node_names}")
        if new_osd_node_name not in node_names:
            log.warning("new osd node name not found in 'ceph osd status' output")
            return False
        if new_osd_node_name not in osd_node_names:
            log.warning("the new osd hostname not found in osd node names")
            return False
    else:
        log.info(
            "New osd node name is not provided. Continue with the other verification steps..."
        )

    if (
        old_node_name in ceph_osd_status
        and config.ENV_DATA["platform"].lower() != constants.VSPHERE_PLATFORM
    ):
        log.warning("old node name found in 'ceph osd status' output")
        return False

    if (
        old_node_name in osd_node_names
        and config.ENV_DATA["platform"].lower() != constants.VSPHERE_PLATFORM
    ):
        log.warning("the old hostname found in osd node names")
        return False

    from ocs_ci.ocs.cluster import check_ceph_osd_tree_after_node_replacement

    if not check_ceph_osd_tree_after_node_replacement():
        return False

    log.info("Verification steps from the ceph side finish successfully")
    return True


def is_node_labeled(node_name, label=constants.OPERATOR_NODE_LABEL):
    """
    Check if the node is labeled with a specified label.

    Args:
        node_name (str): The node name to check if it has the specific label
        label (str): The name of the label. Default value is the OCS label.

    Returns:
        bool: True if the node is labeled with the specified label. False otherwise

    """
    node_names_with_label = machine.get_labeled_nodes(label=label)
    return node_name in node_names_with_label


def taint_nodes(nodes, taint_label=None):
    """
    Taint nodes

    Args:
        nodes (list): list of node names need to taint
        taint_label (str): Taint label to be used,
            If None the constants.OPERATOR_NODE_TAINT will be used.

    """
    ocp_obj = ocp.OCP()
    taint_label = taint_label if taint_label else constants.OPERATOR_NODE_TAINT
    for node in nodes:
        command = f"adm taint node {node} {taint_label}"
        try:
            ocp_obj.exec_oc_cmd(command)
            log.info(f"Successfully tainted {node} with taint {taint_label}")
        except Exception as e:
            log.info(f"{node} was not tainted - {e}")


def check_taint_on_nodes(taint=None):
    """
    Function to check for particular taint on nodes

    Args:
        taint (str): The taint to check on nodes

    Return:
        bool: True if taint is present on node. False otherwise

    """
    taint = taint if taint else constants.OPERATOR_NODE_TAINT
    nodes = get_nodes()
    flag = -1
    for node_obj in nodes:
        if node_obj.get().get("spec").get("taints"):
            if taint in node_obj.get().get("spec").get("taints")[0].get("key"):
                log.info(f"Node {node_obj.name} has taint {taint}")
                flag = 1
        else:
            flag = 0
        return bool(flag)


def untaint_nodes(taint_label=None, nodes_to_untaint=None):
    """
    Function to remove taints from nodes

    Args:
        taint_label (str): taint to use
        nodes_to_untaint (list): list of node objs to untaint

    Return:
        bool: True if untainted, false otherwise

    """
    if check_taint_on_nodes():
        ocp = OCP()
        ocs_nodes = get_ocs_nodes()
        nodes_to_untaint = nodes_to_untaint if nodes_to_untaint else ocs_nodes
        taint = taint_label if taint_label else constants.OPERATOR_NODE_TAINT
        for node in nodes_to_untaint:
            taint_cmd = f"adm taint nodes {node.name} {taint}-"
            ocp.exec_oc_cmd(command=taint_cmd)
            log.info(f"Untainted {node.name}")
        return True
    return False


def get_node_pods(node_name, pods_to_search=None, raise_pod_not_found_error=False):
    """
    Get all the pods of a specified node

    Args:
        node_name (str): The node name to get the pods
        pods_to_search (list): list of pods to search for the node pods.
            If not specified, will search in all the pods.
        raise_pod_not_found_error (bool): If True, it raises an exception, if one of the pods
            in the pod names are not found. If False, it ignores the case of pod not found and
            returns the pod objects of the rest of the pod nodes. The default value is False

    Returns:
        list: list of all the pods of the specified node

    """
    node_pods = []
    pods_to_search = pods_to_search or pod.get_all_pods()

    for p in pods_to_search:
        try:
            if pod.get_pod_node(p).name == node_name:
                node_pods.append(p)
        # Check if the command failed because the pod not found
        except (CommandFailed, NotFoundError) as ex:
            if "not found" not in str(ex):
                raise ex
            # Check the 2 cases of pod not found error
            pod_not_found_error_message = f"Failed to get the pod node of the pod {p.name} due to the exception {ex}"
            if raise_pod_not_found_error:
                raise ResourceNotFoundError(pod_not_found_error_message)
            else:
                log.info(pod_not_found_error_message)

    return node_pods


def get_node_pods_to_scale_down(node_name):
    """
    Get the pods of a node to scale down as described in the documents
    of node replacement with LSO

    Args:
        node_name (str): The node name

    Returns:
        list: The node's pods to scale down

    """
    pods_to_scale_down = [
        *pod.get_mon_pods(),
        *pod.get_osd_pods(),
        *pod.get_mgr_pods(),
    ]

    return get_node_pods(node_name, pods_to_scale_down)


def scale_down_deployments(node_name):
    """
    Scale down the deployments of a node as described in the documents
    of node replacement with LSO

    Args:
        node_name (str): The node name

    """
    ocp = OCP(kind="node", namespace=config.ENV_DATA["cluster_namespace"])
    pods_to_scale_down = get_node_pods_to_scale_down(node_name)
    for p in pods_to_scale_down:
        deployment_name = pod.get_deployment_name(p.name)
        log.info(f"Scale down deploymet {deployment_name}")
        ocp.exec_oc_cmd(f"scale deployment {deployment_name} --replicas=0")

    log.info("Scale down rook-ceph-crashcollector")
    ocp.exec_oc_cmd(
        f"scale deployment --selector=app=rook-ceph-crashcollector,"
        f"node_name='{node_name}' --replicas=0"
    )


def get_node_index_in_local_block(node_name):
    """
    Get the node index in the node values as it appears in the local block resource

    Args:
        node_name (str): The node name to search for his index

    Returns:
        int: The node index in the nodeSelector values

    """
    ocp_lvs_obj = OCP(
        kind=constants.LOCAL_VOLUME_SET,
        namespace=defaults.LOCAL_STORAGE_NAMESPACE,
        resource_name=constants.LOCAL_BLOCK_RESOURCE,
    )
    node_selector = ocp_lvs_obj.get().get("spec").get("nodeSelector")
    node_values = (
        node_selector.get("nodeSelectorTerms")[0]
        .get("matchExpressions")[0]
        .get("values")
    )
    return node_values.index(node_name)


def replace_old_node_in_lvd_and_lvs(old_node_name, new_node_name):
    """
    Replace the old node with the new node in localVolumeDiscovery and localVolumeSet,
    as described in the documents of node replacement with LSO

    Args:
        old_node_name (str): The old node name to remove from the local volume
        new_node_name (str): the new node name to add to the local volume

    Returns:
        bool: True in case if changes are applied. False otherwise

    """
    old_node_index = get_node_index_in_local_block(old_node_name)
    path_to_old_node = f"/spec/nodeSelector/nodeSelectorTerms/0/matchExpressions/0/values/{old_node_index}"
    params = f"""[{{"op": "replace", "path": "{path_to_old_node}", "value": "{new_node_name}"}}]"""

    ocp_lvd_obj = OCP(
        kind=constants.LOCAL_VOLUME_DISCOVERY,
        namespace=defaults.LOCAL_STORAGE_NAMESPACE,
    )

    ocp_lvs_obj = OCP(
        kind=constants.LOCAL_VOLUME_SET,
        namespace=defaults.LOCAL_STORAGE_NAMESPACE,
        resource_name=constants.LOCAL_BLOCK_RESOURCE,
    )

    lvd_result = ocp_lvd_obj.patch(params=params, format_type="json")
    lvs_result = ocp_lvs_obj.patch(params=params, format_type="json")

    return lvd_result and lvs_result


def get_node_hostname_label(node_obj):
    """
    Get the hostname label of a node

    Args:
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object

    Returns:
        str: The node's hostname label

    """
    return node_obj.get().get("metadata").get("labels").get(constants.HOSTNAME_LABEL)


def wait_for_new_osd_node(old_osd_node_names, timeout=600):
    """
    Wait for the new osd node to appear.

    Args:
        old_osd_node_names (list): List of the old osd node names
        timeout (int): time to wait for the new osd node to appear

    Returns:
        str: The new osd node name if the new osd node appear in the specific timeout.
            Else it returns None

    """
    pod.wait_for_pods_to_be_running(
        pod_names=[osd_pod.name for osd_pod in pod.get_osd_pods()], timeout=timeout
    )
    try:
        for current_osd_node_names in TimeoutSampler(
            timeout=timeout, sleep=30, func=get_osd_running_nodes
        ):
            new_osd_node_names = [
                node_name
                for node_name in current_osd_node_names
                if node_name not in old_osd_node_names
            ]
            if new_osd_node_names:
                log.info(f"New osd node is {new_osd_node_names[0]}")
                return new_osd_node_names[0]

    except TimeoutExpiredError:
        log.warning(f"New osd node didn't appear after {timeout} seconds")
        return None


def add_disk_to_node(node_obj, disk_size=None):
    """
    Add a new disk to a node

    Args:
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object
        disk_size (int): The size of the new disk to attach. If not specified,
            the disk size will be equal to the size of the previous disk.

    """
    from ocs_ci.ocs.platform_nodes import PlatformNodesFactory

    plt = PlatformNodesFactory()
    node_util = plt.get_nodes_platform()

    if not disk_size:
        pv_objs = get_pv_objs_in_sc(sc_name=constants.LOCAL_BLOCK_RESOURCE)
        disk_size = get_pv_size(pv_objs[-1])

    node_util.create_and_attach_volume(node=node_obj, size=disk_size)


def verify_all_nodes_created():
    """
    Verify all nodes are created or not

    Raises:
        NotAllNodesCreated: In case all nodes are not created

    """
    expected_num_nodes = (
        config.ENV_DATA["worker_replicas"] + config.ENV_DATA["master_replicas"]
    )
    raise_exception = False
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        expected_num_nodes += 3
    else:
        expected_num_nodes += config.ENV_DATA.get("infra_replicas", 0)

    existing_num_nodes = len(get_all_nodes())

    # Some nodes will take time to create due to the issue https://issues.redhat.com/browse/SDA-6346
    # Increasing the wait time for FaaS where the presence of all nodes are important
    # when DEPLOYMENT["pullsecret_workaround"] is True
    if config.ENV_DATA["platform"].lower() == constants.FUSIONAAS_PLATFORM:
        wait_time = 2400
    else:
        wait_time = 1200

    if expected_num_nodes != existing_num_nodes:
        if (
            config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM
            and config.ENV_DATA["deployment_type"] == "ipi"
        ):
            power_on_ocp_node_vms()

        platforms_to_wait = [
            constants.VSPHERE_PLATFORM,
            constants.IBMCLOUD_PLATFORM,
            constants.AZURE_PLATFORM,
        ]
        platforms_to_wait.extend(constants.MANAGED_SERVICE_PLATFORMS)
        if config.ENV_DATA[
            "platform"
        ].lower() in constants.MANAGED_SERVICE_PLATFORMS and not config.DEPLOYMENT.get(
            "pullsecret_workaround"
        ):
            log.warning(
                f"Expected number of nodes is {expected_num_nodes} but "
                f"created during deployment is {existing_num_nodes}"
            )
        elif config.ENV_DATA["platform"].lower() in platforms_to_wait and (
            config.ENV_DATA["deployment_type"] == "ipi"
            or config.ENV_DATA["deployment_type"] == "managed"
        ):
            try:

                for node_list in TimeoutSampler(
                    timeout=wait_time, sleep=60, func=get_all_nodes
                ):
                    existing_num_nodes = len(node_list)
                    if existing_num_nodes == expected_num_nodes:
                        log.info(
                            f"All {expected_num_nodes} nodes are created successfully."
                        )
                        break
                    else:
                        log.warning(
                            f"waiting for {expected_num_nodes} nodes to create but found {existing_num_nodes} nodes"
                        )
            except TimeoutExpiredError:
                log.error(f"Expected {expected_num_nodes} nodes are not created")
                raise_exception = True
        else:
            raise_exception = True

    if raise_exception:
        raise NotAllNodesCreated(
            f"Expected number of nodes is {expected_num_nodes} but "
            f"created during deployment is {existing_num_nodes}"
        )


def power_on_ocp_node_vms():
    """
    Power on OCP node VM's, this function will directly interact with vCenter.
    This function will make sure all cluster node VM's are powered on.
    """
    vsp = VSPHERE(
        config.ENV_DATA["vsphere_server"],
        config.ENV_DATA["vsphere_user"],
        config.ENV_DATA["vsphere_password"],
    )
    vms_dc = vsp.get_all_vms_in_dc(config.ENV_DATA["vsphere_datacenter"])
    cluster_name = config.ENV_DATA["cluster_name"]
    vms_ipi = []
    for vm in vms_dc:
        if cluster_name in vm.name and "rhcos-generated" not in vm.name:
            vms_ipi.append(vm)
            log.debug(vm.name)

    for vm in vms_ipi:
        power_status = vsp.get_vm_power_status(vm)
        if power_status == constants.VM_POWERED_OFF:
            vsp.start_vms(vms=[vm])


def add_node_to_lvd_and_lvs(node_name):
    """
    Add a new node to localVolumeDiscovery and localVolumeSet

    Args:
        node_name (str): the new node name to add to localVolumeDiscovery and localVolumeSet

    Returns:
        bool: True in case the changes are applied successfully. False otherwise

    """
    path_to_nodes = "/spec/nodeSelector/nodeSelectorTerms/0/matchExpressions/0/values/-"
    params = f"""[{{"op": "add", "path": "{path_to_nodes}", "value": "{node_name}"}}]"""

    ocp_lvd_obj = OCP(
        kind=constants.LOCAL_VOLUME_DISCOVERY,
        namespace=defaults.LOCAL_STORAGE_NAMESPACE,
    )

    ocp_lvs_obj = OCP(
        kind=constants.LOCAL_VOLUME_SET,
        namespace=defaults.LOCAL_STORAGE_NAMESPACE,
        resource_name=constants.LOCAL_BLOCK_RESOURCE,
    )

    lvd_result = ocp_lvd_obj.patch(params=params, format_type="json")
    lvs_result = ocp_lvs_obj.patch(params=params, format_type="json")

    return lvd_result and lvs_result


def add_new_nodes_and_label_upi_lso(
    node_type,
    num_nodes,
    mark_for_ocs_label=True,
    node_conf=None,
    add_disks=True,
    add_nodes_to_lvs_and_lvd=True,
):
    """
    Add a new node for aws/vmware upi lso platform and label it

    Args:
        node_type (str): Type of node, RHEL or RHCOS
        num_nodes (int): number of nodes to add
        mark_for_ocs_label (bool): True if label the new nodes
        node_conf (dict): The node configurations.
        add_disks (bool): True if add disks to the new nodes.
        add_nodes_to_lvs_and_lvd (bool): True if add the new nodes to
            localVolumeDiscovery and localVolumeSet.

    Returns:
        list: new spun node names

    """
    new_node_names = add_new_node_and_label_upi(
        node_type, num_nodes, mark_for_ocs_label, node_conf
    )
    new_nodes = get_node_objs(new_node_names)

    if add_disks:
        for node_obj in new_nodes:
            add_disk_to_node(node_obj)

    if add_nodes_to_lvs_and_lvd:
        for node_obj in new_nodes:
            add_node_to_lvd_and_lvs(node_obj.name)

    return new_node_names


def get_nodes_in_statuses(statuses, node_objs=None):
    """
    Get all nodes in specific statuses

    Args:
        statuses (list): List of the statuses to search for the nodes
        node_objs (list): The node objects to check their statues. If not specified,
            it gets all the nodes.

    Returns:
        list: OCP objects representing the nodes in the specific statuses

    """
    if not node_objs:
        node_objs = get_node_objs()

    nodes_in_statuses = []
    for n in node_objs:
        try:
            node_status = get_node_status(n)
        except CommandFailed as e:
            log.warning(f"Failed to get the node status due to the error: {str(e)}")
            continue

        if node_status in statuses:
            nodes_in_statuses.append(n)

    return nodes_in_statuses


def get_node_osd_ids(node_name):
    """
    Get the node osd ids

    Args:
        node_name (str): The node name to get the osd ids

    Returns:
        list: The list of the osd ids

    """
    osd_pods = pod.get_osd_pods()
    node_osd_pods = get_node_pods(node_name, pods_to_search=osd_pods)
    return [pod.get_osd_pod_id(osd_pod) for osd_pod in node_osd_pods]


def get_node_mon_ids(node_name):
    """
    Get the node mon ids

    Args:
        node_name (str): The node name to get the mon ids

    Returns:
        list: The list of the mon ids

    """
    mon_pods = pod.get_mon_pods()
    node_mon_pods = get_node_pods(node_name, pods_to_search=mon_pods)
    return [pod.get_mon_pod_id(mon_pod) for mon_pod in node_mon_pods]


def get_mon_running_nodes():
    """
    Gets the mon running node names

    Returns:
        list: MON node names

    """
    return [pod.get_pod_node(mon_pod).name for mon_pod in pod.get_mon_pods()]


def get_nodes_where_ocs_pods_running():
    """
    Get the node names where rook ceph pods are running

    Returns:
        set: node names where rook ceph pods are running

    """
    pods_openshift_storage = pod.get_all_pods(
        namespace=config.ENV_DATA["cluster_namespace"]
    )
    ocs_nodes = list()
    for pod_obj in pods_openshift_storage:
        if (
            "rook-ceph" in pod_obj.name
            and "rook-ceph-operator" not in pod_obj.name
            and "rook-ceph-tool" not in pod_obj.name
        ):
            try:
                ocs_nodes.append(pod_obj.data["spec"]["nodeName"])
            except Exception as e:
                log.info(e)
    return set(ocs_nodes)


def get_node_rack(node_obj):
    """
    Get the worker node rack

    Args:
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object

    Returns:
        str: The worker node rack name

    """
    return node_obj.data["metadata"]["labels"].get("topology.rook.io/rack")


def get_node_rack_dict():
    """
    Get worker node rack

    Returns:
        dict: {"Node name":"Rack name"}

    """
    worker_node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
    node_rack_dict = dict()
    for worker_node_obj in worker_node_objs:
        node_rack_dict[worker_node_obj.name] = get_node_rack(worker_node_obj)
    log.info(f"node-rack dictinary {node_rack_dict}")
    return node_rack_dict


def get_node_zone(node_obj):
    """
    Get the worker node zone

    Args:
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object

    Returns:
        str: The worker node zone name

    """
    return node_obj.data["metadata"]["labels"].get(
        "failure-domain.beta.kubernetes.io/zone"
    )


def get_node_zone_dict():
    """
    Get worker node zone dictionary

    Returns:
        dict: {"Node name":"Zone name"}

    """
    node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
    node_zone_dict = dict()
    for node_obj in node_objs:
        node_zone_dict[node_obj.name] = get_node_zone(node_obj)
    log.info(f"node-zone dictionary {node_zone_dict}")
    return node_zone_dict


def get_node_rack_or_zone(failure_domain, node_obj):
    """
    Get the worker node rack or zone name based on the failure domain value

    Args:
        failure_domain (str): The failure domain
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object

    Returns:
        str: The worker node rack/zone name

    """
    return (
        get_node_zone(node_obj) if failure_domain == "zone" else get_node_rack(node_obj)
    )


def get_node_rack_or_zone_dict(failure_domain):
    """
    Get worker node rack or zone dictionary based on the failure domain value

    Args:
        failure_domain (str): The failure domain

    Returns:
        dict: {"Node name":"Zone/Rack name"}

    """
    return get_node_zone_dict() if failure_domain == "zone" else get_node_rack_dict()


def get_node_names(node_type=constants.WORKER_MACHINE):
    """
    Get node names

    Args:
        node_type (str): The node type (e.g. worker, master)

    Returns:
        list: The node names

    """
    log.info(f"Get {node_type} Node names")
    node_objs = get_nodes(node_type=node_type)
    return [node_obj.name for node_obj in node_objs]


def get_crashcollector_nodes():
    """
    Get the nodes names where crashcollector pods are running

    return:
        set: node names where crashcollector pods are running

    """
    crashcollector_pod_objs = pod.get_crashcollector_pods()
    crashcollector_ls = [
        crashcollector_pod_obj.data["spec"]["nodeName"]
        for crashcollector_pod_obj in crashcollector_pod_objs
    ]
    return set(crashcollector_ls)


def add_new_disk_for_vsphere(sc_name):
    """
    Check the PVS in use per node, and add a new disk to the worker node with the minimum PVS.

    Args:
        sc_name (str): The storage class name

    """
    ocs_nodes = get_ocs_nodes()
    num_of_pv_per_node_tuples = [
        (len(get_node_pv_objs(sc_name, n.name)), n) for n in ocs_nodes
    ]
    node_with_min_pvs = min(num_of_pv_per_node_tuples, key=itemgetter(0))[1]
    add_disk_to_node(node_with_min_pvs)


def add_disk_stretch_arbiter():
    """
    Adds disk to storage nodes in a stretch cluster with arbiter
    configuration evenly spread across two zones. Stretch cluster has
    replica 4, hence 2 disks to each of the zones

    """

    from ocs_ci.ocs.resources.stretchcluster import StretchCluster

    data_zones = constants.DATA_ZONE_LABELS
    sc_obj = StretchCluster()

    for zone in data_zones:
        for node in sc_obj.get_ocs_nodes_in_zone(zone)[:2]:
            add_disk_to_node(node)


def get_odf_zone_count():
    """
    Get the number of Availability zones used by ODF cluster

    Returns:
         int : the number of availability zones
    """
    node_obj = OCP(kind="node")
    az_count = node_obj.get(selector=constants.ZONE_LABEL)
    az = set()
    for node in az_count.get("items"):
        node_lables = node.get("metadata")["labels"]
        if "cluster.ocs.openshift.io/openshift-storage" in node_lables:
            az.add(node.get("metadata")["labels"][constants.ZONE_LABEL])
    return len(az)


def get_node_status(node_obj):
    """
    Get the node status.

    Args:
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object

    Return:
        str: The node status. If the command failed, it returns None.

    """
    return node_obj.ocp.get_resource_status(node_obj.name)


def recover_node_to_ready_state(node_obj):
    """
    Recover the node to be in Ready state.

    Args:
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object

    Return:
        bool: True if the node recovered to Ready state. False otherwise

    """
    from ocs_ci.ocs.platform_nodes import PlatformNodesFactory

    plt = PlatformNodesFactory()
    node_util = plt.get_nodes_platform()

    try:
        node_status = get_node_status(node_obj)
    except Exception as e:
        log.info(f"failed to get the node status due to the exception {str(e)}")
        return False

    node_name = node_obj.name
    log.info(f"The status of the node {node_name} is {node_status} ")

    if node_status == constants.NODE_READY:
        log.info(
            f"The node {node_name} is already in the expected status {constants.NODE_READY}"
        )
        return True

    try:
        if node_status == constants.NODE_NOT_READY:
            log.info(f"Starting the node {node_name}...")
            node_util.start_nodes(nodes=[node_obj], wait=True)
            log.info(f"Successfully started the node {node_name}")
        elif node_status == constants.NODE_READY_SCHEDULING_DISABLED:
            log.info(f"Schedule the node {node_name}...")
            schedule_nodes(node_names=[node_name])
            log.info(f"Successfully schedule the node {node_name}")
        elif node_status == constants.NODE_NOT_READY_SCHEDULING_DISABLED:
            log.info(f"Schedule and start the node {node_name}...")
            schedule_nodes(node_names=[node_name])
            node_util.start_nodes(nodes=[node_obj], wait=True)
            log.info(f"Successfully schedule and started the node {node_name}")
        else:
            log.warning(
                f"The node {node_name} is not in the expected statuses. "
                f"Trying to force stop and start the node..."
            )
            node_util.restart_nodes_by_stop_and_start(nodes=[node_obj], force=True)
    except Exception as e:
        log.warning(f"Operation failed due to exception: {str(e)}")

    try:
        wait_for_nodes_status(node_names=[node_name], timeout=60)
        log.info(f"The node {node_name} reached status {constants.NODE_READY}")
        res = True
    except exceptions.ResourceWrongStatusException:
        log.warning(
            f"The node {node_name} failed to reach status {constants.NODE_READY}"
        )
        res = False

    return res


def add_new_nodes_and_label_after_node_failure_ipi(
    machineset_name, num_nodes=1, mark_for_ocs_label=True
):
    """
    Add new nodes for ipi and label them after node failure

    Args:
        machineset_name (str): Name of the machine set
        num_nodes (int): number of nodes to add
        mark_for_ocs_label (bool): True if label the new node

    Returns:
        list: new spun node names

    """
    machine.change_current_replica_count_to_ready_replica_count(machineset_name)
    return add_new_node_and_label_it(machineset_name, num_nodes, mark_for_ocs_label)


def get_encrypted_osd_devices(node_obj, node):
    """
    Get osd encrypted device names of a node

    Args:
        node_obj: OCP object of kind node
        node: node name

    Returns:
        List of encrypted osd device names
    """
    luks_devices_out = node_obj.exec_oc_debug_cmd(
        node=node,
        cmd_list=[
            "lsblk -o NAME,TYPE,FSTYPE | grep -E 'disk.*crypto_LUKS' | awk '{print $1}'"
        ],
    ).split("\n")
    luks_devices = [device for device in luks_devices_out if device != ""]
    return luks_devices


def get_osd_ids_per_node():
    """
    Get a dictionary of the osd ids per node

    Returns:
        dict: The dictionary of the osd ids per node

    """
    osd_node_names = get_osd_running_nodes()
    return {node_name: get_node_osd_ids(node_name) for node_name in osd_node_names}


def get_node_rook_ceph_pod_names(node_name):
    """
    Get the rook ceph pod names associated with the node

    Args:
        node_name (str): The node name

    Returns:
        list: The rook ceph pod names associated with the node

    """
    rook_ceph_pods = pod.get_pod_objs(pod.get_rook_ceph_pod_names())
    node_rook_ceph_pods = get_node_pods(node_name, rook_ceph_pods)
    return [p.name for p in node_rook_ceph_pods]


def get_node_internal_ip(node_obj):
    """
    Get the node internal ip

    Args:
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object

    Returns:
        str: The node internal ip or `None`

    """
    addresses = node_obj.get().get("status").get("addresses")
    for address in addresses:
        if address["type"] == "InternalIP":
            return address["address"]

    return None


def check_node_ip_equal_to_associated_pods_ips(node_obj):
    """
    Check that the node ip is equal to the pods ips associated with the node.
    This function is mainly for the managed service deployment.

    Args:
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object

    Returns:
        bool: True, if the node ip is equal to the pods ips associated with the node.
            False, otherwise.

    """
    rook_ceph_pod_names = pod.get_rook_ceph_pod_names()
    rook_ceph_pod_names = [
        pod_name
        for pod_name in rook_ceph_pod_names
        if not pod_name.startswith("rook-ceph-operator")
    ]
    rook_ceph_pods = pod.get_pod_objs(rook_ceph_pod_names)
    node_rook_ceph_pods = get_node_pods(node_obj.name, rook_ceph_pods)
    node_ip = get_node_internal_ip(node_obj)
    return all([pod.get_pod_ip(p) == node_ip for p in node_rook_ceph_pods])


def verify_worker_nodes_security_groups():
    """
    Check the worker nodes security groups set correctly.
    The function checks that the pods ip are equal to their associated nodes.

    Returns:
        bool: True, if the worker nodes security groups set correctly. False otherwise

    """
    wnodes = get_nodes(constants.WORKER_MACHINE)
    for wnode in wnodes:
        if not check_node_ip_equal_to_associated_pods_ips(wnode):
            log.warning(f"The node {wnode.name} security groups is not set correctly")
            return False

    log.info("All the worker nodes security groups are set correctly")
    return True


def wait_for_osd_ids_come_up_on_node(
    node_name, expected_osd_ids, timeout=180, sleep=10
):
    """
    Wait for the expected osd ids to come up on a node

    Args:
        node_name (str): The node name
        expected_osd_ids (list): The list of the expected osd ids to come up on the node
        timeout (int): Time to wait for the osd ids to come up on the node
        sleep (int): Time in seconds to sleep between attempts

    Returns:
        bool: True, the osd ids to come up on the node. False, otherwise

    """
    try:
        for osd_ids in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_node_osd_ids,
            node_name=node_name,
        ):
            log.info(f"the current node {node_name} osd ids are: {osd_ids}")
            if osd_ids == expected_osd_ids:
                log.info(
                    f"The node {node_name} has the expected osd ids {expected_osd_ids}"
                )
                return True

    except TimeoutExpiredError:
        log.warning(
            f"The node {node_name} didn't have the expected osd ids {expected_osd_ids}"
        )

    return False


def wait_for_all_osd_ids_come_up_on_nodes(
    expected_osd_ids_per_node, timeout=360, sleep=20
):
    """
    Wait for all the expected osd ids to come up on their associated nodes

    Args:
        expected_osd_ids_per_node (dict): The expected osd ids per node
        timeout (int): Time to wait for all the expected osd ids to come up on
            their associated nodes
        sleep (int): Time in seconds to sleep between attempts

    Returns:
        bool: True, if all the expected osd ids come up on their associated nodes.
            False, otherwise

    """
    try:
        for osd_ids_per_node in TimeoutSampler(
            timeout=timeout, sleep=sleep, func=get_osd_ids_per_node
        ):
            log.info(f"the current osd ids per node: {osd_ids_per_node}")
            if osd_ids_per_node == expected_osd_ids_per_node:
                log.info(
                    f"The osd ids per node reached the expected values: "
                    f"{expected_osd_ids_per_node}"
                )
                return True

    except TimeoutExpiredError:
        log.warning(
            f"The osd ids per node didn't reach the expected values: "
            f"{expected_osd_ids_per_node}"
        )

    return False


def get_other_worker_nodes_in_same_rack_or_zone(
    failure_domain, node_obj, node_names_to_search=None
):
    """
    Get other worker nodes in the same rack or zone of a given node.

    Args:
        failure_domain (str): The failure domain
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object to search for other
            worker nodes in the same rack or zone.
        node_names_to_search (list): The list of node names to search the other worker nodes in
            the same rack or zone. If not specified, it will search in all the worker nodes.

    Returns:
        list: The list of the other worker nodes in the same rack or zone of the given node.

    """
    node_rack_or_zone = get_node_rack_or_zone(failure_domain, node_obj)
    log.info(f"The node {node_obj.name} rack or zone is {node_rack_or_zone}")
    wnode_names = node_names_to_search or get_worker_nodes()
    other_wnode_names = [name for name in wnode_names if name != node_obj.name]
    other_wnodes = get_node_objs(other_wnode_names)

    other_wnodes_in_same_rack_or_zone = [
        wnode
        for wnode in other_wnodes
        if get_node_rack_or_zone(failure_domain, wnode) == node_rack_or_zone
    ]

    wnode_names = [n.name for n in other_wnodes_in_same_rack_or_zone]
    log.info(f"other worker nodes in the same rack or zone are: {wnode_names}")
    return other_wnodes_in_same_rack_or_zone


def get_another_osd_node_in_same_rack_or_zone(
    failure_domain, node_obj, node_names_to_search=None
):
    """
    Get another osd node in the same rack or zone of a given node.

    Args:
        failure_domain (str): The failure domain
        node_obj (ocs_ci.ocs.resources.ocs.OCS): The node object to search for another
            osd node in the same rack or zone.
        node_names_to_search (list): The list of node names to search for another osd node in the
            same rack or zone. If not specified, it will search in all the worker nodes.

    Returns:
        ocs_ci.ocs.resources.ocs.OCS: The osd node in the same rack or zone of the given node.
            If not found, it returns None.

    """
    osd_node_names = get_osd_running_nodes()
    other_wnodes_in_same_rack_or_zone = get_other_worker_nodes_in_same_rack_or_zone(
        failure_domain, node_obj, node_names_to_search
    )

    osd_node_in_same_rack_or_zone = None
    for wnode in other_wnodes_in_same_rack_or_zone:
        if wnode.name in osd_node_names:
            osd_node_in_same_rack_or_zone = wnode
            break

    return osd_node_in_same_rack_or_zone


def get_nodes_racks_or_zones(failure_domain, node_names):
    """
    Get the nodes racks or zones

    failure_domain (str): The failure domain
    node_names (list): The node names to get their racks or zones

    Return:
        list: The nodes racks or zones

    """
    node_objects = get_node_objs(node_names)
    return [get_node_rack_or_zone(failure_domain, n) for n in node_objects]


def wait_for_nodes_racks_or_zones(failure_domain, node_names, timeout=120):
    """
    Wait for the nodes racks or zones to appear

    Args:
        failure_domain (str): The failure domain
        node_names (list): The node names to get their racks or zones
        timeout (int): The time to wait for the racks or zones to appear on the nodes

    Raise:
        TimeoutExpiredError: In case not all the nodes racks or zones appear in the given timeout

    """
    for nodes_racks_or_zones in TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=get_nodes_racks_or_zones,
        failure_domain=failure_domain,
        node_names=node_names,
    ):
        log.info(f"The nodes {node_names} racks or zones are: {nodes_racks_or_zones}")
        if all(nodes_racks_or_zones):
            log.info("All the nodes racks or zones exist!")
            break


def wait_for_new_worker_node_ipi(machineset, old_wnodes, timeout=900):
    """
    Wait for the new worker node to be ready

    Args:
        machineset (str): The machineset name
        old_wnodes (list): The old worker nodes
        timeout (int): Time to wait for the new worker node to be ready.

    Returns:
        ocs_ci.ocs.resources.ocs.OCS: The new worker node object

    Raise:
        ResourceWrongStatusException: In case the new spun machine fails to reach Ready state
            or replica count didn't match. Or in case one or more nodes haven't reached
            the desired state.

    """
    machine.wait_for_new_node_to_be_ready(machineset, timeout=timeout)
    new_wnode_names = list(set(get_worker_nodes()) - set(old_wnodes))
    new_wnode = get_node_objs(new_wnode_names)[0]
    log.info(f"Successfully created a new node {new_wnode.name}")

    wait_for_nodes_status([new_wnode.name])
    log.info(f"The new worker node {new_wnode.name} is in a Ready state!")
    return new_wnode


def wait_for_node_count_to_reach_status(
    node_count,
    node_type=constants.WORKER_MACHINE,
    expected_status=constants.STATUS_READY,
    timeout=300,
    sleep=20,
):
    """
    Wait for a node count to reach the expected status

    Args:
        node_count (int): The node count
        node_type (str): The node type. Default value is worker.
        expected_status (str): The expected status. Default value is "Ready".
        timeout (int): Time to wait for the node count to reach the expected status.
        sleep (int): Time in seconds to wait between attempts.

    Raise:
        TimeoutExpiredError: In case the node count didn't reach the expected status in the given timeout.

    """
    log.info(
        f"Wait for {node_count} of the nodes to reach the expected status {expected_status}"
    )

    for node_objs in TimeoutSampler(
        timeout=timeout, sleep=sleep, func=get_nodes, node_type=node_type
    ):
        nodes_in_expected_statuses = get_nodes_in_statuses([expected_status], node_objs)
        node_names_in_expected_status = [n.name for n in nodes_in_expected_statuses]
        if len(node_names_in_expected_status) == node_count:
            log.info(
                f"{node_count} of the nodes reached the expected status: {expected_status}"
            )
            break
        else:
            log.info(
                f"The nodes {node_names_in_expected_status} reached the expected status {expected_status}, "
                f"but we were waiting for {node_count} of them to reach status {expected_status}"
            )


def check_for_zombie_process_on_node(node_name=None):
    """
    Check if there are any zombie process on the nodes

    Args:
        node_name (list): Node names list to check for zombie process

    Raise:
        ZombieProcessFoundException: In case zombie process are found on the node

    """
    node_obj_list = get_node_objs(node_name) if node_name else get_node_objs()
    for node_obj in node_obj_list:
        debug_cmd = (
            f"debug nodes/{node_obj.name} --to-namespace={config.ENV_DATA['cluster_namespace']} "
            '-- chroot /host /bin/bash -c "ps -A -ostat,pid,ppid | grep -e [zZ]"'
        )
        out = node_obj.ocp.exec_oc_cmd(
            command=debug_cmd, ignore_error=True, out_yaml_format=False
        )
        if not out:
            log.info(f"No Zombie process found on the node: {node_obj.name}")
        else:
            log.error(f"Zombie process found on node: {node_obj.name}")
            log.error(f"pid and ppid details: {out}")
            raise exceptions.ZombieProcessFoundException


@switch_to_orig_index_at_last
def get_provider_internal_node_ips(
    node_type=constants.WORKER_MACHINE, num_of_nodes=None
):
    """
    Get the provider node ips according to the node type (e.g. worker, master) and the
    number of requested node ips from that type

    Args:
        node_type (str): The node type (e.g. worker, master)
        num_of_nodes (int): The number of node ips to be returned

    Returns:
        list: The node ips

    """
    log.info("Switching to the provider cluster to get the provider node ips")
    config.switch_to_provider()
    provider_nodes = get_nodes(node_type, num_of_nodes)
    provider_node_ips = [get_node_internal_ip(n) for n in provider_nodes]
    log.info(f"Provider node ips: {provider_node_ips}")
    return provider_node_ips


def consumer_verification_steps_after_provider_node_replacement():
    """
    Check the consumer verification steps after a provider node replacement
    The steps are as follows:

    1. Check if the consumer "storageProviderEndpoint" IP is found in the provider worker node ips
        1.1 If not found, edit the rosa addon installation - modify the param 'storage-provider-endpoint'
            with the first provider worker node IP.
        1.2 Wait and check again if the consumer "storageProviderEndpoint" IP is found in the
            provider worker node IPs.
    2. If found, Check if the mon endpoint ips are found in the provider worker node ips, and we can execute
        a ceph command from the consumer
        2.1 If not found, restart the ocs-operator pod
        2.2 Wait and check again if the mon endpoint ips are found in the provider worker node ips, and we
        can execute a ceph command from the consumer

    Returns:
        bool: True, if the consumer verification steps finished successfully. False, otherwise.

    """
    # Import inside the function to avoid circular loop
    from ocs_ci.ocs.resources.storage_cluster import (
        get_consumer_storage_provider_endpoint,
        check_consumer_storage_provider_endpoint_in_provider_wnodes,
        wait_for_consumer_storage_provider_endpoint_in_provider_wnodes,
        wait_for_consumer_rook_ceph_mon_endpoints_in_provider_wnodes,
    )

    if not check_consumer_storage_provider_endpoint_in_provider_wnodes():
        sp_endpoint_suffix = get_consumer_storage_provider_endpoint().split(":")[-1]
        wnode_ip = get_provider_internal_node_ips()[0]

        edit_addon_installation(
            addon_param_key="storage-provider-endpoint",
            addon_param_value=f"{wnode_ip}:{sp_endpoint_suffix}",
            wait=False,
        )
        try:
            wait_for_addon_to_be_ready()
        except TimeoutExpiredError:
            log.warning("The consumer addon is not in a ready state")
            log.info("Try to restart the ocs-operator pod")
            pod.delete_pods([pod.get_ocs_operator_pod()])
            log.info("Wait again for the consumer addon to be in ready state")
            try:
                wait_for_addon_to_be_ready()
            except TimeoutExpiredError:
                log.warning("The consumer addon is not in a ready state")
                return False

        if not wait_for_consumer_storage_provider_endpoint_in_provider_wnodes():
            log.warning(
                "The consumer storage endpoint is not found in the provider worker node ips"
            )
            return False

    log.info(
        "Check if the mon endpoint ips are found in the provider worker node ips "
        "and we can execute a ceph command from the consumer"
    )
    if not (
        wait_for_consumer_rook_ceph_mon_endpoints_in_provider_wnodes()
        and pod.wait_for_ceph_cmd_execute_successfully()
    ):
        # This is a workaround due to the BZ https://bugzilla.redhat.com/show_bug.cgi?id=2131581
        log.info("Try to restart the ocs-operator pod")
        pod.delete_pods([pod.get_ocs_operator_pod()])

        log.info(
            "Check that the mon endpoint ips are found in the provider worker node ips"
        )
        if not wait_for_consumer_rook_ceph_mon_endpoints_in_provider_wnodes():
            log.warning(
                "Did not find all the mon endpoint ips in the provider worker node ips"
            )
            return False
        log.info("Check again if we can execute a ceph command from the consumer")
        if not pod.wait_for_ceph_cmd_execute_successfully():
            log.warning("Failed to execute the ceph command")
            return False

    log.info("The consumer verification steps finished successfully")
    return True


@switch_to_orig_index_at_last
def consumers_verification_steps_after_provider_node_replacement():
    """
    Check all the consumers verification steps after a provider node replacement.
    On every consumer we will check the steps described in the function
    'consumer_verification_steps_after_provider_node_replacement'.

    Returns:
        bool: True, if all the consumers verification steps finished successfully. False, otherwise.

    """
    from ocs_ci.ocs.managedservice import get_provider_service_type

    if (
        version.get_semantic_ocs_version_from_config() >= version.VERSION_4_11
        and get_provider_service_type() != "NodePort"
    ):
        log.info(
            "We need to change the consumers verification steps when the ODF version is 4.11 and not using "
            "the ocs-provider-server Service type 'NodePort'. Currently, we can skip these steps"
        )
        return True

    consumer_indexes = config.get_consumer_indexes_list()
    for consumer_i in consumer_indexes:
        config.switch_ctx(consumer_i)
        if not consumer_verification_steps_after_provider_node_replacement():
            log.warning(
                f"The consumer '{config.ENV_DATA['cluster_name']}' verification steps"
                f" did not finish successfully"
            )
            return False

    log.info("All the consumers verification steps finished successfully")
    return True


def generate_nodes_for_provider_worker_node_tests():
    """
    Generate worker node objects for the provider worker node tests.

    The function generates the node objects as follows:
    - If we have 3 worker nodes in the cluster, it generates all the 3 worker nodes
    - If we have more than 3 worker nodes, it generates one mgr node, 2 mon nodes, and 2 osd nodes.
    The nodes can overlap, but at least 3 nodes should be selected(and no more than 5 nodes).

    Returns:
        list: The list of the node objects for the provider node tests

    """
    wnode_names = get_worker_nodes()
    if len(wnode_names) <= 3:
        generated_nodes = get_node_objs(wnode_names)
        log.info(f"Generated nodes for provider node tests: {wnode_names}")
        return generated_nodes

    mgr_pod = random.choice(pod.get_mgr_pods())
    mgr_node_name = pod.get_pod_node(mgr_pod).name
    mon_node_names = get_mon_running_nodes()
    if mgr_node_name in mon_node_names:
        mon_node_names.remove(mgr_node_name)
        # Set of one mgr node, and one mon node
        ceph_node_set = {mgr_node_name, random.choice(mon_node_names)}
    else:
        # Set of one mgr node, and two mon nodes
        ceph_node_set = set([mgr_node_name] + random.sample(mon_node_names, k=2))

    osd_node_set = set(get_osd_running_nodes())
    ceph_include_osd_node_set = ceph_node_set.intersection(osd_node_set)

    if len(ceph_include_osd_node_set) < 2:
        num_of_osds_to_add = 2 - len(ceph_include_osd_node_set)
    elif len(ceph_node_set) == 2:
        num_of_osds_to_add = 1
    else:
        num_of_osds_to_add = 0

    osd_exclude_ceph_node_set = osd_node_set - ceph_node_set
    osd_choices = random.sample(osd_exclude_ceph_node_set, k=num_of_osds_to_add)
    node_choice_names = list(ceph_node_set) + osd_choices

    generated_nodes = get_node_objs(node_choice_names)
    log.info(f"Generated nodes for provider node tests: {node_choice_names}")
    return generated_nodes


def gracefully_reboot_nodes(disable_eviction=False):
    """

    Gracefully reboot OpenShift Container Platform nodes

    Args:
        disable_eviction (bool): On True will delete pod that is protected by PDB, False by default

    """
    from ocs_ci.ocs import platform_nodes

    node_objs = get_node_objs()
    factory = platform_nodes.PlatformNodesFactory()
    nodes = factory.get_nodes_platform()
    waiting_time = 30
    for node in node_objs:
        node_name = node.name
        unschedule_nodes([node_name])
        drain_nodes(node_names=[node_name], disable_eviction=disable_eviction)
        nodes.restart_nodes([node], wait=False)
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(waiting_time)
        schedule_nodes([node_name])
        wait_for_nodes_status(
            node_names=[node_name], status=constants.NODE_READY, timeout=1800
        )


def get_num_of_racks():
    """
    Get the number of racks in the cluster

    Returns:
        int: The number of racks in the cluster

    """
    node_racks = get_node_rack_dict().values()
    node_racks = [rack for rack in node_racks if rack]
    return len(set(node_racks))


def generate_new_nodes_and_osd_running_nodes_ipi(
    osd_running_worker_nodes=None, num_of_nodes=2
):
    """
    Create new nodes and generate osd running worker nodes in the same machinesets as the new nodes,
    or if it's a 1AZ cluster, it generates osd running worker nodes in the same rack or zone as the new nodes.
    This function is only for an IPI deployment

    Args:
        osd_running_worker_nodes: The list to use in the function for generate osd running worker nodes.
            If not provided, it generates all the osd running worker nodes.
        num_of_nodes (int): The number of the new nodes to create and generate. The default value is 2.

    Returns:
        list: The list of the generated osd running worker nodes

    """
    osd_running_worker_nodes = osd_running_worker_nodes or get_osd_running_nodes()

    # Get the machine name using the node name
    machine_names = [
        machine.get_machine_from_node_name(osd_running_worker_node)
        for osd_running_worker_node in osd_running_worker_nodes[:num_of_nodes]
    ]
    log.info(f"{osd_running_worker_nodes} associated " f"machine are {machine_names}")

    # Get the machineset name using machine name
    machineset_names = [
        machine.get_machineset_from_machine_name(machine_name)
        for machine_name in machine_names
    ]
    log.info(
        f"{osd_running_worker_nodes[:num_of_nodes]} associated machineset is {machineset_names}"
    )

    # Add a new nodes and label it
    new_node_names = []
    machineset_names = machineset_names[:num_of_nodes]
    for mset in machineset_names:
        new_node_names.extend(add_new_node_and_label_it(mset))
    log.info(f"New added nodes: {new_node_names}")

    num_of_racks = get_num_of_racks()
    num_of_zones = get_az_count()

    if num_of_zones == 1 and num_of_racks >= 3:
        log.info(
            f"We have 1 AZ and {num_of_racks} racks in the cluster. "
            f"Get the osd worker nodes in the same racks as the newly added nodes"
        )
        new_nodes = get_node_objs(new_node_names)
        failure_domain = "rack"
        osd_running_worker_nodes = []
        for n in new_nodes:
            osd_node = get_another_osd_node_in_same_rack_or_zone(
                failure_domain, node_obj=n
            )
            osd_running_worker_nodes.append(osd_node.name)

        log.info(f"osd running worker nodes: {osd_running_worker_nodes}")

    return osd_running_worker_nodes[:num_of_nodes]


def is_node_rack_or_zone_exist(failure_domain, node_name):
    """
    Check if the node rack/zone exist

    Args:
        failure_domain (str): The failure domain
        node_name (str): The node name

    Returns:
        bool: True if the node rack/zone exist. False otherwise

    """
    node_obj = get_node_objs([node_name])[0]
    return get_node_rack_or_zone(failure_domain, node_obj) is not None


def list_encrypted_rbd_devices_onnode(node):
    """
    Get rbd crypt devices from the node

    Args:
        node: node name

    Returns:
        List of encrypted osd device names
    """
    node_obj = OCP(kind="node")
    crypt_devices_out = node_obj.exec_oc_debug_cmd(
        node=node,
        cmd_list=["lsblk | grep crypt | awk '{print $1}'"],
    ).split("\n")
    crypt_devices = [device.strip() for device in crypt_devices_out if device != ""]
    return crypt_devices


def verify_crypt_device_present_onnode(node, vol_handle):
    """
    Find the crypt device maching for given volume handle.

    Args:
        node : node name
        vol_handle : volumen handle name.

    Returns:
        True: if volume handle device found on the node.
        False: if volume handle device not found on the node.
    """
    device_list = list_encrypted_rbd_devices_onnode(node)
    crypt_device = [device for device in device_list if vol_handle in device]
    if not crypt_device:
        log.error(
            f"crypt device for volume handle {vol_handle} not present on node : {node}"
        )
        return False

    log.info(f"Crypt device for volume handle {vol_handle} present on the node: {node}")
    return True


def get_node_by_internal_ip(internal_ip):
    """
    Get the node object by the node internal ip.

    Args:
        internal_ip (str): The node internal ip to search for

    Returns:
        ocs_ci.ocs.resources.ocs.OCS: The node object with the given internal ip.
            If not found, it returns None.

    """
    node_objs = get_node_objs()
    for n in node_objs:
        if get_node_internal_ip(n) == internal_ip:
            return n

    return None
