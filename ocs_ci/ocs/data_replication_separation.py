"""
Data replication separation module

"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import UnavailableResourceException
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod, get_ceph_tools_pod
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.utility.networking import get_node_private_ip

log = logging.getLogger(__name__)


def validate_pods_have_host_network(pods):
    """
    Validate that pods have set host network.

    Args:
        pods (list): List of Pod() objects

    Returns:
        bool: True if all of the pods have configured host network
    """
    result = True
    for pod in pods:
        log.info(f"checking that pod {pod.name} has set host network")
        if pod.pod_data.get("spec").get(
            "dnsPolicy"
        ) != constants.DNSPOLICY_CLUSTERFIRSTWITHHOSTNET or not pod.pod_data.get(
            "spec"
        ).get(
            "hostNetwork"
        ):
            log.error(f"pod {pod.name} doesn't have set Host network: {pod.pod_data}")
            result = False
    return result


@config.run_with_provider_context_if_available
def validate_monitor_pods_have_host_network():
    """
    Validate that monitor pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = [
        Pod(**pod_info)
        for pod_info in get_pods_having_label(
            label=constants.MON_APP_LABEL, statuses=["Running"]
        )
    ]
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_osd_pods_have_host_network():
    """
    Validate that osd pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = [
        Pod(**pod_info)
        for pod_info in get_pods_having_label(
            label=constants.OSD_APP_LABEL, statuses=["Running"]
        )
    ]
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_rgw_pods_have_host_network():
    """
    Validate that rgw pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = [
        Pod(**pod_info)
        for pod_info in get_pods_having_label(
            label=constants.RGW_APP_LABEL, statuses=["Running"]
        )
    ]
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_mgr_and_mdr_pods_have_host_network():
    """
    Validate that MGR and MDR pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = [
        Pod(**pod_info)
        for pod_info in get_pods_having_label(
            label=constants.MGR_APP_LABEL, statuses=["Running"]
        )
    ]
    pods.extend(
        [
            Pod(**pod_info)
            for pod_info in get_pods_having_label(
                label=constants.MDS_APP_LABEL, statuses=["Running"]
            )
        ]
    )
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_ceph_tool_pods_have_host_network():
    """
    Validate that ceph tool pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = [
        Pod(**pod_info)
        for pod_info in get_pods_having_label(
            label=constants.TOOL_APP_LABEL, statuses=["Running"]
        )
    ]
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_ceph_exporter_pods_have_host_network():
    """
    Validate that rook ceph exporter tool pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = [
        Pod(**pod_info)
        for pod_info in get_pods_having_label(
            label=constants.EXPORTER_APP_LABEL, statuses=["Running"]
        )
    ]
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_ceph_operator_pods_have_host_network():
    """
    Validate that odf operator pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = [
        Pod(**pod_info)
        for pod_info in get_pods_having_label(
            label=constants.OPERATOR_LABEL, statuses=["Running"]
        )
    ]
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_metrics_exporter_pods_have_host_network():
    """
    Validate that ocs metrics exporter pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = [
        Pod(**pod_info)
        for pod_info in get_pods_having_label(
            label=constants.OCS_METRICS_EXPORTER, statuses=["Running"]
        )
    ]
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_csi_pods_have_host_network():
    """
    Validate that csi pods have set host network.

    Returns:
        bool: True if all of the pods have configured host network
    """
    pods = []
    for label in (
        constants.CEPHFS_NODEPLUGIN_LABEL,
        constants.RBD_NODEPLUGIN_LABEL,
        constants.CEPHFS_CTRLPLUGIN_LABEL,
        constants.RBD_CTRLPLUGIN_LABEL,
    ):
        pods.extend(
            [
                Pod(**pod_info)
                for pod_info in get_pods_having_label(label=label, statuses=["Running"])
            ]
        )
    return validate_pods_have_host_network(pods)


@config.run_with_provider_context_if_available
def validate_mon_ip_annotation_on_workers():
    """
    Validate that worker nodes have correctly set network.rook.io/mon-ip annotation.
    It should be the private IP of the node.

    This function retrieves the actual private IP from each worker node using
    the 'ip addr' command and compares it with the annotation value.

    Returns:
        bool: True if all nodes have correct annotation
    """
    nodes_obj = OCP(kind="node")
    nodes = nodes_obj.get().get("items", [])
    worker_nodes = [
        node for node in nodes if constants.WORKER_LABEL in node["metadata"]["labels"]
    ]
    if not worker_nodes:
        raise UnavailableResourceException("No worker node found!")

    correct_annotations = True
    for worker in worker_nodes:
        worker_name = worker["metadata"]["name"]
        log.info(f"Checking node {worker_name} for annotation network.rook.io/mon-ip")

        # Get the actual private IP from the node using ip addr command
        try:
            _, expected_private_ip, _ = get_node_private_ip(worker_name)
        except Exception as e:
            log.error(f"Failed to retrieve private IP from node {worker_name}: {e}")
            correct_annotations = False
            continue

        annotation_ip = (
            worker.get("metadata").get("annotations", {}).get("network.rook.io/mon-ip")
        )

        if annotation_ip != expected_private_ip:
            log.error(
                f"Node {worker_name} has annotation network.rook.io/mon-ip={annotation_ip} "
                f"instead of network.rook.io/mon-ip={expected_private_ip}"
            )
            correct_annotations = False
        else:
            log.info(
                f"Node {worker_name} has correct annotation network.rook.io/mon-ip={annotation_ip}"
            )

    return correct_annotations


@config.run_with_provider_context_if_available
def validate_osds_use_cluster_network():
    """
    Validate that OSD processes are listening on the cluster network.

    This function executes netstat on each worker node to check if OSD processes
    are using the second network (cluster network) for replication traffic.

    Returns:
        bool: True if all OSDs are using the cluster network
    """
    nodes_obj = OCP(kind="node")
    worker_nodes = get_worker_nodes(skip_master_nodes=False)
    if not worker_nodes:
        raise UnavailableResourceException("No worker node found!")

    all_osds_using_network = True
    for worker in worker_nodes:
        log.info(f"Checking OSD network usage on node {worker}")

        cmd = "chroot /host netstat -tlnp | grep osd"
        try:
            output = nodes_obj.exec_oc_debug_cmd(node=worker, cmd_list=[cmd])
            if output and output.strip():
                log.info(f"Node {worker} has OSDs using network:\n{output}")
            else:
                log.warning(f"Node {worker} has no OSD network connections found")
        except Exception as e:
            log.error(f"Failed to check OSD network on node {worker}: {e}")
            all_osds_using_network = False

    return all_osds_using_network


@config.run_with_provider_context_if_available
def validate_ceph_cluster_network_configured():
    """
    Validate that Ceph cluster network is configured.

    This function executes 'ceph config dump' in the toolbox pod to check
    if cluster_network is configured in the Ceph configuration.

    Returns:
        bool: True if cluster_network is configured
    """
    toolbox_pod = get_ceph_tools_pod()
    log.info("Checking if cluster_network is configured in Ceph")

    try:
        output = toolbox_pod.exec_ceph_cmd("ceph config dump")
        log.debug(f"Ceph config dump output:\n{output}")

        if "cluster_network" in output:
            log.info("cluster_network is configured in Ceph")
            for line in output.splitlines():
                if "cluster_network" in line:
                    log.info(f"Found: {line.strip()}")
            return True
        else:
            log.error("cluster_network is NOT configured in Ceph config")
            return False
    except Exception as e:
        log.error(f"Failed to check Ceph cluster network configuration: {e}")
        return False
