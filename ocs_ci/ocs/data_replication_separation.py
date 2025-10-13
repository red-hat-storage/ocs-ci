"""
Data replication separation module

"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod

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
