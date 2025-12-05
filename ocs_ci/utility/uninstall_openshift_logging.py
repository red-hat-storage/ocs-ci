"""
Function to teardown the openshift-logging
"""

import logging

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs, delete_pvcs
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.exceptions import UnexpectedBehaviour, CommandFailed
from ocs_ci.utility.retry import retry
from ocs_ci.helpers.helpers import (
    fetch_used_size,
    default_ceph_block_pool,
    verify_volume_deleted_in_backend,
)

logger = logging.getLogger(__name__)


@retry(UnexpectedBehaviour, 5, 30, 2)
def check_pod_vanished(pod_names):
    """
    A function to check all the pods are vanished from the namespace
    """
    pod_list_current = get_all_pods(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)
    pod_names_current = [pod.name for pod in pod_list_current]
    for pod in pod_names:
        if pod in pod_names_current:
            raise UnexpectedBehaviour


def delete_logging_namespaces(force=False):
    """
    Deleting namespaces
    1. UIPlugin
    1. Openshift-cluster-observability-operator
    2. Openshift-logging
    3. Openshift-operators-redhat

    """
    uiplugin_obj = ocp.OCP(
        kind=constants.UIPLUGIN,
        namespace=constants.OPENSHIFT_CLUSTER_OBSERVABILITY_OPERATOR,
    )
    openshift_cluster_observability_namespace = ocp.OCP(
        kind=constants.NAMESPACES,
        resource_name=constants.OPENSHIFT_CLUSTER_OBSERVABILITY_OPERATOR,
    )
    openshift_logging_namespace = ocp.OCP(
        kind=constants.NAMESPACES, resource_name=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    openshift_operators_redhat_namespace = ocp.OCP(
        kind=constants.NAMESPACES,
        resource_name=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
    )
    try:
        uiplugin_obj.delete(
            resource_name="logging",
            force=force,
            wait=True,
        )
        logger.info("UI Plugin deleted successfully")
    except CommandFailed as e:
        logger.info("UI Plugin not found" f"Error message {e}")

    try:
        openshift_cluster_observability_namespace.delete(
            resource_name=constants.OPENSHIFT_CLUSTER_OBSERVABILITY_OPERATOR,
            force=force,
            wait=True,
        )
        logger.info(
            "The project openshift_cluster_observability_namespace got deleted successfully"
        )
    except CommandFailed as e:
        logger.info("Namespace not found" f"Error message {e}")

    try:
        openshift_logging_namespace.delete(
            resource_name=constants.OPENSHIFT_LOGGING_NAMESPACE,
            force=force,
            wait=True,
        )
        logger.info("The namespace openshift-logging got deleted successfully")
    except CommandFailed as e:
        logger.info("Namespace not found" f"Error message {e}")

    try:
        openshift_operators_redhat_namespace.delete(
            resource_name=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
            force=force,
            wait=True,
        )
        logger.info("The namespace openshift-operators-redhat got deleted successfully")
    except CommandFailed as e:
        logger.info("Namespace not found" f"Error message {e}")


def uninstall_cluster_logging():
    """
    Function to uninstall cluster-logging from the cluster
    Deletes UI Plugin and projects "openshift-logging",
    "openshift_cluster_observability_namespace" and "openshift-operators-redhat"
    """

    # Validating the pods before deleting the instance
    pod_list = get_all_pods(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)

    for pod in pod_list:
        logger.info(f"Pods running in the openshift-logging namespace {pod.name}")

    # Excluding cluster-logging-operator from pod_list and getting pod names
    pod_names_list1 = [
        pod.name
        for pod in pod_list
        if not pod.name.startswith(("cluster-logging-operator", "instance"))
    ]
    pod_names_list2 = [
        pod.name
        for pod in pod_list
        if not pod.name.startswith("cluster-logging-operator")
    ]
    pvc_objs = get_all_pvc_objs(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)

    # Fetch image uuid associated with PVCs to be deleted
    pvc_uuid_map = {}
    for pvc_obj in pvc_objs:
        pvc_uuid_map[pvc_obj.name] = pvc_obj.image_uuid

    # Checking for used space
    cbp_name = default_ceph_block_pool()
    used_space_before_deletion = fetch_used_size(cbp_name)
    logger.info(
        f"Used space before deletion of cluster logging {used_space_before_deletion}"
    )

    # Deleting the lokistack and cluster logging pods in openshift-storage namespace
    lokistack_obj = ocp.OCP(
        kind=constants.LOKISTACK, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    try:
        lokistack_obj.delete(resource_name="logging-loki", wait=True)
        logger.info("lokistack got deleted successfully")
        check_pod_vanished(pod_names_list1)

    except CommandFailed as error:
        delete_logging_namespaces(force=True)
        raise error

    clf_obj = ocp.OCP(
        kind=constants.CLUSTER_LOG_FORWADER,
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE,
    )
    try:
        clf_obj.delete(resource_name="instance", wait=True)
        logger.info("clusterlogforwader got deleted successfully")
        check_pod_vanished(pod_names_list2)

    except CommandFailed as error:
        delete_logging_namespaces(force=True)
        raise error

    for pvc_obj in pvc_objs:
        pv_obj = pvc_obj.backed_pv_obj
    assert delete_pvcs(pvc_objs=pvc_objs), "PVCs deletion failed"
    for pvc_obj in pvc_objs:
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name, timeout=300)
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=300)
    logger.info("Verified: PVCs are deleted.")
    logger.info("Verified: PV are deleted")
    for pvc_name, uuid in pvc_uuid_map.items():
        rbd = verify_volume_deleted_in_backend(
            interface=constants.CEPHBLOCKPOOL, image_uuid=uuid, pool_name=cbp_name
        )
        assert rbd, f"Volume associated with PVC {pvc_name} still exists " f"in backend"

    # Checking for used space after PVC deletion
    used_space_after_deletion = fetch_used_size(cbp_name)
    logger.info(
        f"Used space after deletion of cluster logging {used_space_after_deletion}"
    )
    if used_space_after_deletion < used_space_before_deletion:
        logger.info("Expected !!! Space has reclaimed")
    else:
        logger.warning("Unexpected !! No space reclaimed after deletion of PVC")

    delete_logging_namespaces()
