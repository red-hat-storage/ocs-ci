"""
Function to teardown the openshift-logging
"""
import logging

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.pod import get_all_pods, verify_pod_exists

logger = logging.getLogger(__name__)


def uninstall_cluster_logging():
    """
    Function to uninstall cluster-logging from the cluster
    * Deletes the project "openshift-logging"
        & "openshift-operators-redhat"
    """
    # Validating the pods before deleting the instance
    pod_list = get_all_pods(namespace='openshift-logging')
    for pod in pod_list:
        logger.info(f"Pods running in the openshift-logging namespace {pod.name}")

    # Deleting the clusterlogging instance
    clusterlogging_obj = ocp.OCP(
        kind='ClusterLogging', namespace='openshift-logging'
    )
    assert clusterlogging_obj.delete(resource_name='instance')

    # Verifying the pods if exists after deleting instance
    pod_list = [
        pod for pod in pod_list if not pod.name.startswith('cluster-logging-operator')
    ]
    for pod in pod_list:
        ocp.OCP.wait_for_delete(resource_name=pod, timeout=10, sleep=3)
        verify_pod_exists(
            namespace='openshift-logging', pod_name=pod.name
        )

    # Deleting the projects
    openshift_logging_namespace = ocp.OCP(
        kind=constants.NAMESPACES, namespace='openshift-logging'
    )
    openshift_operators_redhat_namespace = ocp.OCP(
        kind=constants.NAMESPACES, namespace='openshift-operators-redhat'
    )

    if openshift_logging_namespace.get():
        assert openshift_logging_namespace.delete(resource_name='openshift-logging')
        logger.info(f"The namespace openshift-logging got deleted successfully")
    if openshift_operators_redhat_namespace.get():
        assert openshift_operators_redhat_namespace.delete(resource_name='openshift-operators-redhat')
        logger.info(f"The project openshift-opertors-redhat got deleted successfully")
    else:
        logger.error("The project does not exists")
