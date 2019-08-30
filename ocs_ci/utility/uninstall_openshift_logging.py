"""
Function to teardown the openshift-logging
"""
import logging

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry
logger = logging.getLogger(__name__)


@retry(UnexpectedBehaviour, 5, 30, 2)
def check_pod_vanished(pod_names):
    """
    A function to check all the pods are vanished from the namespace
    """
    pod_list_current = get_all_pods(namespace='openshift-logging')
    pod_names_current = [pod.name for pod in pod_list_current]
    for pod in pod_names:
        if pod in pod_names_current:
            raise UnexpectedBehaviour


def uninstall_cluster_logging():
    """
    Function to uninstall cluster-logging from the cluster
    * Deletes the project "openshift-logging"
        & "openshift-operators-redhat"
    """
    # Validating the pods before deleting the instance
    pod_list = get_all_pods(namespace='openshift-logging')

    for pod in pod_list:
        logger.info(
            f"Pods running in the openshift-logging namespace {pod.name}"
        )

    # Excluding cluster-logging-operator from pod_list and getting pod names
    pod_names_list = [
        pod.name for pod in pod_list if not pod.name.startswith(
            'cluster-logging-operator'
        )
    ]

    # Deleting the clusterlogging instance
    clusterlogging_obj = ocp.OCP(
        kind=constants.CLUSTER_LOGGING, namespace='openshift-logging'
    )
    assert clusterlogging_obj.delete(resource_name='instance')

    check_pod_vanished(pod_names_list)
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
