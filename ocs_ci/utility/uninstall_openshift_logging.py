"""
Function to teardown the openshift-logging
"""
import logging

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry
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


def uninstall_cluster_logging():
    """
    Function to uninstall cluster-logging from the cluster
    Deletes the project "openshift-logging" and "openshift-operators-redhat"
    """
    # Validating the pods before deleting the instance
    pod_list = get_all_pods(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)

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
        kind=constants.CLUSTER_LOGGING, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    assert clusterlogging_obj.delete(resource_name='instance')

    check_pod_vanished(pod_names_list)

    # Deleting the PVCs
    pvc_obj = ocp.OCP(
        kind=constants.PVC, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    pvc_list = get_all_pvcs(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)
    for pvc in range(len(pvc_list) - 1):
        pvc_obj.delete(resource_name=pvc_list['items'][pvc]['metadata']['name'])
        pvc_obj.wait_for_delete(resource_name=pvc_list['items'][pvc]['metadata']['name'])

    # Deleting the RBAC permission set
    rbac_role = ocp.OCP(
        kind=constants.ROLE, namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE
    )
    rbac_role.delete(yaml_file=constants.EO_RBAC_YAML)

    # Deleting the projects
    openshift_logging_namespace = ocp.OCP(
        kind=constants.NAMESPACES, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    openshift_operators_redhat_namespace = ocp.OCP(
        kind=constants.NAMESPACES, namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE
    )

    if openshift_logging_namespace.get():
        assert openshift_logging_namespace.delete(resource_name=constants.OPENSHIFT_LOGGING_NAMESPACE)
        logger.info("The namespace openshift-logging got deleted successfully")
    if openshift_operators_redhat_namespace.get():
        assert openshift_operators_redhat_namespace.delete(resource_name=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE)
        logger.info("The project openshift-opertors-redhat got deleted successfully")
