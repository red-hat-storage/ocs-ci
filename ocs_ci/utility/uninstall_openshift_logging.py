"""
Function to teardown the openshift-logging
"""
import logging

from ocs_ci.ocs import constants, ocp

logger = logging.getLogger(__name__)


def uninstall_cluster_logging():
    """
    Function to uninstall cluster-logging from the cluster
    1. Deletes the project "openshift-logging"
        & "openshift-operators-redhat"
    2. Deletes the CSC resource "cluster-logging-operator"
        & "elasticsearch"
    """

    # Deleting the clusterlogging
    clusterlogging_obj = ocp.OCP(
        kind='ClusterLogging', namespace='openshift-logging'
    )
    assert clusterlogging_obj.delete(resource_name='instance')

    # Validating all the pods are in Terminating state
    pod_obj = ocp.OCP(
        kind=constants.POD, namespace='openshift-logging'
    )
    ret = pod_obj.get()
    assert ret, "Pods are in Running state."
    logger.info("All pods are deleted")

    # validating all PVCs got deleted
    pv_obj = ocp.OCP(
        kind=constants.PVC, namespace='openshift-logging'
    )
    pvc_check = pv_obj.get()
    assert pvc_check, "PVCs are in Bound state"
    logger.info("All PVCs are successfully deleted")

    # Deleting the projects
    openshift_logging_namespace = ocp.OCP(
        kind=constants.NAMESPACES, namespace='openshift-logging'
    )
    openshift_operators_redhat_namespace = ocp.OCP(
        kind=constants.NAMESPACES, namespace='openshift-operators-redhat'
    )

    if openshift_logging_namespace.get():
        logger.info("The namespaces openshift-logging exists, Deleting.......")
        assert openshift_logging_namespace.delete(resource_name='openshift-logging')
        logger.info("The project openshift-logging got deleted successfully")
    else:
        logger.error("The project openshift-logging does not exists")

    if openshift_operators_redhat_namespace.get():
        logger.info("The namespaces openshift-operators-redhat exists,"
                    "Deleting....")
        assert openshift_operators_redhat_namespace.delete(resource_name='openshift-operators-redhat')
        logger.info("The project openshift-operators-redhat got"
                    "deleted successfully")
    else:
        logger.error("The project openshift-operators-redhat does not exists")

    # Deletes the catalog source config
    resources_to_delete = ['cluster-logging-operator', 'elasticsearch']
    project_obj = ocp.OCP(
        kind='csc', namespace='openshift-marketplace'
    )
    if project_obj.get():
        logger.info("The CSC resources exists")
        for resource in resources_to_delete:
            assert project_obj.delete(resource_name=resource)
            logger.info(f"The resource {resource} is deleted")

