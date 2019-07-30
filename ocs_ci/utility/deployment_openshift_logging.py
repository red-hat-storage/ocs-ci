"""
This module deploys the openshift-logging on the cluster
EFK stack
"""

import logging
import json

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_obj
from ocs_ci.utility import templating
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.retry import retry
from tests import helpers

logger = logging.getLogger(__name__)


def create_namespace(yaml_file):
    """
    Creation of namespace "openshift-operators-redhat"
    for Elasticsearch-operator and "openshift-logging"
    for ClusterLogging-operator

    Args:
        yaml_file (str): Path to yaml file to create namespace

    Example:
        create_namespace(yaml_file=constants.EO_NAMESPACE_YAML)

    """

    namespaces = ocp.OCP(kind=constants.NAMESPACES)

    logger.info("Creating Namespace.........")
    assert namespaces.create(yaml_file=yaml_file), 'Failed to create namespace'
    logger.info("Successfully created Namespace")


def create_elasticsearch_operator_group(yaml_file, resource_name):
    """
    Creation of operator-group for Elastic-search operator

    Args:
        yaml_file (str): Path to yaml file to create operator group for
            elastic-search
        resource_name (str): Name of the operator group to create for
            elastic-search

    Returns:
        Bool: True if operator group for elastic search is created
            successfully, false otherwise

    Example:
        create_elasticsearch_operator_group(
            constants.EO_OG_YAML, 'openshift-operators-redhat'
        )

    """

    es_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace='openshift-operators-redhat'
    )

    es_operator_group.create(yaml_file=yaml_file)
    try:
        es_operator_group.get(resource_name, out_yaml_format=True)
        logger.info('The Operator group is created successfully')
    except CommandFailed:
        logger.error('The resource is not found')
        return False
    return True


def create_elasticsearch_catalogsourceconfig(yaml_file, resource_name):
    """
    Creation of catalogsourceconfig(CSC) to enable the
    Elasticsearch Operator on the cluster.

    Args:
        yaml_file (str): Path to yaml file to create catalogsourceconfig(CSC)
            for elastic-search
        resource_name (str): Name of the catalogsourceconfig(CSC) created for
            elastic-search

    Returns:
        Bool: True if catalogsourceconfig(CSC) for elastic search is
            created successfully, false otherwise

    Example:
        create_elasticsearch_catalogsourceconfig(
            yaml_file=constants.EO_CSC_YAML, resource_name='elasticsearch'
        )

    """

    create_csc = ocp.OCP(
        kind=constants.CATALOG_SOURCE_CONFIG, namespace='openshift-marketplace'
    )
    es_csc = ocp.OCP(
        kind=constants.CATALOG_SOURCE_CONFIG,
        namespace='openshift-operators-redhat'
    )

    create_csc.create(yaml_file=yaml_file)
    try:
        es_csc.get(resource_name, out_yaml_format=True)
        logger.info("The CatalogSourceConfig is created Successfully")
    except CommandFailed:
        logger.error("The resource is not found")
        return False
    return True


def set_rbac(yaml_file, resource_name):
    """
    Setting Role Based Access Control to grant Prometheus
    permission to access the openshift-operators-redhat namespace

    Args:
        yaml_file (str): Path to yaml file to create RBAC
            (ROLE BASED ACCESS CONTROL)
        resource_name (str): Name of the resource for which we give RBAC
            permissions

    Returns:
        Bool: True if RBAC is set successfully,
            false otherwise

    Example:
        set_rbac(constants.EO_RBAC_YAML, 'prometheus-k8s')

    """

    rbac_role = ocp.OCP(
        kind=constants.ROLE, namespace='openshift-operators-redhat'
    )
    rbac_rolebinding = ocp.OCP(
        kind=constants.ROLEBINDING, namespace='openshift-operators-redhat'
    )

    rbac_role.create(yaml_file=yaml_file)
    try:
        rbac_role.get(resource_name, out_yaml_format=True)
        rbac_rolebinding.get(resource_name, out_yaml_format=True)
        logger.info("Setting RBAC is successful")
    except CommandFailed:
        logger.error("RBAC is not set")
        return False
    return True


def create_elasticsearch_subscription(yaml_file):
    """
    Creation of Subscription for the namespace
    to subscribe a Namespace to an Operator.

    Args:
        yaml_file (str): Path to yaml file to create subscription for
            a namespace
        resource_name (str): Name of the subscription

    Returns:
        dict: Contains all the details of the subscription

    Example:
        create_elasticsearch_subscription(constants.EO_SUB_YAML)

    """

    es_subscription = ocp.OCP(
        kind=constants.SUBSCRIPTION, namespace='openshift-operators-redhat'
    )

    subscription = es_subscription.create(yaml_file, out_yaml_format=True)
    logger.info(subscription)
    subscription_info = es_subscription.get(out_yaml_format=True)
    if subscription_info:
        logger.info("The Subscription is created successfully")
    else:
        logger.error("The subscription is not installed properly")
    return subscription_info


def create_clusterlogging_operator_group(yaml_file):
    """
    Creation of operator-group for clusterlogging
    operator.

    Args:
        yaml_file (str): Path to yaml file to create operator group for
            cluster-logging operator
        resource_name (str): Name of the operator group to create for
            cluster-logging operator

    Returns:
        Bool: True if operator group for cluster-logging is created
            successfully, false otherwise

    Example:
        create_clusterlogging_operator_group(yaml_file=constants.CL_OG_YAML)

    """

    operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace='openshift-logging'
    )

    operator_group.create(yaml_file=yaml_file)
    try:
        operator_group.get(out_yaml_format=True)
        logger.info('The Operator group is created successfully')
    except CommandFailed:
        logger.error('The resource is not found')
        return False
    return True


def create_clusterlogging_catalogsourceconfig(yaml_file, resource_name):
    """
    Creation of catalogsourceconfig to enable the
    cluster-logging Operator on the cluster.

    Args:
        yaml_file (str): Path to yaml file to create catalog source config for
            cluster-logging
        resource_name (str) : Name of the catalog source config created
            for cluster-logging

    Returns:
        Bool: True if catalog source config for cluster-logging is created
            successfully, false otherwise

    Example:
        cl_create_catalogsourceconfig(
            constants.CL_CSC_YAML, 'cluster-logging-operator'
        )

    """

    create_csc = ocp.OCP(
        kind=constants.CATALOG_SOURCE_CONFIG, namespace='openshift-marketplace'
    )
    clusterlogging_csc = ocp.OCP(
        kind=constants.CATALOG_SOURCE_CONFIG, namespace='openshift-logging'
    )

    create_csc.create(yaml_file=yaml_file)
    try:
        clusterlogging_csc.get(resource_name, out_yaml_format=True)
        logger.info("The CatalogSourceConfig is created Successfully")
    except CommandFailed:
        logger.error("The resource is not found")
        return False
    return True


def create_clusterlogging_subscription(yaml_file):
    """
    Creation of subscription for clusterlogging to subscribe
    a namespace to an operator

    Args:
        yaml_file (str): Path to yaml file to create subscription for
            the namespace
        resource_name (str): Name of the subscription

    Returns:
        dict: Contains all the details of the subscription.

    Example:
        cl_create_subscription(yaml_file=constants.CL_SUB_YAML)

    """

    clusterlogging_subscription = ocp.OCP(
        kind=constants.SUBSCRIPTION, namespace='openshift-logging'
    )
    subscription = clusterlogging_subscription.create(
        yaml_file, out_yaml_format=True
    )
    logger.info(subscription)
    subscription_info = clusterlogging_subscription.get(
        resource_name='cluster-logging', out_yaml_format=True
    )
    if subscription_info:
        logger.info("The Subscription is created successfully")
    else:
        logger.error("The subscription is not installed properly")
    return subscription_info


def create_instance_in_clusterlogging(sc_name=None):
    """
    Creation of instance for clusterlogging that creates PVCs,
    ElasticSearch, curator fluentd and kibana pods and checks for all
    the pods and PVCs

    Args:
        sc_name (str): Storage class name to create PVCs

    Returns:
        dict: Contains all detailed information of the
            instance such as pods that got created, its resources and limits
            values, storage class and size details etc.

    """
    inst_data = templating.load_yaml_to_dict(constants.CL_INSTANCE_YAML)
    inst_data['spec']['logStore']['elasticsearch']['storage']['storageClassName'] = sc_name
    inst_data['spec']['logStore']['elasticsearch']['storage']['size'] = "200Gi"
    helpers.create_resource(wait=False, **inst_data)
    oc = ocp.OCP('v1', 'ClusterLogging', 'openshift-logging')
    logging_instance = oc.get(resource_name='instance', out_yaml_format='True')
    if logging_instance:
        logger.info("Successfully created instance for cluster-logging")
        logger.debug(logging_instance)
    else:
        logger.error("Instance for clusterlogging is not created properly")

    pv_obj = ocp.OCP(
        kind=constants.POD, namespace='openshift-logging'
    )
    ret = pv_obj.wait_for_resource(condition=constants.STATUS_RUNNING,
                                   resource_count=11,
                                   timeout=150, sleep=5
                                   )
    assert ret, "Pods are not in Running state."
    logger.info("All pods are in Running state")
    pv_obj = ocp.OCP(
        kind=constants.PVC, namespace='openshift-logging'
    )
    ret = pv_obj.wait_for_resource(condition=constants.STATUS_BOUND,
                                   resource_count=3, timeout=150, sleep=5
                                   )
    assert ret, "PVCs are not in bound state."
    logger.info("PVCs are Bound")
    return logging_instance


@retry(CommandFailed, 5, 30, 2)
def check_health_of_clusterlogging():
    """
    * Checks for ElasticSearch, curator, fluentd and kibana pods in
    openshift-logging namespace

    * And check for the health of cluster logging, If status is green then the
    cluster is healthy,if status is red then health is bad

    Returns:
        list: Gives all the pods that are present in the namespace

    """

    pod_list = []
    pods = get_all_pods(namespace='openshift-logging')
    logger.info("Pods that are created by the instance")
    for pod in pods:
        pod_list.append(pod.name)
    logger.info(pod_list)
    elasticsearch_pod = [
        pod for pod in pod_list if pod.startswith('elasticsearch')
    ]
    logger.info(elasticsearch_pod)
    pod_obj = get_pod_obj(
        name=elasticsearch_pod[0], namespace='openshift-logging'
    )
    status_check = pod_obj.exec_cmd_on_pod(
        command='es_util --query=_cluster/health?pretty',
        out_yaml_format=False
    )
    logger.info(status_check)
    status_check = json.loads(status_check)
    if status_check['status'] == 'green':
        logger.info("Cluster logging is in Healthy state & Ready to use")
    else:
        logger.error("Cluster logging is in Bad state")
    return pod_list
