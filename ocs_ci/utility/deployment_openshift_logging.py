"""
This module deploys the openshift-logging on the cluster
EFK stack
"""

import logging
import json

from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.node import get_all_nodes
from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_obj
from ocs_ci.utility import templating, version
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.utility.retry import retry
from ocs_ci.helpers import helpers
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


def create_namespace(yaml_file, skip_resource_exists=False):
    """
    Creation of namespace "openshift-operators-redhat"
    for Elasticsearch-operator and "openshift-logging"
    for ClusterLogging-operator

    Args:
        yaml_file (str): Path to yaml file to create namespace
        skip_resource_exists: Skip the namespace creation if it already exists

    Example:
        create_namespace(yaml_file=constants.EO_NAMESPACE_YAML)

    """

    namespaces = ocp.OCP(kind=constants.NAMESPACES)

    logger.info("Creating Namespace.........")
    try:
        assert namespaces.create(yaml_file=yaml_file), "Failed to create namespace"
    except CommandFailed as e:
        if "AlreadyExists" in str(e) and skip_resource_exists:
            # on Rosa HCP the ns created from the deployment
            logger.warning("Namespace already exists")
        else:
            raise
    logger.info("Successfully created Namespace")


def create_elasticsearch_operator_group(
    yaml_file, resource_name, skip_resource_exists=False
):
    """
    Creation of operator-group for Elastic-search operator

    Args:
        yaml_file (str): Path to yaml file to create operator group for
            elastic-search
        resource_name (str): Name of the operator group to create for
            elastic-search
        skip_resource_exists: Skip the resource creation if it already exists

    Returns:
        bool: True if operator group for elastic search is created
            successfully, false otherwise

    Example::

        create_elasticsearch_operator_group(
            constants.EO_OG_YAML, 'openshift-operators-redhat'
        )

    """

    es_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP,
        namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
    )

    try:
        es_operator_group.create(yaml_file=yaml_file)
    except CommandFailed as e:
        if "AlreadyExists" in str(e) and skip_resource_exists:
            logger.warning("Operator group already exists")
            return True
        else:
            raise
    try:
        es_operator_group.get(resource_name, out_yaml_format=True)
        logger.info("The Operator group is created successfully")
    except CommandFailed:
        logger.error("The resource is not found")
        return False
    return True


def set_rbac(yaml_file, resource_name, skip_resource_exists=False):
    """
    Setting Role Based Access Control to grant Prometheus
    permission to access the openshift-operators-redhat namespace

    Args:
        yaml_file (str): Path to yaml file to create RBAC
            (ROLE BASED ACCESS CONTROL)
        resource_name (str): Name of the resource for which we give RBAC
            permissions
        skip_resource_exists: Skip the resource creation if it already exists
    Returns:
        bool: True if RBAC is set successfully,
            false otherwise

    Example:
        set_rbac(constants.EO_RBAC_YAML, 'prometheus-k8s')

    """

    rbac_role = ocp.OCP(
        kind=constants.ROLE, namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE
    )
    rbac_rolebinding = ocp.OCP(
        kind=constants.ROLEBINDING,
        namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
    )

    try:
        rbac_role.create(yaml_file=yaml_file, out_yaml_format=False)
    except CommandFailed as e:
        if "AlreadyExists" in str(e) and skip_resource_exists:
            logger.warning("RBAC role already exists")
            return True
        else:
            raise
    try:
        rbac_role.get(resource_name, out_yaml_format=True)
        rbac_rolebinding.get(resource_name, out_yaml_format=True)
        logger.info("Setting RBAC is successful")
    except CommandFailed:
        logger.error("RBAC is not set")
        return False
    return True


def get_elasticsearch_subscription():
    """
    Creation of Subscription for the namespace
    to subscribe a Namespace to an Operator.

    Args:
        yaml_file (str): Path to yaml file to create subscription for
            a namespace
        resource_name (str): Name of the subscription

    Returns:
        bool: Subscription exists or not

    Example:
        create_elasticsearch_subscription(constants.EO_SUB_YAML)

    """

    es_subscription = ocp.OCP(
        kind=constants.SUBSCRIPTION,
        namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
    )
    es_sub_info = es_subscription.get(out_yaml_format=True)
    es_sub = es_subscription.check_resource_existence(
        resource_name=constants.ELASTICSEARCH_SUBSCRIPTION,
        should_exist=True,
        timeout=200,
    )
    if es_sub:
        logger.info(es_sub_info)
    return bool(es_sub)


def create_clusterlogging_operator_group(yaml_file, skip_resource_exists=False):
    """
    Creation of operator-group for clusterlogging
    operator.

    Args:
        yaml_file (str): Path to yaml file to create operator group for
            cluster-logging operator
        skip_resource_exists: Skip the resource creation if it already exists

    Returns:
        bool: True if operator group for cluster-logging is created
            successfully, false otherwise

    Example:
        create_clusterlogging_operator_group(yaml_file=constants.CL_OG_YAML)

    """

    operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    try:
        operator_group.create(yaml_file=yaml_file)
    except CommandFailed as e:
        if "AlreadyExists" in str(e) and skip_resource_exists:
            logger.warning("Operator group already exists")
            return True
        else:
            raise
    try:
        operator_group.get(out_yaml_format=True)
        logger.info("The Operator group is created successfully")
    except CommandFailed:
        logger.error("The resource is not found")
        return False
    return True


def get_clusterlogging_subscription():
    """
    Creation of subscription for clusterlogging to subscribe
    a namespace to an operator

    Args:
        yaml_file (str): Path to yaml file to create subscription for
            the namespace
        resource_name (str): Name of the subscription

    Returns:
        bool: Subscription exists or not

    Example:
        cl_create_subscription(yaml_file=constants.CL_SUB_YAML)

    """

    clusterlogging_subscription = ocp.OCP(
        kind=constants.SUBSCRIPTION, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    subscription_info = clusterlogging_subscription.get(out_yaml_format=True)
    logging_sub = clusterlogging_subscription.check_resource_existence(
        resource_name=constants.CLUSTERLOGGING_SUBSCRIPTION,
        timeout=120,
        should_exist=True,
    )
    if logging_sub:
        logger.info(subscription_info)
    return bool(logging_sub)


def create_instance_in_clusterlogging():
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

    nodes_in_cluster = len(get_all_nodes())
    inst_data = templating.load_yaml(constants.CL_INSTANCE_YAML)
    es_node_count = inst_data["spec"]["logStore"]["elasticsearch"]["nodeCount"]
    if helpers.storagecluster_independent_check():
        inst_data["spec"]["logStore"]["elasticsearch"]["storage"][
            "storageClassName"
        ] = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
    helpers.create_resource(wait=False, **inst_data)
    oc = ocp.OCP("v1", "ClusterLogging", "openshift-logging")
    logging_instance = oc.get(resource_name="instance", out_yaml_format="True")
    if logging_instance:
        logger.info("Successfully created instance for cluster-logging")
        logger.debug(logging_instance)
    else:
        logger.error("Instance for clusterlogging is not created properly")

    pod_obj = ocp.OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    pod_status = pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=2 + es_node_count + nodes_in_cluster,
        timeout=800,
        sleep=20,
    )
    assert pod_status, "Pods are not in Running state."
    logger.info("All pods are in Running state")
    pvc_obj = ocp.OCP(
        kind=constants.PVC, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    pvc_status = pvc_obj.wait_for_resource(
        condition=constants.STATUS_BOUND,
        resource_count=es_node_count,
        timeout=150,
        sleep=5,
    )
    assert pvc_status, "PVCs are not in bound state."
    logger.info("PVCs are Bound")
    return logging_instance


@retry((CommandFailed, UnexpectedBehaviour), tries=5, delay=60, backoff=2)
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
    pods = get_all_pods(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)
    logger.info("Pods that are created by the instance")
    for pod in pods:
        pod_list.append(pod.name)
    logger.info(pod_list)
    elasticsearch_pod = [pod for pod in pod_list if pod.startswith("elasticsearch")]
    pod_obj = get_pod_obj(
        name=elasticsearch_pod[0], namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    status_check = pod_obj.exec_cmd_on_pod(
        command="es_util --query=_cluster/health?pretty", out_yaml_format=False
    )
    logger.info(status_check)
    status_check = json.loads(status_check)
    if status_check["status"] == "green":
        logger.info("Cluster logging is in Healthy state & Ready to use")
    else:
        logger.error("Cluster logging is in Bad state")
        raise UnexpectedBehaviour
    return pod_list


@retry(CommandFailed, tries=5, delay=10, backoff=2)
def create_instance():
    """
    The function is used to create instance for
    cluster-logging
    """

    # Create instance
    assert create_instance_in_clusterlogging()

    # Check the health of the cluster-logging
    assert check_health_of_clusterlogging()

    csv_obj = CSV(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)

    # Get the CSV installed
    get_csv = csv_obj.get(out_yaml_format=False)
    logger.info(f"The installed CSV is {get_csv}")


def install_logging():

    csv = ocp.OCP(
        kind=constants.CLUSTER_SERVICE_VERSION,
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE,
    )
    logging_csv = csv.get().get("items")
    if logging_csv:
        logger.info("Logging is already configured, Skipping Installation")
        return

    logger.info("Configuring Openshift-logging")

    # Gets OCP version to align logging version to OCP version
    ocp_version = version.get_semantic_ocp_version_from_config()

    logging_channel = "stable" if ocp_version >= version.VERSION_4_7 else ocp_version

    # Creates namespace openshift-operators-redhat
    ocp_logging_obj.create_namespace(yaml_file=constants.EO_NAMESPACE_YAML)

    # Creates an operator-group for elasticsearch
    assert ocp_logging_obj.create_elasticsearch_operator_group(
        yaml_file=constants.EO_OG_YAML,
        resource_name=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
    )

    # Set RBAC policy on the project
    assert ocp_logging_obj.set_rbac(
        yaml_file=constants.EO_RBAC_YAML, resource_name="prometheus-k8s"
    )

    # Creates subscription for elastic-search operator
    subscription_yaml = templating.load_yaml(constants.EO_SUB_YAML)
    subscription_yaml["spec"]["channel"] = logging_channel
    helpers.create_resource(**subscription_yaml)
    assert ocp_logging_obj.get_elasticsearch_subscription()

    # Creates a namespace openshift-logging
    ocp_logging_obj.create_namespace(yaml_file=constants.CL_NAMESPACE_YAML)

    # Creates an operator-group for cluster-logging
    assert ocp_logging_obj.create_clusterlogging_operator_group(
        yaml_file=constants.CL_OG_YAML
    )

    # Creates subscription for cluster-logging
    cl_subscription = templating.load_yaml(constants.CL_SUB_YAML)
    cl_subscription["spec"]["channel"] = logging_channel
    helpers.create_resource(**cl_subscription)
    assert ocp_logging_obj.get_clusterlogging_subscription()

    # Creates instance in namespace openshift-logging
    cluster_logging_operator = OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    logger.info(f"The cluster-logging-operator {cluster_logging_operator.get()}")
    ocp_logging_obj.create_instance()
