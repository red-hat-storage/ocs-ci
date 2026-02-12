"""
This module deploys the openshift-logging on the cluster
lokistack stack
"""

import logging
import base64

from ocs_ci.ocs import constants, ocp
from ocs_ci.utility import templating
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers import helpers
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.node import get_all_nodes
from ocs_ci.framework import config as config

logger = logging.getLogger(__name__)


def create_namespace(yaml_file):
    """
    Creation of namespace "openshift-operators-redhat"
    for lokistack-operator and "openshift-logging"
    for ClusterLogging-operator

    Args:
        yaml_file (str): Path to yaml file to create namespace

    Example:
        create_namespace(yaml_file=constants.EO_NAMESPACE_YAML)

    """

    namespaces = ocp.OCP(kind=constants.NAMESPACES)

    logger.info("Creating Namespace.........")
    assert namespaces.create(yaml_file=yaml_file), "Failed to create namespace"
    logger.info("Successfully created Namespace")


def create_lokistack_operator_group(
    yaml_file, resource_name, skip_resource_exists=False
):
    """
    Creation of operator-group for lokitsack operator

    Args:
        yaml_file (str): Path to yaml file to create operator group for
            elastic-search
        resource_name (str): Name of the operator group to create for
            elastic-search

    Returns:
        bool: True if operator group for lokistack is created
            successfully, false otherwise

    Example::

        create_lokistack_operator_group(
            constants.EO_OG_YAML, 'openshift-operators-redhat'
        )

    """

    es_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP,
        namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
    )

    es_operator_group.create(yaml_file=yaml_file)
    try:
        es_operator_group.get(out_yaml_format=True)
        logger.info("The Operator group is created successfully")
    except CommandFailed:
        logger.error("The resource is not found")
        return False
    return True


def create_clusterlogging_operator_group(yaml_file, skip_resource_exists=False):
    """
    Creation of operator-group for clusterlogging
    operator.

    Args:
        yaml_file (str): Path to yaml file to create operator group for
            cluster-logging operator
        resource_name (str): Name of the operator group to create for
            cluster-logging operator

    Returns:
        bool: True if operator group for cluster-logging is created
            successfully, false otherwise

    Example:
        create_clusterlogging_operator_group(yaml_file=constants.CL_OG_YAML)

    """

    operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )

    operator_group.create(yaml_file=yaml_file)
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

    Returns:
        bool: True if subscription exists

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


def get_lokistack_subscription():
    """
    Creation of subscription for lokistack to subscribe
    a namespace to an operator

    Returns:
        bool: Subscription exists or not

    """
    lo_subscription = ocp.OCP(
        kind=constants.SUBSCRIPTION,
        namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
    )
    lo_sub_info = lo_subscription.get(out_yaml_format=True)
    lo_sub = lo_subscription.check_resource_existence(
        resource_name=constants.LOKISTACK_SUBSCRIPTION,
        should_exist=True,
        timeout=200,
    )
    if lo_sub:
        logger.info(lo_sub_info)
    else:
        logger.info("Creation of subscription for lokistack failed")
    return bool(lo_sub)


def get_obc():
    """
    Checking for successful creation of OBC for providing s3 object storage for lokistack.

    Returns:
        bool:  The return value. True for success  if obc created, False otherwise.

    """

    obc_obj = ocp.OCP(
        kind=constants.OBC, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )

    obc_info = obc_obj.get(out_yaml_format=True)
    obc = obc_obj.check_resource_existence(
        resource_name=constants.OBJECT_BUCKET_CLAIM,
        should_exist=True,
        timeout=200,
    )
    logger.info("OBC is created successfully")
    if obc:
        logger.info(obc_info)
    return bool(obc)


def get_secret_to_lokistack():
    """
    check if secret is created successfully that will contains endpoint and
    credential details for s3 bucket used by lokistack

    return:
        bool : True if secret created successfully

    """

    lo_secret = ocp.OCP(
        kind=constants.SECRET, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    lo_secret.get(out_yaml_format=True)
    ls_secret = lo_secret.check_resource_existence(
        resource_name=constants.LOKISTACK_SEC,
        timeout=120,
        should_exist=True,
    )
    return bool(ls_secret)


@retry(CommandFailed, tries=5, delay=60, backoff=2)
def create_lokistack(yaml_file, skip_resource_exists=False):
    """
    creates lokistack (logging store)
    for storing logs from nodes and application containers

    Args:
        yaml_file (str): path to where lokistack yaml exists
        skip_resource_exists: Skip the resource creation if it already exists

    """

    lokistack_obj = ocp.OCP(
        kind=constants.LOKISTACK, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    try:
        lokistack_obj.create(yaml_file=yaml_file)
    except CommandFailed as e:
        if "AlreadyExists" in str(e) and skip_resource_exists:
            logger.warning("obc already exists")
            return True
        else:
            raise

    # verification of installation of lokistack from loki pods running
    pod_obj = ocp.OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    pod_status = pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=9,
        timeout=800,
        sleep=20,
    )
    assert pod_status, "Pods are not in Running state."
    logger.info("All pods are in Running state")


def setup_sa_permissions():
    """
    Assign the necessary permissions to the service account for the
    collector to be able to collect and forward logs

    the collector is provided permissions to collect logs from
    infrastructure application and audit logs.
    """

    sa_name = "loki-reader"
    exec_cmd(f"oc create sa {sa_name} -n {constants.OPENSHIFT_LOGGING_NAMESPACE}")

    sample = TimeoutSampler(
        timeout=60,
        sleep=10,
        func=run_cmd_verify_cli_output,
        cmd=(
            f"oc adm policy add-cluster-role-to-user logging-collector-logs-writer"
            f" -z {sa_name} -n {constants.OPENSHIFT_LOGGING_NAMESPACE}"
        ),
        expected_output_lst='clusterrole.rbac.authorization.k8s.io/logging-collector-logs-writer added: "loki-reader" ',
    )
    if not sample.wait_for_func_status(result=True):
        raise Exception("Failed to add role to user")

    sample = TimeoutSampler(
        timeout=60,
        sleep=10,
        func=run_cmd_verify_cli_output,
        cmd=(
            f"oc adm policy add-cluster-role-to-user collect-application-logs"
            f" -z {sa_name} -n {constants.OPENSHIFT_LOGGING_NAMESPACE}"
        ),
        expected_output_lst='clusterrole.rbac.authorization.k8s.io/collect-application-logs added: "loki-reader" ',
    )
    if not sample.wait_for_func_status(result=True):
        raise Exception("Failed to add role to user")

    sample = TimeoutSampler(
        timeout=60,
        sleep=10,
        func=run_cmd_verify_cli_output,
        cmd=(
            f"oc adm policy add-cluster-role-to-user collect-infrastructure-logs"
            f" -z {sa_name} -n {constants.OPENSHIFT_LOGGING_NAMESPACE}"
        ),
        expected_output_lst='clusterrole.rbac.authorization.k8s.io/collect-infrastructure-logs added: "loki-reader" ',
    )
    if not sample.wait_for_func_status(result=True):
        raise Exception("Failed to add role to user")

    sample = TimeoutSampler(
        timeout=60,
        sleep=10,
        func=run_cmd_verify_cli_output,
        cmd=(
            f"oc adm policy add-cluster-role-to-user collect-audit-logs"
            f" -z {sa_name} -n {constants.OPENSHIFT_LOGGING_NAMESPACE}"
        ),
        expected_output_lst='clusterrole.rbac.authorization.k8s.io/collect-audit-logs added: "loki-reader" ',
    )
    if not sample.wait_for_func_status(result=True):
        raise Exception("Failed to add role to user")

    exec_cmd(
        '/bin/bash -c "TOKEN=$(oc create token loki-reader -n openshift-logging)" '
    )

    cmd = (
        "oc -n openshift-logging create secret generic secret-to-lokistack"
        " --from-literal=token=$TOKEN  --from-literal=ca-bundle.crt='$(oc -n openshift-logging"
        " get cm logging-loki-gateway-ca-bundle -o json | jq  "
        ".data."
        "service-ca.crt"
        " -r )' "
    )
    exec_cmd(cmd)


@retry(CommandFailed, tries=5, delay=60, backoff=2)
def create_clusterlogforwarder(yaml_file, skip_resource_exists=False):
    """
    Create a ClusterLogForwarder CR to collect logs from nodes and
    application containers and verifies  verify successfull installation of cfr by
    checking required no of pods and pvc running in project

    Args:
        yaml_file: path tp where yaml for clusterlogforwader exists
        skip_resource_exists: Skip the resource creation if it already exists

    """
    clf_obj = ocp.OCP(
        kind=constants.CLUSTER_LOG_FORWADER,
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE,
    )
    try:
        clf_obj.create(yaml_file=yaml_file)
    except CommandFailed as e:
        if "AlreadyExists" in str(e) and skip_resource_exists:
            logger.warning("clusterlogforwader already exists")
            return True
        else:
            raise

    pod_obj = ocp.OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    # verification of ClusterLogForwarder installation
    nodes_in_cluster = len(get_all_nodes())
    pod_status = pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=10 + nodes_in_cluster,
        timeout=800,
        sleep=20,
    )
    assert pod_status, "Required no of pods are not in Running state."
    logger.info("All pods are in Running state")

    pvc_obj = ocp.OCP(
        kind=constants.PVC, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    pvc_status = pvc_obj.wait_for_resource(
        condition=constants.STATUS_BOUND,
        resource_count=nodes_in_cluster,
        timeout=150,
        sleep=5,
    )
    assert pvc_status, "PVCs are not in bound state."
    logger.info("PVCs are Bound")


def create_clusterobservability_operator_group(
    yaml_file, resource_name, skip_resource_exists=False
):
    """
    Creation of operator-group for cluster obervability operator
    Args:
        yaml_file (str): Path to yaml file to create operator group for
        resource_name (str): Name of the operator group to create for
        skip_resource_exists: Skip the resource creation if it already exists

    Returns:
        bool: True if operator group  is created
            successfully, false otherwise
    """
    co_operator_group = ocp.OCP(
        kind=constants.OPERATOR_GROUP,
        namespace=constants.OPENSHIFT_CLUSTER_OBSERVABILITY_OPERATOR,
    )
    try:
        co_operator_group.create(yaml_file=yaml_file)
    except CommandFailed as e:
        if "AlreadyExists" in str(e) and skip_resource_exists:
            logger.warning("Operator group already exists")
            return True
        else:
            raise

    try:
        co_operator_group.get(resource_name, out_yaml_format=True)
        logger.info("The Operator group is created successfully")
    except CommandFailed:
        logger.error("The resource is not found")
        return False
    return True


def get_cluster_observability_subscription():
    """
    Fetches subscription for cluster obervability operator

    Returns:
        bool: True if subscription is created
            successfully, false otherwise

    """
    co_subscription = ocp.OCP(
        kind=constants.SUBSCRIPTION,
        namespace=constants.OPENSHIFT_CLUSTER_OBSERVABILITY_OPERATOR,
    )
    co_subscription_info = co_subscription.get(out_yaml_format=True)
    co_sub = co_subscription.check_resource_existence(
        resource_name=constants.CLUSTER_OBSERVABILITY_SUBSCRIPTION,
        timeout=120,
        should_exist=True,
    )
    if co_sub:
        logger.info(co_subscription_info)
    return bool(co_sub)


@retry(CommandFailed, tries=5, delay=60, backoff=2)
def create_UI_Plugin(yaml_file, resource_name):
    """
    Creates UI-Plugin for cluster obervability operator

    Args:
        yaml_file: path to yaml file to create UIPlugin
        resource_name: name of the UIPlugin

    Returns:
        bool: True if plugin is created
            successfully, false otherwise
    """

    uiplugin_obj = ocp.OCP(
        kind=constants.UIPLUGIN,
        namespace=constants.OPENSHIFT_CLUSTER_OBSERVABILITY_OPERATOR,
    )
    try:
        uiplugin_obj.create(yaml_file=yaml_file)
    except CommandFailed as e:
        if "AlreadyExists" in str(e):
            logger.warning("UIPlugin already exists")
            return True
        else:
            raise

    try:
        uiplugin_obj.get(resource_name, out_yaml_format=True)
        logger.info("The uiplugin is created successfully")
    except CommandFailed:
        logger.error("The resource is not found")
        return False
    return True


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
    package_manifest = PackageManifest(
        resource_name=constants.CLUSTERLOGGING_SUBSCRIPTION,
        selector="catalog=redhat-operators",
    )
    logging_channel = package_manifest.get_default_channel()

    # Creates namespace openshift-operators-redhat
    ocp_logging_obj.create_namespace(yaml_file=constants.EO_NAMESPACE_YAML)

    # Creates an operator-group for lokistack
    assert ocp_logging_obj.create_lokistack_operator_group(
        yaml_file=constants.EO_OG_YAML,
        resource_name=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE,
    )
    # Creates subscription for lokistack operator
    subscription_yaml = templating.load_yaml(constants.LOKI_OPERATOR_SUB_YAML)
    subscription_yaml["spec"]["channel"] = logging_channel
    helpers.create_resource(**subscription_yaml)
    assert ocp_logging_obj.get_lokistack_subscription()

    # Creates a namespace openshift-logging
    ocp_logging_obj.create_namespace(yaml_file=constants.CL_NAMESPACE_YAML)

    # Create RGW obc
    obc_yaml = templating.load_yaml(constants.LOKI_OPERATOR_OBC_YAML)

    if config.ENV_DATA["platform"].lower() in constants.ON_PREM_PLATFORMS:
        obc_yaml["spec"]["storageClassName"] = constants.DEFAULT_STORAGECLASS_RGW
    else:
        obc_yaml["spec"]["storageClassName"] = constants.NOOBAA_SC

    helpers.create_resource(**obc_yaml)

    ocp_logging_obj.get_obc()

    # Creating secret
    sample = TimeoutSampler(
        timeout=180,
        sleep=20,
        func=run_cmd_verify_cli_output,
        cmd=(
            f"oc -n {constants.OPENSHIFT_LOGGING_NAMESPACE} get configmap"
            f" {constants.OBJECT_BUCKET_CLAIM} -o jsonpath='{{.data.BUCKET_PORT}}'"
        ),
    )
    if not sample.wait_for_func_status(result=True):
        raise Exception("Failed to get configmap")

    configmap_obj = ocp.OCP(
        kind=constants.CONFIGMAP, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    cm_dict = configmap_obj.get(resource_name=constants.OBJECT_BUCKET_CLAIM)

    access_key_cmd = (
        f"oc get -n {constants.OPENSHIFT_LOGGING_NAMESPACE}"
        f" secret {constants.OBJECT_BUCKET_CLAIM} -o jsonpath='{{.data.AWS_ACCESS_KEY_ID}}'"
    )
    access_key = exec_cmd(access_key_cmd)
    decoded1 = base64.b64decode(access_key.stdout).decode("utf-8")

    secret_key_cmd = (
        f"oc get -n {constants.OPENSHIFT_LOGGING_NAMESPACE}"
        f" secret {constants.OBJECT_BUCKET_CLAIM} -o jsonpath='{{.data.AWS_SECRET_ACCESS_KEY}}'"
    )
    secret_key = exec_cmd(secret_key_cmd)
    decoded2 = base64.b64decode(secret_key.stdout).decode("utf-8")

    secret_yaml = templating.load_yaml(constants.LOKI_OPERATOR_SECRET_YAML)
    secret_yaml["stringData"]["access_key_id"] = decoded1
    secret_yaml["stringData"]["access_key_secret"] = decoded2
    secret_yaml["stringData"]["bucketnames"] = cm_dict["data"]["BUCKET_NAME"]
    endpoint = cm_dict["data"]["BUCKET_HOST"]
    secret_yaml["stringData"]["endoint"] = f"https://{endpoint}:80"
    helpers.create_resource(**secret_yaml)
    assert ocp_logging_obj.get_secret_to_lokistack()

    # creates lokistack
    ocp_logging_obj.create_lokistack(yaml_file=constants.LOKISTACK_YAML)
    logger.info("Loki operator is installed successfuly")

    # Creates an operator-group for cluster-logging
    ocp_logging_obj.create_clusterlogging_operator_group(yaml_file=constants.CL_OG_YAML)

    # Creates subscription for cluster-logging
    cl_subscription = templating.load_yaml(constants.CL_SUB_YAML)
    cl_subscription["spec"]["channel"] = logging_channel
    helpers.create_resource(**cl_subscription)
    assert ocp_logging_obj.get_clusterlogging_subscription()

    # creates a service account to be used by the log collector
    ocp_logging_obj.setup_sa_permissions()

    # Creates ClusterLogForwarder
    ocp_logging_obj.create_clusterlogforwarder(yaml_file=constants.CLF_YAML)
    logger.info("Openshift Logging operator is installed successfully")

    # Creates namespace for openshift-cluster-observability-operator
    ocp_logging_obj.create_namespace(yaml_file=constants.CO_NAMESPACE_YAML)

    # Creates OperatorGroup for openshift-cluster-observability-operator
    ocp_logging_obj.create_clusterobservability_operator_group(
        yaml_file=constants.CO_OG_YAML,
        resource_name=constants.CLUSTER_OBSERVABILITY_OPERATOR,
    )
    # Creates subscription for openshift-cluster-observability-operator
    co_subscription_yaml = templating.load_yaml(constants.CO_SUB_YAML)
    helpers.create_resource(**co_subscription_yaml)
    assert ocp_logging_obj.get_cluster_observability_subscription()

    # Creates UI Plugin for openshift-cluster-observability-operator
    ocp_logging_obj.create_UI_Plugin(
        yaml_file=constants.CO_UI_PLUGIN_YAML, resource_name="logging"
    )
