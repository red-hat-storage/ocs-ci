"""
Methods used in awscli_pod fixtures in tests/conftest.py
"""

import logging

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import (
    create_resource,
    create_unique_resource_name,
    storagecluster_independent_check,
    wait_for_resource_state,
)
from ocs_ci.helpers.proxy import update_container_with_proxy_env
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import update_container_with_mirrored_image

log = logging.getLogger(__name__)


def create_awscli_pod(scope_name=None, namespace=None, service_account=None):
    """
    Create AWS cli pod and its resources.

    Args:
        scope_name (str): The name of the fixture's scope
        namespace (str): Namespace for aws cli pod

    Returns:
        object: awscli_pod_obj
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    # Create the service-ca configmap to be mounted upon pod creation
    service_ca_data = templating.load_yaml(constants.AWSCLI_SERVICE_CA_YAML)
    resource_type = scope_name or "caconfigmap"
    service_ca_configmap_name = create_unique_resource_name(
        constants.AWSCLI_SERVICE_CA_CONFIGMAP_NAME, resource_type
    )
    service_ca_data["metadata"]["name"] = service_ca_configmap_name
    service_ca_data["metadata"]["namespace"] = namespace
    s3cli_label_k, s3cli_label_v = constants.S3CLI_APP_LABEL.split("=")
    service_ca_data["metadata"]["labels"] = {s3cli_label_k: s3cli_label_v}
    log.info("Trying to create the AWS CLI service CA")

    service_ca_configmap = create_resource(**service_ca_data)
    OCP(namespace=namespace, kind="ConfigMap").wait_for_resource(
        resource_name=service_ca_configmap.name, column="DATA", condition="1"
    )

    log.info("Creating the AWS CLI StatefulSet")
    awscli_sts_dict = templating.load_yaml(constants.S3CLI_MULTIARCH_STS_YAML)
    awscli_sts_dict["spec"]["template"]["spec"]["volumes"][0]["configMap"][
        "name"
    ] = service_ca_configmap_name
    awscli_sts_dict["metadata"]["namespace"] = namespace
    update_container_with_mirrored_image(awscli_sts_dict)
    update_container_with_proxy_env(awscli_sts_dict)
    s3cli_sts_obj = create_resource(**awscli_sts_dict)

    log.info("Verifying the AWS CLI StatefulSet is running")
    assert s3cli_sts_obj, "Failed to create S3CLI STS"
    if service_account:
        s3cli_sts_obj.ocp.exec_oc_cmd(
            f"set serviceaccount statefulset {s3cli_sts_obj.name} {service_account}"
        )
    awscli_pod_obj = retry(IndexError, tries=3, delay=15)(
        lambda: Pod(**get_pods_having_label(constants.S3CLI_LABEL, namespace)[0])
    )()
    wait_for_resource_state(awscli_pod_obj, constants.STATUS_RUNNING, timeout=180)

    awscli_pod_obj.exec_cmd_on_pod(
        f"cp {constants.SERVICE_CA_CRT_AWSCLI_PATH} {constants.AWSCLI_CA_BUNDLE_PATH}"
    )

    if storagecluster_independent_check() and config.EXTERNAL_MODE.get("rgw_secure"):
        log.info("Concatenating the RGW CA to the AWS CLI pod's CA bundle")
        awscli_pod_obj.exec_cmd_on_pod(
            f"bash -c 'wget -O - {config.EXTERNAL_MODE['rgw_cert_ca']} >> {constants.AWSCLI_CA_BUNDLE_PATH}'"
        )

    return awscli_pod_obj


def awscli_pod_cleanup(namespace=None):
    """
    Delete AWS cli pod resources.

    Args:
        namespace (str): Namespace for aws cli pod
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    log.info("Deleting the AWS CLI StatefulSet")
    ocp_sts = OCP(
        kind="StatefulSet",
        namespace=namespace,
    )
    try:
        ocp_sts.delete(resource_name=constants.S3CLI_STS_NAME)
    except CommandFailed as e:
        if "NotFound" in str(e):
            log.info("The AWS CLI STS was not found, assuming it was already deleted")
    except TimeoutError:
        log.warning("Standard deletion of the AWS CLI STS timed-out, forcing deletion")
        ocp_sts.delete(resource_name=constants.S3CLI_STS_NAME, force=True)

    log.info("Deleting the AWS CLI service CA")
    ocp_cm = OCP(
        kind="ConfigMap",
        namespace=namespace,
    )
    awscli_service_ca_query = ocp_cm.get(selector=constants.S3CLI_APP_LABEL).get(
        "items"
    )
    if awscli_service_ca_query:
        ocp_cm.delete(resource_name=awscli_service_ca_query[0]["metadata"]["name"])
