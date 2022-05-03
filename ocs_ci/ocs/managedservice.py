import base64
import json
import logging
import os
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_pods_having_label, Pod
from ocs_ci.utility.utils import exec_cmd


logger = logging.getLogger(__name__)


def get_pagerduty_secret_name():
    """
    Get name of the PagerDuty secret for currently used addon.

    Returns:
        string: name of the secret
    """
    return config.ENV_DATA["addon_name"] + constants.MANAGED_PAGERDUTY_SECRET_SUFFIX


def get_smtp_secret_name():
    """
    Get name of the SMTP secret for currently used addon.

    Returns:
        string: name of the secret
    """
    return config.ENV_DATA["addon_name"] + constants.MANAGED_SMTP_SECRET_SUFFIX


def get_dms_secret_name():
    """
    Get name of the Dead Man's Snitch secret for currently used addon.

    Returns:
        string: name of the secret
    """
    return (
        config.ENV_DATA["addon_name"] + constants.MANAGED_DEADMANSSNITCH_SECRET_SUFFIX
    )


def get_parameters_secret_name():
    """
    Get name of the addon parameters secret for currently used addon.

    Returns:
        string: name of the secret
    """
    return (
        constants.MANAGED_PARAMETERS_SECRET_PREFIX
        + config.ENV_DATA["addon_name"]
        + constants.MANAGED_PARAMETERS_SECRET_SUFFIX
    )


def update_pull_secret():
    """
    Update pull secret with extra quay.io/rhceph-dev credentials.

    Note: This is a hack done to allow odf to odf deployment before full addon is available.
    """
    oc = ocp.OCP(kind=constants.SECRET, namespace="openshift-config")
    logger.info("Update pull secret")
    pull_secret = oc.exec_oc_cmd("get -n openshift-config secret/pull-secret -o yaml")
    secret_data = pull_secret["data"][".dockerconfigjson"]
    secret_data = base64.b64decode(secret_data).decode()
    rhceph_dev_key = config.AUTH["quay-rhceph-dev-auth"]
    secret_data = json.loads(secret_data)
    secret_data["quay.io/rhceph-dev"] = {"auth": rhceph_dev_key, "email": ""}
    secret_data = str.encode(json.dumps(secret_data))
    with tempfile.NamedTemporaryFile() as secret_file:
        secret_file.write(secret_data)
        secret_file.flush()
        exec_cmd(
            f"oc set data secret/pull-secret -n openshift-config --from-file=.dockerconfigjson={secret_file.name}"
        )


def get_consumer_names():
    """
    Get the names of all consumers connected to this provider cluster.
    Runs on provider cluster

    Returns:
        list: names of all connected consumers, empty list if there are none
    """
    consumer = ocp.OCP(
        kind="StorageConsumer", namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    consumer_yamls = consumer.get().get("items")
    return [consumer["metadata"]["name"] for consumer in consumer_yamls]


def get_ceph_clients():

    """
    Get the yamls of all ceph clients.
    Runs on provider cluster

    Returns:
        list: yamls of all ceph clients
    """
    consumer = ocp.OCP(kind="CephClient", namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    return consumer.get().get("items")


def patch_consumer_toolbox(ceph_admin_key=None):
    """
    Patch the rook-ceph-tools deployment with ceph.admin key. Applicable for MS platform only to enable rook-ceph-tools
    to run ceph commands.

    Args:
        ceph_admin_key (str): The ceph admin key which should be used to patch rook-ceph-tools deployment on consumer

    """

    # Get the admin key if available
    ceph_admin_key = (
        ceph_admin_key
        or os.environ.get("CEPHADMINKEY")
        or config.AUTH.get("external", {}).get("ceph_admin_key")
    )

    if not ceph_admin_key:
        # TODO: Get the key from provider rook-ceph-tools pod after implementing multicluster deployment
        logger.warning(
            "Ceph admin key not found to patch rook-ceph-tools deployment on consumer with ceph.admin key. "
            "Skipping the step."
        )
        return

    consumer_tools_pod = get_ceph_tools_pod()

    # Check whether ceph command is working on tools pod. Patch is needed only if the error is "RADOS permission error"
    try:
        consumer_tools_pod.exec_ceph_cmd("ceph health")
        return
    except Exception as exc:
        if "RADOS permission error" not in str(exc):
            logger.warning(
                f"Ceph command on rook-ceph-tools deployment is failing with error {str(exc)}. "
                "This error cannot be fixed by patching the rook-ceph-tools deployment with ceph admin key."
            )
            return

    consumer_tools_deployment = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        resource_name="rook-ceph-tools",
    )
    patch_value = (
        f'[{{"op": "replace", "path": "/spec/template/spec/containers/0/env", '
        f'"value":[{{"name": "ROOK_CEPH_USERNAME", "value": "client.admin"}}, '
        f'{{"name": "ROOK_CEPH_SECRET", "value": "{ceph_admin_key}"}}]}}]'
    )
    try:
        consumer_tools_deployment.patch(params=patch_value, format_type="json")
    except Exception as exe:
        logger.warning(
            "Failed to patch rook-ceph-tools deployment in consumer cluster. "
            f"The patch can be applied manually after deployment. Error {str(exe)}"
        )
        return

    # Wait for the existing tools pod to delete
    consumer_tools_pod.ocp.wait_for_delete(resource_name=consumer_tools_pod.name)

    # Wait for the new tools pod to reach Running state
    new_tools_pod_info = get_pods_having_label(
        label=constants.TOOL_APP_LABEL,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    )[0]
    new_tools_pod = Pod(**new_tools_pod_info)
    helpers.wait_for_resource_state(new_tools_pod, constants.STATUS_RUNNING)
