import base64
import json
import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, ocp
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
