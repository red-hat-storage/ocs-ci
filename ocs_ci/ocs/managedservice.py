import base64
import json
import logging
import requests
import tempfile
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.catalog_source import disable_specific_source
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


def disable_odf_operator_and_update_pull_secret():
    """
    For unreleased odf operator version -  Disable odf operator
      1. Disable odf operator
      2. Update pull secret
      3. Create a catalogSource using the ocs-registry image

    Note: This is a hack done to allow odf to odf deployment before full addon is available.
    """
    oc = ocp.OCP(kind=constants.SECRET, namespace="openshift-config")
    logger.info("Disable odf operator")
    disable_specific_source("redhat-operators")
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
    logger.info("Create a catalogSource using the ocs-registry image")
    olm_data = requests.get(
        "http://perf1.perf.lab.eng.bos.redhat.com/shberry/odfodf/files/olm.yaml"
    )
    olm_data = list(yaml.safe_load_all(olm_data.text))
    image = (
        config.DEPLOYMENT.get("ocs_registry_image")
        or config.DEPLOYMENT["default_ocs_registry_image"]
    )
    with tempfile.NamedTemporaryFile() as olm_file:
        for olm_yaml in olm_data:
            olm_file.write(str.encode("---\n"))
            if olm_yaml.get("spec").get("image"):
                olm_yaml["spec"]["image"] = image
            olm_file.write(str.encode(yaml.dump(olm_yaml)))
        olm_file.flush()
        exec_cmd(f"oc create -f {olm_file.name}")
