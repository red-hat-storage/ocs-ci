"""
Helpers for interacting with the csi-addons-config ConfigMap.

Provides generic get/set operations for any key in the ConfigMap.
Used by set_schedule_precedence() in helpers.py and by stagger tests.
"""

import json
import logging
import os
import tempfile

import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def update_csi_addons_config(key: str, value: str) -> None:
    """
    Create or update a key in the 'csi-addons-config' ConfigMap and restart
    the CSI Addons controller manager so the change is picked up.

    Handles both cases: ConfigMap exists / does not exist.
    Uses JSON merge patch to preserve existing keys.

    Args:
        key (str): The ConfigMap data key to set (e.g. 'cronjob-stagger-window').
        value (str): The value to assign to the key.

    Raises:
        CommandFailed: If the ConfigMap patch or create operation fails.

    """
    configmap_name = constants.CSI_ADDONS_CONFIGMAP_NAME
    namespace = config.ENV_DATA.get("cluster_namespace", "openshift-storage")

    cm_ocp = OCP(kind=constants.CONFIGMAP, namespace=namespace)

    if cm_ocp.is_exist(configmap_name):
        patch_payload = json.dumps({"data": {key: value}})
        logger.info(
            "Patching ConfigMap '%s' in ns '%s': %s=%s",
            configmap_name,
            namespace,
            key,
            value,
        )
        cm_ocp.exec_oc_cmd(
            f"patch configmap {configmap_name} -p '{patch_payload}' --type=merge",
            out_yaml_format=False,
        )
    else:
        logger.info(
            "Creating ConfigMap '%s' in ns '%s' with %s=%s",
            configmap_name,
            namespace,
            key,
            value,
        )
        cm_manifest = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": configmap_name, "namespace": namespace},
            "data": {key: value},
        }
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".yaml"
        ) as fp:
            yaml.safe_dump(cm_manifest, fp, sort_keys=False)
            tmp_path = fp.name
        try:
            cm_ocp.exec_oc_cmd(f"apply -f {tmp_path}", out_yaml_format=False)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    logger.info("ConfigMap '%s' updated: %s=%s", configmap_name, key, value)

    logger.info("Restarting CSI Addons controller manager pods...")
    pod.restart_pods_having_label(
        label=constants.CSI_ADDONS_CONTROLLER_MANAGER_LABEL,
        namespace=namespace,
    )

    logger.info("Waiting for CSI Addons controller manager pod to be Ready...")
    for pods_data in TimeoutSampler(
        timeout=120,
        sleep=10,
        func=pod.get_pods_having_label,
        label=constants.CSI_ADDONS_CONTROLLER_MANAGER_LABEL,
        namespace=namespace,
        statuses=[constants.STATUS_RUNNING],
    ):
        if pods_data:
            logger.info("CSI Addons controller manager pod is Ready.")
            break


def get_csi_addons_config_value(key: str, default: str = "") -> str:
    """
    Read a value from the 'csi-addons-config' ConfigMap.

    Returns the default if the ConfigMap does not exist, the key is missing,
    or the read operation fails.

    Args:
        key (str): The ConfigMap data key to read (e.g. 'cronjob-stagger-window').
        default (str): Fallback value when the key is absent. Defaults to "".

    Returns:
        str: The value from the ConfigMap, or *default* if unavailable.

    """
    configmap_name = constants.CSI_ADDONS_CONFIGMAP_NAME
    namespace = config.ENV_DATA.get("cluster_namespace", "openshift-storage")

    cm_ocp = OCP(kind=constants.CONFIGMAP, namespace=namespace)

    try:
        if not cm_ocp.is_exist(configmap_name):
            logger.info(
                "ConfigMap '%s' not found in ns '%s'; returning default '%s' for key '%s'.",
                configmap_name,
                namespace,
                default,
                key,
            )
            return default

        cm = cm_ocp.get(resource_name=configmap_name)
        value = cm.get("data", {}).get(key, "").strip()

        if not value:
            logger.info(
                "Key '%s' missing or empty in ConfigMap '%s'; returning default '%s'.",
                key,
                configmap_name,
                default,
            )
            return default

        logger.info("ConfigMap '%s' key '%s' = '%s'", configmap_name, key, value)
        return value

    except CommandFailed as e:
        logger.warning(
            "Failed to read ConfigMap '%s' in ns '%s' (%s); returning default '%s'.",
            configmap_name,
            namespace,
            e,
            default,
        )
        return default
