"""
StorageCluster related helpers.

Note: This file shows ONLY the new ReadAffinity helper functions that must be
appended to the existing ocs_ci/ocs/resources/storage_cluster.py module.
In practice, paste these functions at the end of the existing file.
"""

import json
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def get_storage_cluster_name(namespace=None):
    """
    Get the name of the StorageCluster CR in the given namespace.

    Args:
        namespace (str): The namespace to look in. Defaults to cluster namespace.

    Returns:
        str: The name of the StorageCluster CR.
    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    sc_obj = ocp.OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    sc_list = sc_obj.get()
    items = sc_list.get("items", [])
    if not items:
        raise RuntimeError(f"No StorageCluster found in namespace {namespace}")
    return items[0]["metadata"]["name"]


def get_read_affinity_spec(storage_cluster_name, namespace=None):
    """
    Get the ReadAffinity spec from the StorageCluster CR.

    Args:
        storage_cluster_name (str): Name of the StorageCluster CR.
        namespace (str): The namespace of the StorageCluster. Defaults to cluster namespace.

    Returns:
        tuple: (bool, dict) - (whether readAffinity exists, the readAffinity spec dict)
    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    sc_obj = ocp.OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    sc_data = sc_obj.get(resource_name=storage_cluster_name)
    csi_spec = sc_data.get("spec", {}).get("csi", {})
    ra_spec = csi_spec.get("readAffinity", None)
    if ra_spec is None:
        return False, {}
    return True, ra_spec


def get_read_affinity_enabled(storage_cluster_name, namespace=None):
    """
    Get the enabled state of ReadAffinity from the StorageCluster CR.

    Args:
        storage_cluster_name (str): Name of the StorageCluster CR.
        namespace (str): The namespace of the StorageCluster. Defaults to cluster namespace.

    Returns:
        bool or None: The enabled state, or None if readAffinity is not set.
    """
    exists, ra_spec = get_read_affinity_spec(storage_cluster_name, namespace=namespace)
    if not exists:
        return None
    return ra_spec.get("enabled", None)


def patch_read_affinity(storage_cluster_name, enabled, namespace=None):
    """
    Patch the StorageCluster CR to enable or disable ReadAffinity.

    Args:
        storage_cluster_name (str): Name of the StorageCluster CR.
        enabled (bool): Whether to enable or disable ReadAffinity.
        namespace (str): The namespace of the StorageCluster. Defaults to cluster namespace.

    Returns:
        bool: True if the patch was successful.
    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    sc_obj = ocp.OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    patch_data = json.dumps({"spec": {"csi": {"readAffinity": {"enabled": enabled}}}})
    logger.info(f"Patching StorageCluster {storage_cluster_name} with readAffinity.enabled={enabled}")
    sc_obj.patch(
        resource_name=storage_cluster_name,
        params=patch_data,
        format_type="merge",
    )
    logger.info(f"Successfully patched StorageCluster {storage_cluster_name}")
    return True


def patch_read_affinity_spec(storage_cluster_name, ra_spec, namespace=None):
    """
    Patch the StorageCluster CR with an exact readAffinity spec dict.

    This is used to restore the original readAffinity spec exactly as it was,
    including cases where 'enabled' was absent but other fields like
    'crushLocationLabels' were present.

    Args:
        storage_cluster_name (str): Name of the StorageCluster CR.
        ra_spec (dict): The exact readAffinity spec to set.
        namespace (str): The namespace of the StorageCluster. Defaults to cluster namespace.

    Returns:
        bool: True if the patch was successful.
    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    sc_obj = ocp.OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    patch_data = json.dumps({"spec": {"csi": {"readAffinity": ra_spec}}})
    logger.info(f"Patching StorageCluster {storage_cluster_name} readAffinity spec to {ra_spec}")
    sc_obj.patch(
        resource_name=storage_cluster_name,
        params=patch_data,
        format_type="merge",
    )
    logger.info(f"Successfully patched StorageCluster {storage_cluster_name} readAffinity spec")
    return True


def get_read_affinity_from_cluster_config(namespace=None):
    """
    Get the ReadAffinity configuration from the cluster's CephConnection
    resources or the ceph-csi-configs ConfigMap.

    First attempts to read from CephConnection resources. If none are found,
    falls back to the ceph-csi-configs ConfigMap.

    Note: This function raises on read/parse failures so that callers
    (especially polling loops) can distinguish between "confirmed absent"
    (returns None) and "failed to read" (raises exception).

    Args:
        namespace (str): The namespace to look in. Defaults to cluster namespace.

    Returns:
        dict or None: The readAffinity configuration dict, or None if
            confirmed absent after a successful read.

    Raises:
        CommandFailed: If the underlying API calls fail.
        json.JSONDecodeError: If ConfigMap JSON is malformed.
    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]

    # Try CephConnection resources first
    cephconnection_available = True
    try:
        cc_obj = ocp.OCP(kind=constants.CEPHCONNECTION_KIND, namespace=namespace)
        cc_list = cc_obj.get()
        items = cc_list.get("items", [])
        if items:
            for item in items:
                ra = item.get("spec", {}).get("readAffinity", None)
                if ra is not None:
                    logger.debug(
                        f"Found readAffinity in CephConnection {item['metadata']['name']}: {ra}"
                    )
                    return ra
            logger.debug("CephConnection resources found but no readAffinity spec present")
            return None
    except CommandFailed:
        logger.debug("CephConnection CRD not available, falling back to ConfigMap")
        cephconnection_available = False

    if not cephconnection_available:
        # Fallback to ceph-csi-configs ConfigMap — let exceptions propagate
        cm_obj = ocp.OCP(kind="ConfigMap", namespace=namespace)
        cm_data = cm_obj.get(resource_name=constants.CEPH_CSI_CONFIGS_CONFIGMAP)
        config_json = cm_data.get("data", {}).get("config.json", "[]")
        config_list = json.loads(config_json)
        if config_list and isinstance(config_list, list):
            for entry in config_list:
                ra = entry.get("readAffinity", None)
                if ra is not None:
                    logger.debug(f"Found readAffinity in ceph-csi-configs ConfigMap: {ra}")
                    return ra
        logger.debug("No readAffinity found in ceph-csi-configs ConfigMap")
        return None

    return None


def wait_for_read_affinity_state(expected_enabled, namespace=None, timeout=300, poll_interval=10):
    """
    Wait for the ReadAffinity state to propagate to CephConnection or ConfigMap.

    Uses TimeoutSampler to poll the cluster configuration until the expected
    ReadAffinity enabled state is observed or the timeout is reached.

    Args:
        expected_enabled (bool): The expected enabled state of ReadAffinity.
        namespace (str): The namespace to look in. Defaults to cluster namespace.
        timeout (int): Maximum time to wait in seconds.
        poll_interval (int): Interval between polls in seconds.

    Returns:
        bool: True if the expected state was observed within the timeout.

    Raises:
        TimeoutError: If the expected state is not observed within the timeout.
    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]

    logger.info(
        f"Waiting up to {timeout}s for ReadAffinity enabled={expected_enabled} "
        f"to propagate to cluster config"
    )

    def _check_read_affinity():
        """
        Check the current ReadAffinity state from cluster config.

        Returns the readAffinity dict or None. Exceptions from
        get_read_affinity_from_cluster_config propagate to TimeoutSampler
        which will log a warning and retry.
        """
        return get_read_affinity_from_cluster_config(namespace=namespace)

    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=poll_interval,
        func=_check_read_affinity,
    ):
        if sample is not None:
            current_enabled = sample.get("enabled", None)
            if current_enabled == expected_enabled:
                logger.info(
                    f"ReadAffinity enabled={expected_enabled} confirmed in cluster config"
                )
                return True
            logger.debug(
                f"ReadAffinity current enabled={current_enabled}, "
                f"expected={expected_enabled}, retrying..."
            )
        else:
            if not expected_enabled:
                logger.info(
                    "ReadAffinity absent from cluster config, consistent with disabled state"
                )
                return True
            logger.debug("ReadAffinity not yet present in cluster config, retrying...")

    # TimeoutSampler raises TimeoutExpiredError if we never return True
    return False