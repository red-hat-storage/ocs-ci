import logging

from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import get_percent_used_capacity

logger = logging.getLogger(__name__)


def get_all_storage_autoscaler_names(namespace=None):
    """
    Retrieve a list of all StorageAutoScaler resource names in the cluster namespace.

    Args:
        namespace (str): The namespace of the auto-scaler resources.

    Returns:
        list: A list of StorageAutoScaler names. Empty if none exist.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    storage_auto_scaler = OCP(kind=constants.STORAGE_AUTO_SCALER, namespace=namespace)
    result = storage_auto_scaler.get(dont_raise=True)
    if not result:
        return []

    autoscaler_items = result.get("items", [])
    return [item["metadata"]["name"] for item in autoscaler_items]


def delete_all_storage_autoscalers(namespace=None, wait=True, timeout=120, force=False):
    """
    Delete all StorageAutoScaler custom resources in the cluster namespace.

    Args:
        namespace (str): The namespace of the auto-scaler resources.
        wait (bool): Whether to wait for deletion to complete.
        timeout (int): Time in seconds to wait for deletion of each resource.
        force (bool): Force deletion if standard deletion fails.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    storage_auto_scaler = OCP(kind=constants.STORAGE_AUTO_SCALER, namespace=namespace)
    autoscaler_names = get_all_storage_autoscaler_names(namespace=namespace)
    logger.info(f"storage-autoscaler objects to delete: {autoscaler_names}")

    for name in autoscaler_names:
        storage_auto_scaler.delete(
            resource_name=name,
            wait=wait,
            timeout=timeout,
            force=force,
        )


def wait_for_auto_scaler_status(
    expected_status, namespace=None, resource_name=None, timeout=600, sleep=10
):
    """
    Wait for the StorageAutoScaler resource to reach the desired status (PHASE column).

    Args:
        expected_status (str): The expected status value in the "PHASE" column
            (e.g., 'NotStarted', 'InProgress', 'Succeeded', 'Failed').
        namespace (str): The namespace of the auto-scaler resource.
        resource_name (str, optional): Name of the StorageAutoScaler resource.
            If not provided, the function will detect the first available one in the namespace.
        timeout (int): Maximum time in seconds to wait for the desired status. Default is 600 seconds.
        sleep (int): Interval in seconds between status checks. Default is 10 seconds.

    Raises:
        ResourceWrongStatusException: If no StorageAutoScaler resources are found.
        TimeoutExpiredError: If the expected status is not reached within the timeout duration.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    storage_auto_scaler = OCP(kind=constants.STORAGE_AUTO_SCALER, namespace=namespace)

    if not resource_name:
        logger.info(
            "Didn't get the storage-autoscaler name. Trying to get the first "
            "storage-autoscaler name..."
        )
        autoscaler_names = get_all_storage_autoscaler_names(namespace=namespace)
        if not autoscaler_names:
            raise ResourceWrongStatusException(
                f"Didn't find any resources for {constants.STORAGE_AUTO_SCALER}"
            )
        else:
            resource_name = autoscaler_names[0]

    storage_auto_scaler.wait_for_resource(
        condition=expected_status,
        resource_name=resource_name,
        column="PHASE",
        timeout=timeout,
        sleep=sleep,
    )


def generate_default_scaling_threshold(default_threshold=30, min_diff=7):
    """
    Generate a safe default scaling threshold based on current used capacity.

    Ensures the threshold is at least `min_diff` percent higher than the used capacity.

    Args:
        default_threshold (int): Initial threshold to use.
        min_diff (int): Minimum gap between used capacity and scaling threshold.

    Returns:
        int: A safe scaling threshold percentage.
    """
    used_capacity = get_percent_used_capacity()
    scaling_threshold = default_threshold

    if scaling_threshold - min_diff < used_capacity:
        scaling_threshold = used_capacity + min_diff
        logger.info(
            f"The scaling_threshold {scaling_threshold} is too close to the used "
            f"capacity {used_capacity}. Increasing the scaling_threshold"
        )

    return scaling_threshold
