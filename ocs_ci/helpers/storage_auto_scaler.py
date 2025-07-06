import logging
import time

from ocs_ci.ocs.exceptions import ResourceWrongStatusException, TimeoutExpiredError
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import get_percent_used_capacity, get_osd_utilization

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
    Generate a safe scaling threshold based on current Ceph usage.

    This function calculates a default scaling threshold that avoids triggering
    scaling too soon. It compares:
    - Ceph's overall used capacity percentage
    - The highest OSD's individual usage percentage

    It selects the larger of these two and ensures the scaling threshold is at least
    'min_diff' percent higher than that usage value. If the provided default threshold
    is too close to the current usage, it is increased accordingly.

    Args:
        default_threshold (int): The initial threshold to start with (default: 30).
        min_diff (int): Minimum gap (in percentage points) between current usage
                        and scaling threshold to avoid premature scaling (default: 7).

    Returns:
        int: A safe and adjusted scaling threshold percentage.
    """
    ceph_used_capacity = get_percent_used_capacity()
    osds_per_used_capacity = get_osd_utilization()
    logger.info(
        f"Ceph percent used capacity = {ceph_used_capacity}, "
        f"OSDs used capacity = {osds_per_used_capacity}"
    )

    max_osd_used_capacity = max(osds_per_used_capacity.values())
    max_used_capacity = max(max_osd_used_capacity, ceph_used_capacity)
    scaling_threshold = default_threshold

    if scaling_threshold - min_diff < max_used_capacity:
        logger.info(
            f"The scaling_threshold {scaling_threshold} is too close to the used "
            f"capacity {max_used_capacity}. Increasing the scaling_threshold."
        )
        scaling_threshold = int(max_used_capacity) + min_diff

    return scaling_threshold


def verify_autoscaler_status_not_trigger(auto_scaler_name, namespace=None):
    """
    Verify that the StorageAutoscaler does not enter 'InProgress' and remains NotStarted.

    Args:
        auto_scaler_name (str): Name of the StorageAutoScaler resource
        namespace (str): Namespace of the autoscaler. Defaults to ENV_DATA namespace.
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]

    verify_not_trigger = False
    timeout = constants.PROMETHEUS_RECONCILE_TIMEOUT
    logger.info(f"Wait {timeout} seconds to verify the autoscaler doesn't trigger.")
    try:
        wait_for_auto_scaler_status(
            expected_status=constants.IN_PROGRES,
            namespace=namespace,
            resource_name=auto_scaler_name,
            timeout=timeout,
            sleep=30,
        )
    except TimeoutExpiredError:
        logger.info(
            "The StorageAutoScaler didn't reach the InProgress status as expected"
        )
        verify_not_trigger = True

    assert verify_not_trigger, "The StorageAutoScaler has been triggered"

    storage_auto_scaler = OCP(kind=constants.STORAGE_AUTO_SCALER, namespace=namespace)
    auto_scaler_status = storage_auto_scaler.get_resource_status(
        auto_scaler_name, "PHASE"
    )
    assert (
        auto_scaler_status == constants.NOT_STARTED
    ), f"The StorageAutoScaler is not in the expected status {constants.NOT_STARTED}"


def safe_teardown_delete_all_autoscalers(namespace=None):
    """
    Safely delete all StorageAutoScaler resources, ensuring Prometheus has reconciled since the last cleanup.

    This function performs the following:
    - Reads the last cluster cleanup timestamp from `config.RUN["cleanup_cluster_time"]`
    - If set, waits for a Prometheus reconcile timeout (default: 660 seconds) from that time
    - Then deletes all autoscaler resources in the given namespace

    This helps prevent premature scaling or leftover metrics issues caused by fast teardown/recreate cycles.

    Args:
        namespace (str): Namespace of the autoscaler. Defaults to ENV_DATA namespace.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]

    autoscaler_names = get_all_storage_autoscaler_names(namespace)
    if not autoscaler_names:
        logger.info("All the autoscaler objects have been deleted — skipping wait.")
        return

    cleanup_cluster_time = config.RUN.get("cleanup_cluster_time")
    if not cleanup_cluster_time:
        logger.info("No last cleanup cluster time recorded — skipping wait.")
        delete_all_storage_autoscalers(namespace)
        return

    time_remaining = (
        cleanup_cluster_time + constants.PROMETHEUS_RECONCILE_TIMEOUT - time.time()
    )

    if time_remaining > 0:
        logger.info(
            f"Waiting {int(time_remaining)} seconds from the last cleanup cluster time "
            f"to ensure Prometheus reconciliation completes before deleting autoscalers."
        )
        time.sleep(time_remaining)

    delete_all_storage_autoscalers(namespace)
