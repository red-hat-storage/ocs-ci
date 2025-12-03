import logging

from ocs_ci.ocs.cluster import get_percent_used_capacity
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

logger = logging.getLogger(__name__)


def wait_for_percent_used_capacity_reached(
    expected_used_capacity, timeout=1800, sleep=20
):
    """
    Wait until the used capacity percentage reaches or exceeds a specified threshold.

    This function repeatedly samples the current used capacity using
    `get_percent_used_capacity()` until it meets or exceeds the `expected_used_capacity`
    or until the timeout is reached.

    Args:
        expected_used_capacity (int or float): The percentage of used capacity to wait for.
        timeout (int): Maximum time to wait in seconds. Defaults to 1800 seconds (30 minutes).
        sleep (int): Time to wait between checks in seconds. Defaults to 20 seconds.

    Raises:
        TimeoutExpiredError: If the expected capacity is not reached within the timeout.

    """
    logger.info(f"Wait for the percent used capacity to reach {expected_used_capacity}")

    try:
        for used_capacity in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_percent_used_capacity,
        ):
            logger.info(f"Current percent used capacity = {used_capacity}%")
            if used_capacity >= expected_used_capacity:
                logger.info(
                    f"The expected percent used capacity {expected_used_capacity}% reached"
                )
                break
    except TimeoutExpiredError as ex:
        raise TimeoutExpiredError(
            f"Failed to reach the expected percent used capacity {expected_used_capacity}% "
            f"in the given timeout {timeout}"
        ) from ex


def get_ceph_used_capacity() -> float:
    """
    Return the cluster used Ceph capacity in GiB.

    Returns:
        float: Used capacity in GiB.

    """
    ct_pod = get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph df")
    total_used = int(output.get("stats").get("total_used_raw_bytes"))
    return total_used / constants.BYTES_IN_GB


def wait_for_ceph_used_capacity_reached(expected_used_capacity, timeout=1800, sleep=20):
    """
    Wait until the cluster used Ceph capacity in GiB reaches or exceeds a specified threshold.

    Args:
        expected_used_capacity (int|float): The used capacity in GiB to wait for.
        timeout (int): Maximum time to wait in seconds. Defaults to 1800 seconds (30 minutes).
        sleep (int): Time to wait between checks in seconds. Defaults to 20 seconds.

    Raises:
        TimeoutExpiredError: If the expected capacity is not reached within the timeout.

    """
    logger.info(
        f"Wait for the used Ceph capacity to reach {expected_used_capacity} GiB"
    )

    try:
        for used_gib in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_ceph_used_capacity,
        ):
            logger.info(f"Current used Ceph capacity = {used_gib} GiB")
            if used_gib >= expected_used_capacity:
                logger.info(
                    f"The expected used Ceph capacity {expected_used_capacity} GiB reached"
                )
                break
    except TimeoutExpiredError as ex:
        raise TimeoutExpiredError(
            f"Failed to reach the expected used Ceph capacity {expected_used_capacity} GiB "
            f"in the given timeout {timeout}"
        ) from ex
