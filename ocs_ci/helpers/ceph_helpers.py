from ocs_ci.ocs.cluster import (
    logger,
    get_mon_quorum_ranks,
    get_percent_used_capacity,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler


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


def get_mon_quorum_count() -> int:
    """
    Get the current number of monitors in quorum.

    Returns:
        int: The number of monitors currently in quorum.

    """
    mon_quorum_ranks = get_mon_quorum_ranks()
    return len(mon_quorum_ranks)


def wait_for_mons_in_quorum(expected_mon_count, timeout=300, sleep=20) -> None:
    """
    Wait until the number of monitors in quorum reaches the expected count.

    Args:
        expected_mon_count (int): The expected number of monitors in quorum.
        timeout (int): Maximum time to wait in seconds. Defaults to 300 seconds (5 minutes).
        sleep (int): Time to wait between checks in seconds. Defaults to 10 seconds.

    Raises:
        TimeoutExpiredError: If the expected number of monitors in quorum is not reached within the timeout.

    """
    logger.info(f"Waiting for {expected_mon_count} monitors to be in quorum.")

    try:
        for current_count in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_mon_quorum_count,
        ):
            logger.info(f"Current monitors in quorum: {current_count}")
            if current_count >= expected_mon_count:
                logger.info(
                    f"The expected number of monitors {expected_mon_count} in quorum reached."
                )
                break
    except TimeoutExpiredError as ex:
        raise TimeoutExpiredError(
            f"Failed to reach the expected number of monitors {expected_mon_count} "
            f"in quorum within the given timeout {timeout}."
        ) from ex
