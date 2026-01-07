import logging

from ocs_ci.ocs.cluster import get_percent_used_capacity, get_ceph_used_capacity
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.utility.utils import TimeoutSampler

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


def get_mon_quorum_ranks():
    """
    Returns a mapping of mon names to ranks using 'ceph mon stat'.
    Only monitors currently in quorum are returned.

    Returns:
        dict: A dictionary mapping monitor names to their ranks.

    """
    ceph_tools_pod = get_ceph_tools_pod()
    # Execute the command to get monitor status data
    data = dict(ceph_tools_pod.exec_cmd_on_pod(command="ceph mon stat --format json"))

    # Build the dictionary directly from the quorum list
    # data['quorum'] looks like: [{"rank": 0, "name": "a"}, {"rank": 1, "name": "b"}]
    return {mon["name"]: mon["rank"] for mon in data.get("quorum", [])}


def is_mon_down(mon_id):
    """
    Check if a monitor is down by verifying its absence in the quorum list.

    Args:
        mon_id (str): The monitor ID (e.g., 'a', 'b', 'c') to check.

    Returns:
        bool: True if the monitor is down, False otherwise.

    """
    mon_quorum_ranks = get_mon_quorum_ranks()
    logger.info(f"Current monitor quorum ranks: {mon_quorum_ranks}")

    # If the monitor ID is not in the quorum ranks, it is considered down
    if mon_id not in mon_quorum_ranks:
        logger.info(f"Monitor {mon_id} is down.")
        return True
    else:
        logger.info(f"Monitor {mon_id} is up.")
        return False


def wait_for_mon_down(mon_id, timeout=180, sleep=10):
    """
    Wait until a specified monitor is down.

    Args:
        mon_id (str): The monitor ID (e.g., 'a', 'b', 'c') to wait for.
        timeout (int): Maximum time to wait in seconds. Defaults to 300 seconds (5 minutes).
        sleep (int): Time to wait between checks in seconds. Defaults to 10 seconds.

    Raises:
        TimeoutExpiredError: If the monitor does not go down within the timeout.

    """
    logger.info(f"Waiting for monitor {mon_id} to go down.")
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=is_mon_down,
        mon_id=mon_id,
    )
    if not sample.wait_for_func_status(result=True):
        raise TimeoutExpiredError(
            f"Monitor {mon_id} did not go down within {timeout} seconds."
        )


def wait_for_mon_up(mon_id, timeout=300, sleep=20):
    """
    Wait until a specified monitor is up.

    Args:
        mon_id (str): The monitor ID (e.g., 'a', 'b', 'c') to wait for.
        timeout (int): Maximum time to wait in seconds. Defaults to 300 seconds (5 minutes).
        sleep (int): Time to wait between checks in seconds. Defaults to 10 seconds.

    Raises:
        TimeoutExpiredError: If the monitor does not come up within the timeout.

    """
    logger.info(f"Waiting for monitor {mon_id} to come up.")
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=is_mon_down,
        mon_id=mon_id,
    )
    if not sample.wait_for_func_status(result=False):
        raise TimeoutExpiredError(
            f"Monitor {mon_id} did not come up within {timeout} seconds."
        )


def get_mon_quorum_count():
    """
    Get the current number of monitors in quorum.

    Returns:
        int: The number of monitors currently in quorum.

    """
    mon_quorum_ranks = get_mon_quorum_ranks()
    return len(mon_quorum_ranks)


def wait_for_mons_in_quorum(expected_mon_count, timeout=300, sleep=20):
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
