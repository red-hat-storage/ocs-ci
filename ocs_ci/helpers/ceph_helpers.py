import logging

from ocs_ci.ocs.cluster import (
    get_percent_used_capacity,
    get_ceph_used_capacity,
    get_ceph_pool_property,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.helpers.helpers import get_failure_domain
from ocs_ci.utility.decorators import switch_to_provider_for_function
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import constants

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


def get_mon_quorum_ranks() -> dict:
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


def get_mon_status(mon_id: str) -> str:
    """
    Check the status of a monitor by verifying its presence in the quorum.

    Args:
        mon_id: The monitor ID (e.g., 'a', 'b', 'c') to check

    Returns:
        str: MON_STATUS_UP if the monitor is in quorum, MON_STATUS_DOWN otherwise.

    """
    mon_quorum_ranks = get_mon_quorum_ranks()
    logger.info(f"Current monitor quorum ranks: {mon_quorum_ranks}")

    if mon_id in mon_quorum_ranks:
        return constants.MON_STATUS_UP
    else:
        return constants.MON_STATUS_DOWN


def wait_for_mon_status(
    mon_id: str,
    status: str = constants.MON_STATUS_UP,
    timeout: int = 300,
    sleep: int = 20,
) -> None:
    """
    Wait until a specified monitor reaches the desired status

    Args:
        mon_id: The monitor ID (e.g., 'a', 'b', 'c') to check
        status: The desired status to wait for ('up' or 'down'). Defaults to 'up'.
        timeout: Maximum time to wait in seconds. Defaults to 300 seconds.
        sleep: Time to wait between checks in seconds. Defaults to 20 seconds.

    Raises:
        TimeoutExpiredError: If the monitor does not reach the desired status within the timeout.

    """
    if status not in [constants.MON_STATUS_UP, constants.MON_STATUS_DOWN]:
        raise ValueError(f"Invalid status: {status}")

    logger.info(f"Waiting for monitor {mon_id} to reach status: {status}")
    try:
        for current_status in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_mon_status,
            mon_id=mon_id,
        ):
            logger.info(f"Current status of monitor {mon_id}: {current_status}")
            if current_status == status:
                logger.info(f"Monitor {mon_id} reached the desired status: {status}")
                break
    except TimeoutExpiredError as ex:
        raise TimeoutExpiredError(
            f"Monitor {mon_id} did not reach the desired status {status} "
            f"within the given timeout {timeout}."
        ) from ex


def get_mon_quorum_count() -> int:
    """
    Get the current number of monitors in quorum.

    Returns:
        int: The number of monitors currently in quorum.

    """
    mon_quorum_ranks = get_mon_quorum_ranks()
    return len(mon_quorum_ranks)


@switch_to_provider_for_function
def get_ec_drain_thresholds(pool_name=None):
    """
    Calculate how many OSD-host shutdowns an EC pool can tolerate at each tier.

    Args:
        pool_name (str): CephBlockPool CR name. Defaults to DEFAULT_CEPHBLOCKPOOL.

    Returns:
        dict: Threshold values with keys k, m, size (k+m), min_size,
            failure_domain, total_osd_hosts, max_drain_io_ok, and
            min_drain_io_stops.

    Raises:
        ValueError: If the pool is not erasure-coded.

    """
    from ocs_ci.framework import config

    pool_name = pool_name or constants.DEFAULT_CEPHBLOCKPOOL
    namespace = config.ENV_DATA["cluster_namespace"]

    cbp = OCP(kind="CephBlockPool", namespace=namespace, resource_name=pool_name).get()
    ec_spec = cbp.get("spec", {}).get("erasureCoded", {})
    k = ec_spec.get("dataChunks")
    m = ec_spec.get("codingChunks")
    if not k or not m:
        raise ValueError(
            f"Pool {pool_name} is not erasure-coded " f"(erasureCoded spec: {ec_spec})"
        )

    size = int(get_ceph_pool_property(pool_name, "size"))
    min_size = int(get_ceph_pool_property(pool_name, "min_size"))
    failure_domain = get_failure_domain()
    total_osd_hosts = len(get_osd_running_nodes())

    if failure_domain != "host":
        logger.warning(
            f"Failure domain is '{failure_domain}', not 'host'. "
            f"Host-based drain thresholds may not be accurate."
        )

    max_drain_io_ok = total_osd_hosts - min_size
    min_drain_io_stops = max_drain_io_ok + 1

    result = {
        "k": k,
        "m": m,
        "size": size,
        "min_size": min_size,
        "failure_domain": failure_domain,
        "total_osd_hosts": total_osd_hosts,
        "max_drain_io_ok": max_drain_io_ok,
        "min_drain_io_stops": min_drain_io_stops,
    }
    logger.info(f"EC drain thresholds for pool {pool_name}: {result}")
    return result


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
