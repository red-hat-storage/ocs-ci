from ocs_ci.ocs.cluster import (
    logger,
    get_percent_used_capacity,
    get_ceph_config_property,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd


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


def update_mon_target_pg(new_value):
    """
    Update mon_target_pg_per_osd to new_value

    Args:
        new value of mon_target_pg_per_osd

    Returns:
        True: if the value was changed successfully
        False: otherwise
    """
    patch_path = (
        f'{{"spec": {{"managedResources": '
        f'{{"cephCluster"": {{"cephConfig": {{"global": '
        f'{{"mon_target_pg_per_osd": {new_value}}}}}}}}}}}}}'
    )
    patch_cmd = (
        "oc patch storagecluster ocs-storagecluster -n "
        f"openshift-storage --type merge --patch '{patch_path}'"
    )
    cmd_res = exec_cmd(patch_cmd)
    if cmd_res.returncode != 0:
        logger.error(f"Failed to patch storagecluster. Error: {cmd_res.stderr}")
        return False
    mon_target_pg = get_ceph_config_property("mon", "mon_target_pg_per_osd")
    if mon_target_pg == new_value:
        logger.info(f"mon_target_pg_per_osd successfully changed to {new_value}")
        return True
    else:
        logger.info(
            f"mon_target_pg_per_osd should be {new_value}, but it is {mon_target_pg}"
        )
        return False
