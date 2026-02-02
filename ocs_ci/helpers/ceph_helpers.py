from ocs_ci.ocs.cluster import logger, get_percent_used_capacity
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
    
def cleanup_stale_cephfs_subvolumes(odf_cli_runner, log):
    """
    Runbook-aligned mitigation to clean up stale CephFS subvolumes using ODF CLI.

    Steps:
        1. List stale subvolumes
        2. Delete each stale subvolume
        3. Verify cleanup

    Args:
        odf_cli_runner: ODF CLI runner instance used to execute CephFS subvolume commands.
        log: Logger instance used to log cleanup progress and results.
    """
    log.info("Running cleanup of stale CephFS subvolumes using ODF CLI")

    try:
        result = odf_cli_runner.list_stale_cephfs_subvolumes()
        out = result.stdout.decode().strip()

        log.info(f"Raw output of 'odf subvolume ls --stale': {out}")

        if not out:
            log.info("No stale subvolumes found")
            return

        lines = out.splitlines()

        # Skip header
        if len(lines) <= 1:
            log.info("No stale subvolumes found after header parsing")
            return

        for line in lines[1:]:
            parts = line.split()

            # Safety guard
            if len(parts) < 3:
                log.warning(f"Unexpected odf output format: {line}")
                continue

            # Expected format:
            # Filesystem  Subvolume  SubvolumeGroup  State
            filesystem = parts[0]
            subvol = parts[1]
            group = parts[2]

            log.info(
                f"Deleting stale subvolume: {subvol}, "
                f"fs: {filesystem}, group: {group}"
            )

            delete_out = odf_cli_runner.run_command(
                f"subvolume delete {filesystem} {subvol} {group}"
            )
            log.info(f"Delete output for {subvol}: {delete_out}")

        # Verify cleanup
        verify_result = odf_cli_runner.list_stale_cephfs_subvolumes()
        remaining = verify_result.stdout.decode().strip()
        log.info(f"Post-cleanup stale list: {remaining}")

        if remaining and "stale" in remaining.lower():
            log.warning(
                f"Stale subvolumes still present after cleanup: {remaining}"
            )
        else:
            log.info("All stale subvolumes successfully cleaned up")

    except Exception as e:
        log.error(f"Failed to cleanup stale subvolumes: {e}")


