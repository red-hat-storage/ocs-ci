import logging
import subprocess
import threading
from contextlib import contextmanager

from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility.utils import (
    remove_ceph_crashes,
    get_ceph_crashes,
    log_all_ceph_crash_details,
    format_ceph_crash_summary_lines,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    CephHealthException,
    NoRunningCephToolBoxException,
)
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)

# Default interval (seconds) for periodic Ceph crash checks during long-running tests.
CEPH_CRASH_POLL_INTERVAL = 180


class CephStatusTool:
    """
    Class to check the health of Ceph cluster.
    """

    def __init__(self):
        """
        Initialize the CephHealthCheck class.
        """
        self.ceph_health = ceph_health_check
        self.ceph_crashes = get_ceph_crashes
        self.remove_ceph_crashes = remove_ceph_crashes
        self.toolbox = pod.get_ceph_tools_pod()

    @retry(CommandFailed, tries=8, delay=3)
    def wait_till_ceph_status_became_healthy(self):
        """
        Get the status of the Ceph cluster.

        Returns:
            str: The status of the Ceph cluster.
        """

        log.info("Performing post-failure injection checks...")
        try:
            ceph_health_check(fix_ceph_health=True)
        except (
            CephHealthException,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ) as e:
            log.error(f"Ceph health check failed after failure injection. : {e}")

    def check_ceph_crashes(self):
        """
        Check for any Ceph crashes and log full ``ceph crash info`` for each one.

        When multiple crashes exist, iterates over every entry from
        ``ceph crash ls`` and prints detailed info per crash.

        Returns:
            bool: True if crashes are found, False otherwise.
        """
        ceph_crash_ids = self.ceph_crashes(self.toolbox)
        if not ceph_crash_ids:
            return False
        log.error("Ceph crash ID(s) found: %s", ceph_crash_ids)
        log_all_ceph_crash_details(self.toolbox)
        return True

    def archive_ceph_crashes(self):
        """
        Archive any existing Ceph crash logs.
        """
        log.info("Removing any existing Ceph crash logs...")
        self.remove_ceph_crashes(self.toolbox)
        log.info("Ceph crash logs archived successfully.")
        return True

    def ceph_status_details(self):
        """
        Get detailed status of the Ceph cluster.

        Returns:
            str: The detailed status of the Ceph cluster.
        """
        ceph_status = {}
        try:
            ceph_status = self.toolbox.exec_cmd_on_pod(
                "ceph -s --format json-pretty", timeout=60
            )
        except (
            CephHealthException,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ) as ex:
            log.error(f"Failed to get Ceph status: {ex}")

        return ceph_status

    def is_ceph_health_ok(self):
        """
        Get the status of the Ceph cluster.

        Returns:
            str: The status of the Ceph cluster.
        """
        if (
            self.ceph_status_details().get("health", {}).get("status", "")
            == "HEALTH_OK"
        ):
            log.info("Ceph cluster is healthy.")
            return True
        log.error("Ceph cluster is not healthy.")
        return False

    def get_ceph_health(self, detail=False):
        """
        Get Ceph cluster health status.

        Args:
            detail (bool): If True, get detailed health information

        Returns:
            str: Ceph health status (e.g., "HEALTH_OK", "HEALTH_WARN", "HEALTH_ERR")
        """
        try:
            ceph_health_cmd = "ceph health"
            if detail:
                ceph_health_cmd = f"{ceph_health_cmd} detail"

            health_output = self.toolbox.exec_cmd_on_pod(
                ceph_health_cmd, out_yaml_format=False, timeout=60
            )

            # Extract just the health status from the output
            if isinstance(health_output, str):
                return health_output.strip().split()[0]
            return health_output

        except (
            CephHealthException,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ) as ex:
            log.error(f"Failed to get Ceph health: {ex}")
            return "HEALTH_ERR"

    def get_ceph_crashes(self):
        """
        Get list of Ceph crashes.

        Returns:
            list: List of Ceph crash information dictionaries
        """
        try:
            # Get full crash objects instead of just IDs
            ceph_crashes = self.toolbox.exec_ceph_cmd("ceph crash ls")
            return ceph_crashes if ceph_crashes else []
        except (
            CephHealthException,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ) as ex:
            log.error(f"Failed to get Ceph crashes: {ex}")
            return []


def _format_ceph_crash_assertion_message(ceph_tool, chaos_type, poll_interval):
    """Build a detailed AssertionError message when Ceph crashes are present."""
    log.error("Ceph crashes detected during %s", chaos_type)
    # Per-crash ``ceph crash info`` output is already logged by check_ceph_crashes().
    crashes = ceph_tool.get_ceph_crashes()

    error_msg = f"Ceph crashes detected during {chaos_type}."
    if crashes:
        error_msg += f" Found {len(crashes)} crash(es):\n"
        error_msg += "\n".join(format_ceph_crash_summary_lines(crashes))
        error_msg += (
            "\n\nFull ``ceph crash info <crash_id>`` output was logged above "
            "for every crash."
        )
    else:
        error_msg += " Unable to retrieve crash details."

    if poll_interval:
        full_msg = (
            f"Periodic Ceph crash check failed (every {poll_interval} s). "
            f"Ceph crash detected; failing test to generate evidence.\n{error_msg}"
        )
    else:
        full_msg = f"Ceph crash check failed. {error_msg}"
    log.error(full_msg)
    return full_msg


def raise_if_ceph_crashes_detected(
    ceph_tool,
    chaos_type,
    poll_interval=CEPH_CRASH_POLL_INTERVAL,
):
    """
    Run a Ceph crash check and raise AssertionError when any crash is found.

    Args:
        ceph_tool (CephStatusTool): Tool used to query Ceph crashes.
        chaos_type (str): Context for log/assert messages.
        poll_interval (int): Interval in seconds for periodic checks (message only).
            Pass 0 or None to omit the interval from the error message.

    Raises:
        AssertionError: If Ceph crash(es) are detected or the check fails.
    """
    try:
        crashes_found = ceph_tool.check_ceph_crashes()
    except Exception as ex:
        log.error("Failed to check Ceph crashes: %s", ex)
        raise AssertionError(f"Failed to check Ceph crashes: {ex}") from ex

    if not crashes_found:
        return

    interval = poll_interval or None
    raise AssertionError(
        _format_ceph_crash_assertion_message(ceph_tool, chaos_type, interval)
    )


class CephCrashMonitor(threading.Thread):
    """
    Background thread that checks for Ceph crashes every ``interval`` seconds.

    Used during long-running resiliency failure injection so tests fail promptly
    when a crash appears instead of only at post-scenario teardown.
    """

    def __init__(
        self,
        interval=CEPH_CRASH_POLL_INTERVAL,
        context="resiliency test",
    ):
        super().__init__(daemon=True, name="ceph-crash-monitor")
        self.interval = interval
        self.context = context
        self._stop = threading.Event()
        self.crash_error = None

    def run(self):
        ceph_tool = CephStatusTool()
        chaos_type = f"{self.context} (periodic check every {self.interval} s)"
        log.info(
            "Ceph crash monitor started for %s (check every %ss)",
            self.context,
            self.interval,
        )

        def _check_once():
            try:
                raise_if_ceph_crashes_detected(
                    ceph_tool,
                    chaos_type,
                    poll_interval=self.interval,
                )
                log.info(
                    "Ceph crash monitor: no crashes detected for %s; "
                    "next check in %ss",
                    self.context,
                    self.interval,
                )
                return True
            except AssertionError as ex:
                self.crash_error = ex
                log.error("Ceph crash detected by background monitor: %s", ex)
                return False

        if not _check_once():
            return
        while not self._stop.wait(self.interval):
            if not _check_once():
                break

    def stop(self):
        self._stop.set()

    def raise_if_crash_detected(self):
        """Re-raise any crash detected by the background thread."""
        if self.crash_error:
            raise self.crash_error

    def final_check(self):
        """Run a final crash check after failure injection completes."""
        raise_if_ceph_crashes_detected(
            CephStatusTool(),
            f"{self.context} (final check)",
            poll_interval=0,
        )


@contextmanager
def ceph_crash_monitor(
    enabled=True, interval=CEPH_CRASH_POLL_INTERVAL, context="resiliency test"
):
    """
    Context manager that starts a :class:`CephCrashMonitor` for the test body.

    Args:
        enabled (bool): When False, yields None and performs no monitoring.
        interval (int): Seconds between periodic crash checks.
        context (str): Description for log/assert messages.

    Yields:
        CephCrashMonitor or None
    """
    if not enabled:
        yield None
        return

    monitor = CephCrashMonitor(interval=interval, context=context)
    monitor.start()
    try:
        yield monitor
    finally:
        monitor.stop()
        monitor.join(timeout=120)
        monitor.raise_if_crash_detected()
        monitor.final_check()
