import logging
import subprocess
import threading
from contextlib import contextmanager
from dataclasses import dataclass

from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility.utils import (
    remove_ceph_crashes,
    get_ceph_crashes,
    log_all_ceph_crash_details,
    format_ceph_crash_summary_lines,
)
from ocs_ci.ocs import constants
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

CRASH_CHECK_CLEAN = "clean"
CRASH_CHECK_CRASHES = "crashes"
CRASH_CHECK_UNRELIABLE = "unreliable"

CRASH_COLLECTOR_AUTH_MARKERS = (
    "CephXAuthenticate",
    "failed to decode",
    "handle_request failed",
    "unable to authenticate",
    "auth: unable",
)


@dataclass
class CrashReportingStatus:
    """Whether ``ceph crash ls`` can be trusted as a complete crash list."""

    available: bool
    reason: str = ""


def assess_crash_reporting_status(
    collector_pods,
    collector_logs=None,
    crash_ls_error=None,
):
    """
    Distinguish "no crashes" from "crash reporting is broken".

    Args:
        collector_pods (list): ``[{"name": str, "phase": str}, ...]``
        collector_logs (iterable): Crash collector pod log blobs
        crash_ls_error: Exception raised by ``ceph crash ls``, if any

    Returns:
        CrashReportingStatus
    """
    if crash_ls_error is not None:
        return CrashReportingStatus(False, f"ceph crash ls failed: {crash_ls_error}")
    if not collector_pods:
        return CrashReportingStatus(False, "no crash collector pods found")

    not_running = [
        pod_info.get("name", "?")
        for pod_info in collector_pods
        if (pod_info.get("phase") or "") != constants.STATUS_RUNNING
    ]
    if not_running:
        return CrashReportingStatus(
            False, f"crash collector pods not Running: {not_running}"
        )

    for log_text in collector_logs or []:
        if not log_text:
            continue
        lowered = log_text if isinstance(log_text, str) else str(log_text)
        for marker in CRASH_COLLECTOR_AUTH_MARKERS:
            if marker in lowered:
                return CrashReportingStatus(
                    False, "crash collector authentication is failing"
                )
    return CrashReportingStatus(True)


def evaluate_crash_monitor_check(reporting_available, reporting_reason, crashes_found):
    """
    Decide the crash-monitor outcome without claiming a clean result when
    telemetry cannot be trusted.

    Returns:
        tuple: (outcome, message) where outcome is one of CRASH_CHECK_*
    """
    if not reporting_available:
        message = (
            f"Ceph crash reporting is unavailable ({reporting_reason}); "
            "not treating an empty crash list as a clean result"
        )
        return CRASH_CHECK_UNRELIABLE, message
    if crashes_found:
        return CRASH_CHECK_CRASHES, "Ceph crash(es) detected"
    return CRASH_CHECK_CLEAN, "no crashes detected"


# Resiliency/chaos tests treat recovery and degraded (HEALTH_WARN) as expected.
# Only HEALTH_ERR is a failure.
ACCEPTABLE_CEPH_HEALTH_STATES = {
    constants.CEPH_HEALTH_OK,
    constants.CEPH_HEALTH_WARN,
}


def get_ceph_health_status(health_output):
    """
    Extract the Ceph health token from ``ceph health`` output.

    Args:
        health_output: Raw ``ceph health`` / ``ceph health detail`` output.

    Returns:
        str: ``HEALTH_OK``, ``HEALTH_WARN``, ``HEALTH_ERR``, or ``HEALTH_ERR``
            when the output is missing.
    """
    if health_output is None:
        return constants.CEPH_HEALTH_ERROR
    text = str(health_output).strip()
    if not text:
        return constants.CEPH_HEALTH_ERROR
    return text.split()[0]


def is_ceph_health_acceptable(health_output):
    """
    Return True when Ceph health is acceptable for resiliency/chaos tests.

    HEALTH_OK and HEALTH_WARN (degraded, recovering, noout, mon down, etc.)
    pass. Only HEALTH_ERR — or a missing status — fails.

    Args:
        health_output: Raw health string or status token.

    Returns:
        bool: True if status is HEALTH_OK or HEALTH_WARN.
    """
    return get_ceph_health_status(health_output) in ACCEPTABLE_CEPH_HEALTH_STATES


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

    @retry(
        (
            AssertionError,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ),
        tries=15,
        delay=20,
        backoff=1,
    )
    def wait_till_ceph_status_became_healthy(self):
        """
        Wait until Ceph is not in HEALTH_ERR.

        HEALTH_OK and HEALTH_WARN (degraded, recovery, noout, mon down, etc.)
        are acceptable during resiliency, matching chaos test exit criteria.
        Only HEALTH_ERR fails the wait.

        Returns:
            bool: True when Ceph health is HEALTH_OK or HEALTH_WARN.

        Raises:
            AssertionError: If Ceph remains in HEALTH_ERR after retries.
        """
        log.info("Checking Ceph health (HEALTH_WARN is acceptable)...")
        health_status = self.get_ceph_health(detail=True)
        if is_ceph_health_acceptable(health_status):
            status = get_ceph_health_status(health_status)
            if status == constants.CEPH_HEALTH_WARN:
                log.warning(
                    "Ceph health is HEALTH_WARN (acceptable for resiliency; "
                    "recovery/degraded is not treated as failure): %s",
                    health_status,
                )
            else:
                log.info("Ceph cluster health is HEALTH_OK.")
            return True

        log.error("Ceph cluster is in error state: %s", health_status)
        raise AssertionError(
            f"Ceph cluster is in {constants.CEPH_HEALTH_ERROR} state "
            f"(status: {health_status})"
        )

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

    def verify_crash_reporting_available(self):
        """
        Verify crash collectors are Running and authenticating.

        An empty ``ceph crash ls`` is not a clean result when collectors are
        down or failing CephX auth.

        Returns:
            CrashReportingStatus
        """
        collector_pods = pod.get_crashcollector_pods()
        pods_info = []
        collector_logs = []
        for crash_pod in collector_pods:
            phase = (crash_pod.data.get("status") or {}).get("phase", "")
            pods_info.append({"name": crash_pod.name, "phase": phase})
            try:
                collector_logs.append(crash_pod.ocp.get_logs(name=crash_pod.name) or "")
            except Exception as ex:
                log.debug(
                    "Could not read crash collector logs for %s: %s",
                    crash_pod.name,
                    ex,
                )
                collector_logs.append("")

        crash_ls_error = None
        try:
            self.toolbox.exec_ceph_cmd("ceph crash ls")
        except Exception as ex:
            crash_ls_error = ex

        return assess_crash_reporting_status(
            collector_pods=pods_info,
            collector_logs=collector_logs,
            crash_ls_error=crash_ls_error,
        )

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

            if isinstance(health_output, str):
                health_output = health_output.strip()
                if detail:
                    return health_output
                return health_output.split()[0]
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
        # Avoid ``_stop`` — it shadows Thread._stop and breaks join() on Python 3.11+.
        self._stop_event = threading.Event()
        self.crash_error = None
        self.reporting_unreliable = False
        self.reporting_unreliable_reason = ""

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
                reporting = ceph_tool.verify_crash_reporting_available()
                # Still look for crashes that made it into the mon even when
                # collectors are unhealthy, unless ``ceph crash ls`` itself failed.
                if "crash ls failed" not in (reporting.reason or ""):
                    raise_if_ceph_crashes_detected(
                        ceph_tool,
                        chaos_type,
                        poll_interval=self.interval,
                    )
                outcome, message = evaluate_crash_monitor_check(
                    reporting.available,
                    reporting.reason,
                    crashes_found=False,
                )
                if outcome == CRASH_CHECK_UNRELIABLE:
                    self.reporting_unreliable = True
                    self.reporting_unreliable_reason = reporting.reason
                    log.warning("%s", message)
                    return True
                log.info(
                    "Ceph crash monitor: %s for %s; next check in %ss",
                    message,
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
        while not self._stop_event.wait(self.interval):
            if not _check_once():
                break

    def stop(self):
        self._stop_event.set()

    def raise_if_crash_detected(self):
        """Re-raise any crash detected by the background thread."""
        if self.crash_error:
            raise self.crash_error

    def final_check(self):
        """Run a final crash check after failure injection completes."""
        ceph_tool = CephStatusTool()
        reporting = ceph_tool.verify_crash_reporting_available()
        if "crash ls failed" not in (reporting.reason or ""):
            raise_if_ceph_crashes_detected(
                ceph_tool,
                f"{self.context} (final check)",
                poll_interval=0,
            )
        if not reporting.available:
            self.reporting_unreliable = True
            self.reporting_unreliable_reason = reporting.reason
            log.warning(
                "Final Ceph crash check: reporting unavailable (%s); "
                "not treating an empty crash list as a clean result",
                reporting.reason,
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
