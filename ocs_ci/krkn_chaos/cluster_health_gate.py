"""
Cluster health gate for Krkn chaos and resiliency tests.

Distinguishes recoverable degradation during chaos (HEALTH_WARN, a single OSD
down, MDS failover in progress, StorageCluster Progressing) from unrecoverable
failure that must skip the current test and the rest of the session:
StorageCluster phase Error,
MDS_DAMAGE, PGs inactive above a threshold, OSDs below pool min_size, or a
fully offline filesystem when chaos is not in progress.

StorageCluster phase Error is independent of ``ceph health``. Ceph can be
HEALTH_OK while the ODF StorageCluster CR is Error (Available=False). The
session gate waits up to 1 hour for that phase to leave Error before
skipping the remaining tests, because the CR often returns to Ready after
chaos.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

HEALTHY = "healthy"
DEGRADED = "degraded"
UNRECOVERABLE = "unrecoverable"

# Fraction of PGs that must be inactive before the cluster is unrecoverable.
DEFAULT_INACTIVE_PG_THRESHOLD = 0.5

ALWAYS_UNRECOVERABLE_CODES = frozenset({"MDS_DAMAGE"})
PRE_TEST_UNRECOVERABLE_CODES = frozenset({"MDS_DAMAGE", "MDS_ALL_DOWN"})
STORAGECLUSTER_ERROR_PHASES = frozenset({"error", "failed"})
# Transient Error after chaos is common; do not skip tests until this window
# has elapsed with the phase still Error or Failed.
STORAGECLUSTER_ERROR_WAIT_SECONDS = 60 * 60
STORAGECLUSTER_ERROR_POLL_SECONDS = 30
# Toolbox exec defaults to 600s. Cap the gate and stop after the first failure
# so a hung toolbox cannot run four commands back to back.
TOOLBOX_HEALTH_DEADLINE = 45
TOOLBOX_HEALTH_COMMAND_TIMEOUT = 30
_TOOLBOX_HEALTH_COMMANDS = (
    ("ceph health detail", "health_detail"),
    ("ceph pg stat", "pg_stat"),
    ("ceph osd stat", "osd_stat"),
    ("ceph osd dump", "osd_dump"),
)
# Set after the first unrecoverable decision so later tests skip immediately
# instead of waiting another hour each.
_session_unrecoverable_reason: Optional[str] = None


class UnrecoverableClusterError(Exception):
    """The Ceph cluster cannot recover; the chaos session must stop."""


@dataclass
class ClusterHealthClassification:
    status: str
    reason: str


def _health_checks(health_detail) -> Dict[str, Any]:
    if isinstance(health_detail, dict):
        checks = health_detail.get("checks") or {}
        return checks if isinstance(checks, dict) else {}
    return {}


def _health_status_token(health_detail) -> str:
    if isinstance(health_detail, dict):
        return str(health_detail.get("status") or "").split()[0]
    if health_detail is None:
        return ""
    return str(health_detail).strip().split()[0] if str(health_detail).strip() else ""


def _health_codes(health_detail) -> set:
    checks = set(_health_checks(health_detail).keys())
    if isinstance(health_detail, str):
        for code in ("MDS_DAMAGE", "MDS_ALL_DOWN", "OSD_DOWN", "PG_AVAILABILITY"):
            if code in health_detail:
                checks.add(code)
    return checks


def _inactive_pg_fraction(pg_stat) -> Optional[float]:
    if not isinstance(pg_stat, dict):
        return None
    num_pg = pg_stat.get("num_pg") or pg_stat.get("num_pgs")
    if not num_pg:
        return None
    if "num_inactive" in pg_stat:
        return pg_stat["num_inactive"] / num_pg
    if "num_pg_active" in pg_stat:
        return max(0.0, 1.0 - (pg_stat["num_pg_active"] / num_pg))
    return None


def _min_pool_min_size(osd_dump) -> Optional[int]:
    if not isinstance(osd_dump, dict):
        return None
    pools = osd_dump.get("pools") or []
    min_sizes = [
        int(pool["min_size"])
        for pool in pools
        if isinstance(pool, dict) and pool.get("min_size") is not None
    ]
    return min(min_sizes) if min_sizes else None


def _num_up_osds(osd_stat) -> Optional[int]:
    if not isinstance(osd_stat, dict):
        return None
    if "num_up_osds" in osd_stat:
        return int(osd_stat["num_up_osds"])
    if "num_up_osd" in osd_stat:
        return int(osd_stat["num_up_osd"])
    return None


def get_storagecluster_phase(namespace=None) -> Optional[str]:
    """
    Return StorageCluster ``status.phase``, or None if it cannot be read.

    Does not require the Ceph toolbox. Lists StorageCluster CRs so this works
    even when ``storage_cluster_name`` is missing from config.
    """
    from ocs_ci.framework import config
    from ocs_ci.ocs import constants
    from ocs_ci.ocs.ocp import OCP

    namespace = namespace or config.ENV_DATA.get("cluster_namespace")
    namespace = namespace or constants.OPENSHIFT_STORAGE_NAMESPACE
    sc = OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    items = sc.get().get("items") or []
    if not items:
        log.warning("No StorageCluster found in namespace %s", namespace)
        return None
    return (items[0].get("status") or {}).get("phase")


def classify_cluster_health(
    health_detail,
    pg_stat=None,
    osd_stat=None,
    osd_dump=None,
    chaos_in_progress=False,
    inactive_pg_threshold=DEFAULT_INACTIVE_PG_THRESHOLD,
    storagecluster_phase=None,
) -> ClusterHealthClassification:
    """
    Classify cluster health as healthy, degraded, or unrecoverable.

    Args:
        health_detail: ``ceph health detail`` dict or string
        pg_stat: ``ceph pg stat`` dict
        osd_stat: ``ceph osd stat`` dict
        osd_dump: ``ceph osd dump`` dict (for pool min_size)
        chaos_in_progress: When True, MDS_ALL_DOWN alone is degraded (failover)
        inactive_pg_threshold: Inactive PG fraction that is unrecoverable
        storagecluster_phase: StorageCluster ``status.phase``. A single reading
            of Error is unrecoverable. The session gate waits 1 hour for
            recovery before skipping the remaining tests.

    Returns:
        ClusterHealthClassification
    """
    if storagecluster_phase and str(storagecluster_phase).lower() in (
        STORAGECLUSTER_ERROR_PHASES
    ):
        return ClusterHealthClassification(
            UNRECOVERABLE,
            f"StorageCluster phase is {storagecluster_phase} "
            "(Ceph HEALTH_OK/WARN is not sufficient)",
        )

    codes = _health_codes(health_detail)
    token = _health_status_token(health_detail)

    fatal_codes = ALWAYS_UNRECOVERABLE_CODES
    if not chaos_in_progress:
        fatal_codes = PRE_TEST_UNRECOVERABLE_CODES
    hit = codes & fatal_codes
    if hit:
        return ClusterHealthClassification(
            UNRECOVERABLE,
            f"Unrecoverable Ceph health check(s): {', '.join(sorted(hit))}",
        )

    inactive_frac = _inactive_pg_fraction(pg_stat)
    if inactive_frac is not None and inactive_frac >= inactive_pg_threshold:
        return ClusterHealthClassification(
            UNRECOVERABLE,
            f"PGs inactive fraction {inactive_frac:.2f} exceeds threshold "
            f"{inactive_pg_threshold:.2f}",
        )

    up_osds = _num_up_osds(osd_stat)
    min_size = _min_pool_min_size(osd_dump)
    if up_osds is not None and min_size is not None and up_osds < min_size:
        return ClusterHealthClassification(
            UNRECOVERABLE,
            f"OSDs up ({up_osds}) below pool min_size ({min_size})",
        )

    if token in ("HEALTH_WARN", "HEALTH_ERR") or codes:
        reason = token or ", ".join(sorted(codes)) or "degraded"
        return ClusterHealthClassification(DEGRADED, reason)

    return ClusterHealthClassification(HEALTHY, token or "HEALTH_OK")


def raise_if_cluster_unrecoverable(
    health_detail,
    pg_stat=None,
    osd_stat=None,
    osd_dump=None,
    chaos_in_progress=False,
    inactive_pg_threshold=DEFAULT_INACTIVE_PG_THRESHOLD,
    storagecluster_phase=None,
):
    """Raise UnrecoverableClusterError when the cluster cannot recover."""
    result = classify_cluster_health(
        health_detail,
        pg_stat=pg_stat,
        osd_stat=osd_stat,
        osd_dump=osd_dump,
        chaos_in_progress=chaos_in_progress,
        inactive_pg_threshold=inactive_pg_threshold,
        storagecluster_phase=storagecluster_phase,
    )
    if result.status == UNRECOVERABLE:
        log.error("Cluster is unrecoverable: %s", result.reason)
        raise UnrecoverableClusterError(result.reason)
    if result.status == DEGRADED:
        log.warning("Cluster is degraded (chaos may continue): %s", result.reason)
    return result


def evaluate_cluster_health_from_toolbox(ct_pod=None, chaos_in_progress=False):
    """
    Classify ODF/Ceph health from StorageCluster phase and optional toolbox cmds.

    StorageCluster phase is queried even when the toolbox pod is unavailable.
    ``ct_pod`` may be None.

    Returns:
        ClusterHealthClassification
    """
    storagecluster_phase = None
    try:
        storagecluster_phase = get_storagecluster_phase()
        log.info("StorageCluster phase: %s", storagecluster_phase)
    except Exception as ex:
        log.warning("Could not get StorageCluster phase for health gate: %s", ex)

    health_detail = None
    pg_stat = None
    osd_stat = None
    osd_dump = None
    if ct_pod is not None:
        collected = _toolbox_health_commands(ct_pod)
        health_detail = collected.get("health_detail")
        pg_stat = collected.get("pg_stat")
        osd_stat = collected.get("osd_stat")
        osd_dump = collected.get("osd_dump")
    return classify_cluster_health(
        health_detail,
        pg_stat=pg_stat,
        osd_stat=osd_stat,
        osd_dump=osd_dump,
        chaos_in_progress=chaos_in_progress,
        storagecluster_phase=storagecluster_phase,
    )


def _toolbox_health_commands(ct_pod):
    """
    Run Ceph toolbox reads until one fails or the gate deadline is reached.

    Returns:
        dict: Command key to parsed output for the commands that succeeded.
    """
    deadline = time.monotonic() + TOOLBOX_HEALTH_DEADLINE
    collected = {}
    for ceph_cmd, key in _TOOLBOX_HEALTH_COMMANDS:
        remaining = int(deadline - time.monotonic())
        if remaining <= 0:
            log.warning("Health gate toolbox deadline reached; skipping %s", ceph_cmd)
            break
        try:
            collected[key] = ct_pod.exec_ceph_cmd(
                ceph_cmd,
                timeout=min(TOOLBOX_HEALTH_COMMAND_TIMEOUT, remaining),
            )
        except Exception as ex:
            log.warning(
                "Could not get %s for health gate: %s; "
                "not running further toolbox commands",
                ceph_cmd,
                ex,
            )
            break
    return collected


def evaluate_odf_cluster_health(chaos_in_progress=False):
    """
    Classify ODF health using StorageCluster phase and Ceph toolbox if present.

    Toolbox failure does not skip the StorageCluster check.
    """
    from ocs_ci.ocs.resources import pod as pod_helpers

    ct_pod = None
    try:
        ct_pod = pod_helpers.get_ceph_tools_pod()
    except Exception as ex:
        log.warning("Could not get ceph tools pod for health gate: %s", ex)
    return evaluate_cluster_health_from_toolbox(
        ct_pod, chaos_in_progress=chaos_in_progress
    )


def storagecluster_phase_is_error(phase) -> bool:
    """Return True when StorageCluster phase is Error or Failed."""
    return bool(phase) and str(phase).lower() in STORAGECLUSTER_ERROR_PHASES


def _classification_is_storagecluster_error(result) -> bool:
    return result.status == UNRECOVERABLE and str(result.reason).startswith(
        "StorageCluster phase is "
    )


def _wait_for_storagecluster_phase_recovery(context):
    """
    Poll until StorageCluster leaves Error/Failed, or the wait window ends.

    Returns:
        ClusterHealthClassification: Evaluation after recovery or timeout.
    """
    timeout_seconds = STORAGECLUSTER_ERROR_WAIT_SECONDS
    poll_seconds = STORAGECLUSTER_ERROR_POLL_SECONDS
    deadline = time.monotonic() + timeout_seconds
    phase = None
    while True:
        try:
            phase = get_storagecluster_phase()
        except Exception as ex:
            log.warning(
                "%s: could not read StorageCluster phase while waiting: %s",
                context,
                ex,
            )
            phase = None
        if phase is not None and not storagecluster_phase_is_error(phase):
            log.info(
                "%s: StorageCluster phase recovered to %s",
                context,
                phase,
            )
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            log.error(
                "%s: StorageCluster phase still %s after %ss",
                context,
                phase,
                timeout_seconds,
            )
            break
        log.warning(
            "%s: StorageCluster phase is %s; waiting %.0fs more "
            "(up to %ss) before skipping the remaining tests",
            context,
            phase,
            remaining,
            timeout_seconds,
        )
        time.sleep(min(poll_seconds, remaining))
    return evaluate_odf_cluster_health(chaos_in_progress=False)


def skip_test_if_cluster_unrecoverable(context):
    """
    Skip this test when the cluster is unrecoverable, before chaos starts.

    The first unrecoverable decision is remembered for the pytest process.
    Later tests skip immediately with the same reason, so JUnit records each
    skip and the StorageCluster wait does not repeat.

    StorageCluster phase Error or Failed waits up to 1 hour for recovery
    before that decision. Other unrecoverable Ceph conditions skip
    immediately. Degraded HEALTH_WARN is logged and the test runs.
    Evaluation failures are logged and do not skip the test.

    Args:
        context (str): Lifecycle label used in log lines, for example
            ``Krkn chaos`` or ``Resiliency``.
    """
    import pytest

    global _session_unrecoverable_reason
    if _session_unrecoverable_reason:
        log.error(_session_unrecoverable_reason)
        pytest.skip(_session_unrecoverable_reason)

    try:
        result = evaluate_odf_cluster_health(chaos_in_progress=False)
    except Exception as ex:
        log.warning(
            "%s test lifecycle: could not evaluate cluster health gate: %s",
            context,
            ex,
        )
        return

    if _classification_is_storagecluster_error(result):
        log.warning(
            "%s test lifecycle: StorageCluster is in an error phase; "
            "waiting up to %ss for recovery before skipping tests",
            context,
            STORAGECLUSTER_ERROR_WAIT_SECONDS,
        )
        try:
            result = _wait_for_storagecluster_phase_recovery(context)
        except Exception as ex:
            log.warning(
                "%s test lifecycle: could not re-evaluate cluster health "
                "after waiting for StorageCluster: %s",
                context,
                ex,
            )
            return

    if result.status == UNRECOVERABLE:
        _session_unrecoverable_reason = (
            f"Skipping test: cluster is unrecoverable: {result.reason}"
        )
        log.error(_session_unrecoverable_reason)
        pytest.skip(_session_unrecoverable_reason)
    if result.status != HEALTHY:
        log.warning(
            "%s test lifecycle: cluster is degraded but recoverable: %s",
            context,
            result.reason,
        )
