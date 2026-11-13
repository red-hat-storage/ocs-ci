"""Per-issue pipeline gating after live manual verification."""

from __future__ import annotations

from typing import Any

from run_record import STAGE_LIVE_CLUSTER_VERIFICATION, STAGE_TEST_MATCHING

MANUAL_VERIFICATION_FAILED = "manual_verification_failed"
MANUAL_VERIFICATION_PASSED = "manual_verification_passed"
SKIP_MANUAL_VERIFICATION_FAILED = "manual_verification_failed"


def _live_verification_stage(issue: dict[str, Any]) -> dict[str, Any] | None:
    return issue.get("stages", {}).get(STAGE_LIVE_CLUSTER_VERIFICATION)


def live_verification_was_attempted(issue: dict[str, Any]) -> bool:
    """True when stage 3 ran in live mode (not dry-run plan only)."""
    stage = _live_verification_stage(issue)
    if not stage:
        return False
    return not stage.get("data", {}).get("dry_run", True)


def is_manual_verification_failed(issue: dict[str, Any]) -> bool:
    """
    Return True when live manual verification failed for this issue.

    Dry-run plans and skipped live repro do not block downstream stages.
    """
    if not live_verification_was_attempted(issue):
        return False

    stage = _live_verification_stage(issue)
    if not stage:
        return False

    stage_status = stage.get("status")
    data = stage.get("data", {})

    if stage_status == "failed":
        return True

    verdict = data.get("verdict")
    if verdict in ("not_fixed", "inconclusive"):
        return True

    if str(data.get("issue_reproduced", "")).strip() == "Yes":
        return True

    return False


def is_eligible_for_downstream_stages(issue: dict[str, Any]) -> bool:
    """Issues that may proceed to test_matching and ocs_ci_execution."""
    return not is_manual_verification_failed(issue)


def partition_issues_for_downstream(
    issues: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split issues into (eligible, blocked) lists."""
    eligible: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for issue in issues:
        if is_manual_verification_failed(issue):
            blocked.append(issue)
        else:
            eligible.append(issue)
    return eligible, blocked


def summarize_live_verification(issue: dict[str, Any]) -> dict[str, Any]:
    """Compact summary of live repro stage for skip reports."""
    stage = _live_verification_stage(issue) or {}
    data = stage.get("data", {})
    return {
        "stage_status": stage.get("status"),
        "verdict": data.get("verdict"),
        "issue_reproduced": data.get("issue_reproduced"),
        "conclusion": data.get("conclusion"),
        "skip_reason": data.get("skip_reason"),
        "error": data.get("error"),
    }


def build_test_matching_skip_payload(issue: dict[str, Any]) -> dict[str, Any]:
    """Stage data when test_matching is skipped due to failed manual verification."""
    key = issue.get("key", "")
    live_summary = summarize_live_verification(issue)
    return {
        "issue_id": key,
        "issue_summary": issue.get("summary", ""),
        "skip_reason": SKIP_MANUAL_VERIFICATION_FAILED,
        "qualification_status": MANUAL_VERIFICATION_FAILED,
        "matching_test_count": 0,
        "matching_tests": [],
        "analysis_notes": (
            f"Skipped test matching for {key}: manual live verification failed. "
            f"Verdict={live_summary.get('verdict')!r}, "
            f"issue_reproduced={live_summary.get('issue_reproduced')!r}."
        ),
        "live_verification_summary": live_summary,
    }


def issue_already_skipped_test_matching(issue: dict[str, Any]) -> bool:
    """True if test_matching was already recorded as skipped for manual repro failure."""
    stage = issue.get("stages", {}).get(STAGE_TEST_MATCHING)
    if not stage:
        return False
    return stage.get("data", {}).get("skip_reason") == SKIP_MANUAL_VERIFICATION_FAILED
