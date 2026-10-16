"""Build reporting context from issue verification run records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from issue_gate import (
    MANUAL_VERIFICATION_FAILED,
    is_manual_verification_failed,
    summarize_live_verification,
)
from run_record import (
    STAGE_LIVE_CLUSTER_VERIFICATION,
    STAGE_REPRO_STEPS,
    STAGE_TEST_MATCHING,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _stage_data(issue: dict[str, Any], stage: str) -> dict[str, Any]:
    return issue.get("stages", {}).get(stage, {}).get("data", {}) or {}


def _stage_status(issue: dict[str, Any], stage: str) -> str:
    return issue.get("stages", {}).get(stage, {}).get("status", "—") or "—"


def _truncate(text: str, limit: int = 220) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _repro_steps_summary(issue: dict[str, Any]) -> str:
    data = _stage_data(issue, STAGE_REPRO_STEPS)
    repro = data.get("reproduction_steps") or []
    verify = data.get("verification_steps") or []
    if not repro and not verify:
        return "—"
    return f"{len(repro)} repro / {len(verify)} verify steps"


def _repro_status(issue: dict[str, Any]) -> str:
    status = _stage_status(issue, STAGE_REPRO_STEPS)
    data = _stage_data(issue, STAGE_REPRO_STEPS)
    if status == "failed" or data.get("status") == "failed":
        return "Fail"
    if status == "completed":
        return "Pass"
    if status == "skipped":
        return "Skipped"
    return status.capitalize() if status != "—" else "Pending"


def _live_repro_status(issue: dict[str, Any]) -> str:
    stage = issue.get("stages", {}).get(STAGE_LIVE_CLUSTER_VERIFICATION)
    if not stage:
        return "N/A"
    data = stage.get("data", {})
    status = stage.get("status", "")
    if status == "skipped":
        return f"Skipped ({data.get('skip_reason', '—')})"
    if data.get("dry_run"):
        return "Dry-run plan"
    verdict = data.get("verdict")
    reproduced = data.get("issue_reproduced")
    if verdict == "fixed" or reproduced == "No":
        return "Pass"
    if is_manual_verification_failed(issue):
        return "Fail"
    if verdict in ("not_fixed", "inconclusive"):
        return "Fail"
    if status == "failed":
        return "Fail"
    if status == "completed":
        return str(verdict or "Completed").capitalize()
    return status or "—"


def _test_match_summary(issue: dict[str, Any]) -> str:
    stage = issue.get("stages", {}).get(STAGE_TEST_MATCHING)
    if not stage:
        return "Pending"
    data = stage.get("data", {})
    if stage.get("status") == "skipped":
        return "Skipped"
    count = data.get("matching_test_count")
    if count is None:
        count = len(data.get("matching_tests") or [])
    if count == 0:
        return "No matches"
    tests = data.get("matching_tests") or []
    top = tests[0].get("test_node_id", "") if tests else ""
    short = top.split("/")[-1] if top else ""
    return f"{count} matched" + (f" (top: {short})" if short else "")


def _observation(issue: dict[str, Any]) -> str:
    parts: list[str] = []
    repro = _stage_data(issue, STAGE_REPRO_STEPS)
    if repro.get("error"):
        parts.append(f"Repro error: {repro['error']}")
    if repro.get("analysis_notes"):
        parts.append(str(repro["analysis_notes"]))

    live = summarize_live_verification(issue)
    if live.get("conclusion"):
        parts.append(str(live["conclusion"]))
    elif live.get("error"):
        parts.append(f"Live repro: {live['error']}")
    elif live.get("verdict"):
        parts.append(f"Live verdict: {live['verdict']}")

    match = _stage_data(issue, STAGE_TEST_MATCHING)
    if match.get("analysis_notes"):
        parts.append(str(match["analysis_notes"]))
    if match.get("error"):
        parts.append(f"Test match: {match['error']}")

    qual = issue.get("qualification_status")
    if qual == MANUAL_VERIFICATION_FAILED:
        parts.append("Blocked downstream: manual verification failed")

    if not parts:
        return "—"
    return _truncate(" | ".join(parts))


def _qualification_status(issue: dict[str, Any]) -> str:
    return issue.get("qualification_status") or "—"


def build_issue_row(issue: dict[str, Any]) -> dict[str, Any]:
    """One table row for the issue verification report."""
    key = issue.get("key", "")
    title = issue.get("summary", "")
    issue_label = f"{key}: {_truncate(title, 80)}" if key else _truncate(title, 80)
    repro_data = _stage_data(issue, STAGE_REPRO_STEPS)
    return {
        "issue_id": key,
        "title": title,
        "issue_label": issue_label,
        "topology": repro_data.get("topology_label") or repro_data.get("topology", "—"),
        "repro_steps_summary": _repro_steps_summary(issue),
        "repro_status": _repro_status(issue),
        "live_repro_status": _live_repro_status(issue),
        "test_match_summary": _test_match_summary(issue),
        "qualification_status": _qualification_status(issue),
        "observation": _observation(issue),
    }


def _build_summary(issues: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "repro_completed": 0,
        "live_passed": 0,
        "live_failed": 0,
        "live_skipped": 0,
        "test_match_completed": 0,
        "manual_verification_failed": 0,
    }
    for issue in issues:
        if _stage_status(issue, STAGE_REPRO_STEPS) == "completed":
            summary["repro_completed"] += 1
        live = _live_repro_status(issue)
        if live.startswith("Pass") or live == "Dry-run plan":
            if live == "Pass":
                summary["live_passed"] += 1
            else:
                summary["live_skipped"] += 1
        elif live.startswith("Fail"):
            summary["live_failed"] += 1
        elif live.startswith("Skipped") or live == "N/A":
            summary["live_skipped"] += 1
        if _stage_status(issue, STAGE_TEST_MATCHING) == "completed":
            summary["test_match_completed"] += 1
        if is_manual_verification_failed(issue):
            summary["manual_verification_failed"] += 1
    return summary


def build_issue_verification_report_context(
    run_record_data: dict[str, Any],
    *,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Build Jinja context for issue_verification.md.j2 from a run record.

    Args:
        run_record_data: RunRecord._data dict (or loaded JSON root)
        parameters: Optional pipeline parameters (deploy_job_url, etc.)

    """
    parameters = parameters or {}
    issues = run_record_data.get("issues") or []
    jenkins = run_record_data.get("jenkins_execution") or {}
    trigger = jenkins.get("trigger_result") or {}

    run_id = run_record_data.get("run_id", "")
    odf_version = run_record_data.get("odf_version", parameters.get("odf_version", ""))

    jenkins_status = "not run"
    if jenkins:
        jenkins_status = "dry-run" if jenkins.get("dry_run") else "triggered"

    context = {
        "workflow": "issue_verification",
        "subject": f"Issue Verification Report — {odf_version} — run {run_id}",
        "run": {
            "title": f"Issue Verification — {odf_version}",
            "run_id": run_id,
            "odf_version": odf_version,
            "issue_count": len(issues),
            "stages_completed": run_record_data.get("stages_completed") or [],
            "deploy_job_url": parameters.get("deploy_job_url")
            or jenkins.get("deploy_job_url"),
            "jenkins_status": jenkins_status,
            "generated_at": _utc_now(),
            "issues_file": run_record_data.get("issues_file"),
        },
        "summary": _build_summary(issues),
        "issues": [build_issue_row(issue) for issue in issues],
        "jenkins": (
            {
                "dry_run": jenkins.get("dry_run"),
                "test_paths": jenkins.get("test_paths") or [],
                "build_url": trigger.get("build_url") or trigger.get("job_url"),
                "excluded_issues": jenkins.get("excluded_issues") or [],
            }
            if jenkins
            else None
        ),
    }
    if jenkins:
        context["summary"]["jenkins_triggered"] = len(jenkins.get("test_paths") or [])
    return context
