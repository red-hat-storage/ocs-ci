"""Issue verification workflow stage executors."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any

_ISSUE_VERIFICATION_DIR = Path(__file__).resolve().parent
_WORKFLOW_DIR = _ISSUE_VERIFICATION_DIR.parent
_CLAUDE_DIR = _WORKFLOW_DIR.parent
_REPO_ROOT = _CLAUDE_DIR.parent
_AGENTS_DIR = _CLAUDE_DIR / "agents"
_OCS_CI_JIRA_DIR = _AGENTS_DIR / "ocs_ci_jira"
_OCS_CI_TEST_MATCH_DIR = _AGENTS_DIR / "ocs_ci_test_match"
_OCS_CI_REPORTING_DIR = _AGENTS_DIR / "ocs_ci_reporting"
_OCS_CI_RUN_DIR = _AGENTS_DIR / "ocs_ci_run"

_OCS_CI_LIVE_REPRO_DIR = _AGENTS_DIR / "ocs_ci_live_repro"

for _path in (
    _ISSUE_VERIFICATION_DIR,
    _WORKFLOW_DIR,
    _CLAUDE_DIR,
    _REPO_ROOT,
):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from issue_gate import (
    MANUAL_VERIFICATION_FAILED,
    MANUAL_VERIFICATION_PASSED,
    build_test_matching_skip_payload,
    is_manual_verification_failed,
    partition_issues_for_downstream,
    summarize_live_verification,
)
from repro_steps_generator import run_repro_steps_stage
from run_record import (
    STAGE_LIVE_CLUSTER_VERIFICATION,
    STAGE_OCS_CI_EXECUTION,
    STAGE_REPRO_STEPS,
    STAGE_REPORTING,
    STAGE_TEST_MATCHING,
)
from workflow_lib.import_helpers import load_agent_module
from workflow_context import IssueVerificationRunContext

log = logging.getLogger(__name__)


def _run_record(context: Any) -> Any:
    if not isinstance(context, IssueVerificationRunContext):
        raise TypeError(
            "Issue verification executors require IssueVerificationRunContext"
        )
    return context.run_record


def _extract_test_paths(
    issues: list[dict[str, Any]],
    *,
    tests_per_issue: int = 1,
) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for issue in issues:
        stage_data = (
            issue.get("stages", {}).get(STAGE_TEST_MATCHING, {}).get("data", {})
        )
        for test in stage_data.get("matching_tests", [])[:tests_per_issue]:
            node_id = test.get("test_node_id", "")
            if not node_id:
                command = test.get("pytest_command", "")
                if command.startswith("pytest "):
                    node_id = command[len("pytest ") :].strip()
            if node_id and node_id not in seen:
                seen.add(node_id)
                paths.append(node_id)
    return paths


def run_jira_search(
    parameters: dict[str, Any],
    context: IssueVerificationRunContext,
) -> dict[str, Any]:
    """Stage 1: fetch JIRA issues (explicit list or ON_QA search)."""
    if not parameters.get("odf_version"):
        raise ValueError("jira_search requires odf_version")

    run_record = _run_record(context)
    jira_config = parameters.get("jira_config")
    issue_keys = parameters.get("issues")

    jira_ops = load_agent_module(
        _OCS_CI_JIRA_DIR,
        "operations.py",
        "ocs_ci_jira_operations",
    )

    if issue_keys:
        details, jql = jira_ops.get_issues_by_keys(issue_keys, jira_config=jira_config)
        intake_mode = "explicit_issues"
    else:
        details, jql = jira_ops.search_by_params(
            parameters,
            jira_config=jira_config,
        )
        intake_mode = "jql_search"

    run_record.init_jira_intake(
        details,
        jql=jql,
        odf_version=parameters["odf_version"],
    )
    issues = run_record.get_issues()
    return {
        "issues": issues,
        "issues_file": str(run_record.issues_file),
        "issue_count": len(issues),
        "jql": jql,
        "intake_mode": intake_mode,
    }


def run_repro_steps(
    parameters: dict[str, Any],
    context: IssueVerificationRunContext,
) -> dict[str, Any]:
    """Stage 2: generate reproduction and verification steps."""
    run_record = _run_record(context)
    issues = parameters.get("issues") or run_record.get_issues()
    if not issues:
        raise ValueError("repro_steps requires a non-empty issues list")

    odf_version = parameters.get("odf_version") or run_record._data.get("odf_version")
    if not odf_version:
        raise ValueError("repro_steps requires odf_version")

    per_issue = run_repro_steps_stage(
        issues,
        odf_version,
        jira_config=parameters.get("jira_config"),
        refresh_jira=bool(parameters.get("refresh_jira", True)),
        include_fix_prs=bool(parameters.get("include_fix_prs", True)),
        claude_model=parameters.get("claude_model"),
        backend=parameters.get("backend") or "auto",
        max_turns=int(parameters.get("max_turns", 20)),
    )
    run_record.append_stage_bulk(STAGE_REPRO_STEPS, per_issue)
    return {
        "issues": run_record.get_issues(),
        "issues_file": str(run_record.issues_file),
        "issue_count": len(per_issue),
    }


def run_live_cluster_verification(
    parameters: dict[str, Any],
    context: IssueVerificationRunContext,
) -> dict[str, Any]:
    """Stage 3: plan live cluster verification (Phase A dry-run)."""
    live_repro_ops = load_agent_module(
        _OCS_CI_LIVE_REPRO_DIR,
        "operations.py",
        "ocs_ci_live_repro_operations",
    )
    run_record = _run_record(context)
    issues = parameters.get("issues") or run_record.get_issues()
    if not issues:
        raise ValueError("live_cluster_verification requires a non-empty issues list")

    deploy_job_url = parameters.get("deploy_job_url")
    if not deploy_job_url:
        raise ValueError("live_cluster_verification requires deploy_job_url")

    per_issue = live_repro_ops.verify_issues(
        issues,
        deploy_job_url=deploy_job_url,
        target_zstream=parameters.get("odf_version")
        or run_record._data.get("odf_version"),
        dry_run=bool(parameters.get("dry_run", True)),
        skip_on_env_mismatch=bool(parameters.get("skip_on_env_mismatch", True)),
        force=bool(parameters.get("force", False)),
        oc_command_path=parameters.get("oc_command_path") or "oc",
        model=parameters.get("claude_model"),
        max_turns=int(parameters.get("max_turns", 40)),
        backend=parameters.get("backend") or "auto",
    )

    for issue_key, data in per_issue.items():
        payload = dict(data)
        stage_status = payload.pop("stage_status", "completed")
        run_record.append_stage(
            STAGE_LIVE_CLUSTER_VERIFICATION,
            issue_key,
            payload,
            status=stage_status,
        )
        issue = run_record.get_issue(issue_key)
        if issue is None:
            continue
        if is_manual_verification_failed(issue):
            issue["qualification_status"] = MANUAL_VERIFICATION_FAILED
        elif not payload.get("dry_run") and stage_status == "completed":
            verdict = payload.get("verdict")
            if verdict == "fixed" or payload.get("issue_reproduced") == "No":
                issue["qualification_status"] = MANUAL_VERIFICATION_PASSED
    run_record.save()
    run_record.mark_stage_completed(STAGE_LIVE_CLUSTER_VERIFICATION)

    failed_keys = [
        key
        for key in per_issue
        if is_manual_verification_failed(run_record.get_issue(key) or {})
    ]

    return {
        "issues": run_record.get_issues(),
        "issues_file": str(run_record.issues_file),
        "issue_count": len(per_issue),
        "deploy_job_url": deploy_job_url,
        "dry_run": bool(parameters.get("dry_run", True)),
        "manual_verification_failed_count": len(failed_keys),
        "manual_verification_failed_issues": failed_keys,
    }


def run_test_matching(
    parameters: dict[str, Any],
    context: IssueVerificationRunContext,
) -> dict[str, Any]:
    """Stage 4: find matching ocs-ci tests (skips issues that failed manual verification)."""
    test_match_ops = load_agent_module(
        _OCS_CI_TEST_MATCH_DIR,
        "operations.py",
        "ocs_ci_test_match_operations",
    )
    run_record = _run_record(context)
    issues = run_record.get_issues() or parameters.get("issues") or []
    if not issues:
        raise ValueError("test_matching requires a non-empty issues list")

    eligible, blocked = partition_issues_for_downstream(issues)

    for issue in blocked:
        key = issue.get("key")
        if not key:
            continue
        run_record.append_stage(
            STAGE_TEST_MATCHING,
            key,
            build_test_matching_skip_payload(issue),
            status="skipped",
        )
        blocked_issue = run_record.get_issue(key)
        if blocked_issue is not None:
            blocked_issue["qualification_status"] = MANUAL_VERIFICATION_FAILED

    matched_count = 0
    if eligible:
        per_issue = test_match_ops.match_issues(
            eligible,
            top_n=int(parameters.get("top_n", 10)),
            use_claude=bool(parameters.get("use_claude", False)),
            backend=parameters.get("backend")
            or parameters.get("test_match_backend")
            or "auto",
            model=parameters.get("claude_model"),
        )
        run_record.append_stage_bulk(STAGE_TEST_MATCHING, per_issue)
        matched_count = len(per_issue)

    if blocked:
        run_record.save()

    if blocked and not eligible:
        run_record.mark_stage_completed(STAGE_TEST_MATCHING)

    return {
        "issues": run_record.get_issues(),
        "issues_file": str(run_record.issues_file),
        "issue_count": matched_count,
        "skipped_manual_verification_failed": len(blocked),
        "skipped_issue_keys": [i.get("key") for i in blocked if i.get("key")],
    }


def run_ocs_ci_execution(
    parameters: dict[str, Any],
    context: IssueVerificationRunContext,
) -> dict[str, Any]:
    """Stage 4: trigger Jenkins test runs."""
    job_trigger = load_agent_module(
        _OCS_CI_RUN_DIR,
        "job_trigger.py",
        "ocs_ci_run_job_trigger",
    )
    run_record = _run_record(context)
    deploy_job_url = parameters.get("deploy_job_url")
    if not deploy_job_url:
        raise ValueError("ocs_ci_execution requires deploy_job_url")

    issues = run_record.get_issues() or parameters.get("issues") or []
    eligible, blocked = partition_issues_for_downstream(issues)
    test_paths = _extract_test_paths(
        eligible,
        tests_per_issue=int(parameters.get("tests_per_issue", 1)),
    )
    if not test_paths:
        blocked_keys = [i.get("key") for i in blocked if i.get("key")]
        if blocked_keys:
            raise ValueError(
                "No matched tests for Jenkins execution: all issues failed manual "
                f"verification ({', '.join(blocked_keys)})"
            )
        raise ValueError("No matched tests found for Jenkins execution")

    dry_run = bool(parameters.get("dry_run", True))
    result = job_trigger.trigger_test_run(
        deploy_job_url,
        test_paths,
        test_name_expression=parameters.get("test_name_expression") or "",
        run_teardown=bool(parameters.get("run_teardown", False)),
        additional_pytest_params=parameters.get("additional_pytest_params") or "",
        dry_run=dry_run,
    )

    jenkins_file = run_record.run_dir / f"{run_record.run_id}_jenkins.json"
    payload = {
        "run_id": run_record.run_id,
        "deploy_job_url": deploy_job_url,
        "test_paths": test_paths,
        "dry_run": dry_run,
        "trigger_result": result.to_dict(),
        "excluded_issues": [
            {
                "issue_key": issue.get("key"),
                "qualification_status": MANUAL_VERIFICATION_FAILED,
                "reason": "manual_verification_failed",
                "live_verification_summary": summarize_live_verification(issue),
            }
            for issue in blocked
            if issue.get("key")
        ],
    }
    jenkins_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    run_record._data["jenkins_execution"] = payload
    if STAGE_OCS_CI_EXECUTION not in run_record._data.setdefault(
        "stages_completed", []
    ):
        run_record._data["stages_completed"].append(STAGE_OCS_CI_EXECUTION)
    run_record.save()

    return {
        "jenkins_results": payload,
        "jenkins_file": str(jenkins_file),
        "test_paths": test_paths,
        "dry_run": dry_run,
    }


def run_reporting(
    parameters: dict[str, Any],
    context: IssueVerificationRunContext,
) -> dict[str, Any]:
    """Stage 6: build and deliver comprehensive run report."""
    reporting_ops = load_agent_module(
        _OCS_CI_REPORTING_DIR,
        "operations.py",
        "ocs_ci_reporting_operations",
    )
    run_record = _run_record(context)
    from report_context import build_issue_verification_report_context

    report_context = build_issue_verification_report_context(
        run_record._data,
        parameters=parameters,
    )

    template = parameters.get("template") or "issue_verification.md.j2"
    report_format = parameters.get("format") or "markdown"
    subject = parameters.get("subject")
    channels = parameters.get("channels") or [{"type": "file"}]
    dry_run = bool(parameters.get("dry_run", True))
    auth_file = parameters.get("auth_file")

    delivery = reporting_ops.build_and_deliver(
        report_context,
        template=template,
        channels=channels,
        report_format=report_format,
        subject=subject,
        output_dir=str(run_record.run_dir),
        dry_run=dry_run,
        auth_path=auth_file,
    )

    from issue_summary import write_issue_summaries

    summary_result = write_issue_summaries(
        run_record._data,
        run_record.run_dir,
        parameters=parameters,
    )

    report_file = None
    for channel in delivery.channels:
        if channel.artifact_path:
            report_file = channel.artifact_path
            break

    stage_data = {
        "template": template,
        "format": report_format,
        "subject": delivery.report.subject,
        "dry_run": dry_run,
        "channels": [
            {
                "type": c.channel_type,
                "status": c.status,
                "detail": c.detail,
                "artifact_path": c.artifact_path,
            }
            for c in delivery.channels
        ],
        "report_file": report_file,
        "succeeded": delivery.succeeded,
        "issue_summaries": summary_result,
    }

    run_record._data["reporting"] = stage_data
    if STAGE_REPORTING not in run_record._data.setdefault("stages_completed", []):
        run_record._data["stages_completed"].append(STAGE_REPORTING)
    run_record.save()

    return {
        "reporting": stage_data,
        "report_file": report_file,
        "issue_summaries": summary_result,
        "issues_file": str(run_record.issues_file),
        "succeeded": delivery.succeeded,
    }


AGENT_EXECUTORS = {
    "jira_search": run_jira_search,
    "repro_steps": run_repro_steps,
    "live_repro": run_live_cluster_verification,
    "test_matching": run_test_matching,
    "ocs_ci_execution": run_ocs_ci_execution,
    "reporting": run_reporting,
}
