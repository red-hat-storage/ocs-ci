"""Z-stream workflow stage executors."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any

_ZSTREAM_DIR = Path(__file__).resolve().parent
_WORKFLOW_DIR = _ZSTREAM_DIR.parent
_CLAUDE_DIR = _WORKFLOW_DIR.parent
_REPO_ROOT = _CLAUDE_DIR.parent
_AGENTS_DIR = _CLAUDE_DIR / "agents"
_OCS_CI_JIRA_DIR = _AGENTS_DIR / "ocs_ci_jira"
_OCS_CI_TEST_MATCH_DIR = _AGENTS_DIR / "ocs_ci_test_match"
_OCS_CI_RUN_DIR = _AGENTS_DIR / "ocs_ci_run"

for _path in (_ZSTREAM_DIR, _WORKFLOW_DIR, _CLAUDE_DIR, _REPO_ROOT, _OCS_CI_JIRA_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from repro_steps_generator import run_repro_steps_stage
from run_record import (
    STAGE_OCS_CI_EXECUTION,
    STAGE_REPRO_STEPS,
    STAGE_TEST_MATCHING,
)
from workflow_lib.import_helpers import load_agent_module
from workflow_context import ZstreamRunContext

log = logging.getLogger(__name__)


def _run_record(context: Any) -> Any:
    if not isinstance(context, ZstreamRunContext):
        raise TypeError("Z-stream executors require ZstreamRunContext")
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
    context: ZstreamRunContext,
) -> dict[str, Any]:
    """Stage 1: fetch JIRA issues via ocs_ci_jira and initialize run record."""
    from operations import search_by_params

    if not parameters.get("odf_version"):
        raise ValueError("jira_search requires odf_version")

    run_record = _run_record(context)
    details, jql = search_by_params(
        parameters,
        jira_config=parameters.get("jira_config"),
    )
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
    }


def run_repro_steps(
    parameters: dict[str, Any],
    context: ZstreamRunContext,
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
    )
    run_record.append_stage_bulk(STAGE_REPRO_STEPS, per_issue)
    return {
        "issues": run_record.get_issues(),
        "issues_file": str(run_record.issues_file),
        "issue_count": len(per_issue),
    }


def run_test_matching(
    parameters: dict[str, Any],
    context: ZstreamRunContext,
) -> dict[str, Any]:
    """Stage 3: find matching ocs-ci tests."""
    test_match_ops = load_agent_module(
        _OCS_CI_TEST_MATCH_DIR,
        "operations.py",
        "ocs_ci_test_match_operations",
    )
    run_record = _run_record(context)
    issues = parameters.get("issues") or run_record.get_issues()
    if not issues:
        raise ValueError("test_matching requires a non-empty issues list")

    per_issue = test_match_ops.match_issues(
        issues,
        top_n=int(parameters.get("top_n", 10)),
        use_claude=bool(parameters.get("use_claude", False)),
        model=parameters.get("claude_model"),
    )
    run_record.append_stage_bulk(STAGE_TEST_MATCHING, per_issue)
    return {
        "issues": run_record.get_issues(),
        "issues_file": str(run_record.issues_file),
        "issue_count": len(per_issue),
    }


def run_ocs_ci_execution(
    parameters: dict[str, Any],
    context: ZstreamRunContext,
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

    issues = parameters.get("issues") or run_record.get_issues()
    test_paths = _extract_test_paths(
        issues,
        tests_per_issue=int(parameters.get("tests_per_issue", 1)),
    )
    if not test_paths:
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


AGENT_EXECUTORS = {
    "jira_search": run_jira_search,
    "repro_steps": run_repro_steps,
    "test_matching": run_test_matching,
    "ocs_ci_execution": run_ocs_ci_execution,
}
