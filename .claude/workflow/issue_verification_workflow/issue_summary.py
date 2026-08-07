"""Generate per-issue AI summary files at the end of issue verification runs."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

_WORKFLOW_DIR = Path(__file__).resolve().parents[1]
if str(_WORKFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_DIR))

from issue_gate import summarize_live_verification
from run_record import (
    STAGE_LIVE_CLUSTER_VERIFICATION,
    STAGE_REPRO_STEPS,
    STAGE_TEST_MATCHING,
)
from workflow_lib.claude_session import extend_claude_cli_cmd, resolve_issue_session

log = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
_REPO_ROOT = Path(__file__).resolve().parents[3]

_CLAUDE_ENV_VARS = (
    "CLAUDECODE",
    "CLAUDE_CODE",
    "CLAUDE_CODE_ENTRYPOINT",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_VERTEX_PROJECT_ID",
    "CLOUD_ML_REGION",
)


def _stage_data(issue: dict[str, Any], stage: str) -> dict[str, Any]:
    return issue.get("stages", {}).get(stage, {}).get("data", {}) or {}


def _truncate(text: str, limit: int = 1200) -> str:
    text = " ".join(str(text or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _numbered_lines(items: list[str]) -> list[str]:
    return [f"{index}. {step}" for index, step in enumerate(items, start=1)]


def _issue_test_paths(issue: dict[str, Any], tests_per_issue: int = 1) -> list[str]:
    match_data = _stage_data(issue, STAGE_TEST_MATCHING)
    paths: list[str] = []
    for test in (match_data.get("matching_tests") or [])[:tests_per_issue]:
        node_id = test.get("test_node_id", "")
        if not node_id:
            command = test.get("pytest_command", "")
            if str(command).startswith("pytest "):
                node_id = command[len("pytest ") :].strip()
        if node_id:
            paths.append(node_id)
    return paths


def build_issue_summary_facts(
    issue: dict[str, Any],
    *,
    jenkins_execution: dict[str, Any] | None = None,
    odf_version: str = "",
    tests_per_issue: int = 1,
) -> dict[str, Any]:
    """Structured facts for template or Claude summary generation."""
    key = issue.get("key", "")
    repro_data = _stage_data(issue, STAGE_REPRO_STEPS)
    live_data = _stage_data(issue, STAGE_LIVE_CLUSTER_VERIFICATION)
    match_data = _stage_data(issue, STAGE_TEST_MATCHING)
    live_summary = summarize_live_verification(issue)

    jenkins = jenkins_execution or {}
    trigger = jenkins.get("trigger_result") or {}
    issue_paths = _issue_test_paths(issue, tests_per_issue=tests_per_issue)
    jenkins_paths = jenkins.get("test_paths") or []
    triggered_paths = [path for path in issue_paths if path in jenkins_paths]

    matching_tests = []
    for test in (match_data.get("matching_tests") or [])[:5]:
        matching_tests.append(
            {
                "test_node_id": test.get("test_node_id"),
                "relevance_score": test.get("relevance_score"),
                "match_reasons": (test.get("match_reasons") or [])[:3],
                "pytest_command": test.get("pytest_command"),
                "coverage_summary": _truncate(test.get("coverage_summary") or "", 400),
            }
        )

    return {
        "issue_key": key,
        "summary": issue.get("summary", ""),
        "description_excerpt": _truncate(issue.get("description") or "", 2000),
        "odf_version": odf_version,
        "jira_status": issue.get("status"),
        "components": issue.get("components") or [],
        "qualification_status": issue.get("qualification_status"),
        "reproduction_steps": repro_data.get("reproduction_steps") or [],
        "verification_steps": repro_data.get("verification_steps") or [],
        "expected_result": repro_data.get("expected_result"),
        "repro_analysis_notes": repro_data.get("analysis_notes"),
        "topology": repro_data.get("topology_label") or repro_data.get("topology"),
        "live_verification": {
            "stage_status": issue.get("stages", {})
            .get(STAGE_LIVE_CLUSTER_VERIFICATION, {})
            .get("status"),
            "verdict": live_data.get("verdict"),
            "dry_run": live_data.get("dry_run"),
            "issue_reproduced": live_data.get("issue_reproduced"),
            "cluster_name": (live_data.get("cluster_profile") or {}).get(
                "cluster_name"
            ),
            "conclusion": live_summary.get("conclusion"),
            "error": live_summary.get("error"),
            "analysis_notes": live_data.get("analysis_notes"),
        },
        "matching_tests": matching_tests,
        "matching_test_count": match_data.get("matching_test_count")
        or len(match_data.get("matching_tests") or []),
        "test_execution": {
            "jenkins_dry_run": jenkins.get("dry_run"),
            "deploy_job_url": jenkins.get("deploy_job_url"),
            "issue_test_paths": issue_paths,
            "triggered_test_paths": triggered_paths,
            "build_url": trigger.get("build_url") or trigger.get("job_url"),
            "trigger_message": trigger.get("message"),
            "trigger_status": trigger.get("status"),
        },
    }


def format_issue_summary_text(facts: dict[str, Any]) -> str:
    """Deterministic summary in the requested format (no Claude)."""
    key = facts.get("issue_key", "")
    summary = facts.get("summary", "")
    description = facts.get("description_excerpt") or summary

    repro_lines = _numbered_lines(facts.get("reproduction_steps") or [])
    repro_block = "\n".join(repro_lines) if repro_lines else "Not available"

    live = facts.get("live_verification") or {}
    repro_status_parts = [
        f"stage={live.get('stage_status', 'N/A')}",
        f"verdict={live.get('verdict', 'N/A')}",
    ]
    if live.get("dry_run"):
        repro_status_parts.append("mode=dry-run plan")
    if live.get("cluster_name"):
        repro_status_parts.append(f"cluster={live['cluster_name']}")
    if live.get("issue_reproduced"):
        repro_status_parts.append(f"issue_reproduced={live['issue_reproduced']}")
    if live.get("conclusion"):
        repro_status_parts.append(str(live["conclusion"]))
    if live.get("error"):
        repro_status_parts.append(f"error={live['error']}")
    repro_status = "; ".join(repro_status_parts) or "Not run"

    match_lines: list[str] = []
    for test in facts.get("matching_tests") or []:
        node_id = test.get("test_node_id", "")
        score = test.get("relevance_score")
        prefix = f"- {node_id}"
        if score is not None:
            prefix += f" (score={score})"
        match_lines.append(prefix)
        reasons = test.get("match_reasons") or []
        if reasons:
            match_lines.append(f"  Reasons: {'; '.join(reasons[:2])}")
    matching_block = "\n".join(match_lines) if match_lines else "No matches"

    test_exec = facts.get("test_execution") or {}
    exec_parts = []
    if test_exec.get("jenkins_dry_run") is True:
        exec_parts.append("Jenkins dry-run (not triggered)")
    elif test_exec.get("jenkins_dry_run") is False:
        exec_parts.append("Jenkins triggered")
    if test_exec.get("trigger_message"):
        exec_parts.append(str(test_exec["trigger_message"]))
    if test_exec.get("triggered_test_paths"):
        exec_parts.append("Tests: " + ", ".join(test_exec["triggered_test_paths"]))
    elif test_exec.get("issue_test_paths"):
        exec_parts.append(
            "Matched (not triggered): " + ", ".join(test_exec["issue_test_paths"])
        )
    if test_exec.get("build_url"):
        exec_parts.append(f"Build: {test_exec['build_url']}")
    if test_exec.get("deploy_job_url"):
        exec_parts.append(f"Deploy job: {test_exec['deploy_job_url']}")
    test_exec_block = "\n".join(exec_parts) if exec_parts else "Not run"

    return (
        f"Issue ID and description : {key} — {summary}\n"
        f"{description}\n\n"
        f"issue repro steps:\n{repro_block}\n\n"
        f"repro execution status : {repro_status}\n\n"
        f"matching test :\n{matching_block}\n\n"
        f"test execution details :\n{test_exec_block}\n"
    )


def _load_system_prompt() -> str:
    path = _PROMPTS_DIR / "issue_summary_system.txt"
    if path.is_file():
        return path.read_text(encoding="utf-8").strip()
    return "Write a plain-text issue verification summary from the facts JSON."


def _resolve_claude_bin() -> str:
    claude_bin = shutil.which("claude")
    if not claude_bin:
        raise RuntimeError(
            "Claude CLI not found on PATH (install: npm i -g @anthropic-ai/claude-code)"
        )
    return claude_bin


def _build_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in _CLAUDE_ENV_VARS:
        if key in os.environ:
            env[key] = os.environ[key]
    return env


def generate_issue_summary_with_claude(
    facts: dict[str, Any],
    issue: dict[str, Any],
    *,
    model: str | None = None,
    cwd: Path | None = None,
    timeout: int = 300,
) -> str:
    """Use Claude CLI to produce a polished per-issue summary."""
    session_id, resume = resolve_issue_session(issue)
    system_prompt = _load_system_prompt()
    user_prompt = (
        "Write the issue verification summary for this issue using the facts below.\n\n"
        f"{json.dumps(facts, indent=2)}"
    )

    claude_bin = _resolve_claude_bin()
    cmd = [
        claude_bin,
        "-p",
        user_prompt,
        "--output-format",
        "json",
        "--permission-mode",
        "bypassPermissions",
        "--system-prompt",
        system_prompt,
    ]
    if model:
        cmd.extend(["--model", model])
    extend_claude_cli_cmd(cmd, session_id, resume=resume)

    work_dir = cwd or _REPO_ROOT
    log.info(
        "Running claude -p issue summary for %s (session=%s, resume=%s)",
        facts.get("issue_key"),
        session_id,
        resume,
    )
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_build_env(),
        cwd=str(work_dir),
        stdin=subprocess.DEVNULL,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Claude CLI failed (code {proc.returncode}): "
            f"{(proc.stderr or proc.stdout or '')[:800]}"
        )
    raw = (proc.stdout or "").strip()
    response = json.loads(raw)
    if response.get("is_error"):
        raise RuntimeError(response.get("result", "Claude CLI error"))
    text = str(response.get("result", "")).strip()
    if not text:
        raise RuntimeError("Claude returned empty issue summary")
    return text


def write_issue_summaries(
    run_record_data: dict[str, Any],
    output_dir: Path | str,
    *,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Write one summary file per issue under ``<output_dir>/summaries/``.

    Returns metadata dict with paths and generator used per issue.
    """
    parameters = parameters or {}
    if not parameters.get("issue_summary_enabled", True):
        return {"enabled": False, "files": []}

    use_claude = bool(parameters.get("issue_summary_use_claude", True))
    model = parameters.get("issue_summary_model")
    tests_per_issue = int(parameters.get("tests_per_issue", 1))
    odf_version = run_record_data.get("odf_version", "")
    jenkins = run_record_data.get("jenkins_execution") or {}

    summaries_dir = Path(output_dir) / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)

    files: list[dict[str, Any]] = []
    for issue in run_record_data.get("issues") or []:
        key = issue.get("key") or "unknown"
        facts = build_issue_summary_facts(
            issue,
            jenkins_execution=jenkins,
            odf_version=odf_version,
            tests_per_issue=tests_per_issue,
        )
        generator = "template"
        try:
            if use_claude:
                body = generate_issue_summary_with_claude(
                    facts,
                    issue,
                    model=model,
                )
                generator = "claude"
            else:
                body = format_issue_summary_text(facts)
        except Exception as exc:
            log.warning(
                "Claude issue summary failed for %s (%s); using template",
                key,
                exc,
            )
            body = format_issue_summary_text(facts)
            generator = "template_fallback"

        dest = summaries_dir / f"{key}_summary.txt"
        dest.write_text(body.strip() + "\n", encoding="utf-8")
        log.info("Wrote issue summary: %s (%s)", dest, generator)
        files.append(
            {
                "issue_key": key,
                "path": str(dest.resolve()),
                "generator": generator,
            }
        )

    return {
        "enabled": True,
        "summaries_dir": str(summaries_dir.resolve()),
        "files": files,
    }
