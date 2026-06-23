"""
Live cluster verification via the Claude Code CLI (``claude -p``).

Uses your existing Claude Code login — no ANTHROPIC_API_KEY required.
Run ``claude login`` once if you have not already.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from claude_verifier import (
    VERIFY_OUTPUT_SCHEMA,
    _extract_json_from_text,
    _load_prompt,
    _normalize_verdict,
    _verification_work_dir,
    build_verify_prompt,
)
from models import VERIFIER_LIVE_CLI

log = logging.getLogger(__name__)

_CLAUDE_ENV_VARS = (
    "CLAUDECODE",
    "CLAUDE_CODE",
    "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_CODE_SSE_PORT",
)

DEFAULT_PHASE1_TIMEOUT_S = 3600
DEFAULT_PHASE2_TIMEOUT_S = 120


def _build_env() -> dict[str, str]:
    """Child env for ``claude -p`` — drop nested-session markers."""
    env = os.environ.copy()
    for key in _CLAUDE_ENV_VARS:
        env.pop(key, None)
    return env


def _resolve_claude_bin() -> str:
    path = shutil.which("claude", path=_build_env().get("PATH"))
    if not path:
        raise RuntimeError(
            "Claude Code CLI ('claude') not found. Install Claude Code and run "
            "'claude login', or use --backend sdk with claude-agent-sdk."
        )
    return path


def is_claude_cli_available() -> bool:
    try:
        claude_bin = _resolve_claude_bin()
    except RuntimeError:
        return False
    try:
        proc = subprocess.run(
            [claude_bin, "--version"],
            capture_output=True,
            timeout=15,
            env=_build_env(),
            stdin=subprocess.DEVNULL,
        )
        return proc.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _run_claude_cli(
    prompt: str,
    *,
    system_prompt: str | None = None,
    cwd: Path,
    allowed_tools: list[str] | None = None,
    model: str | None = None,
    permission_mode: str = "bypassPermissions",
    timeout: int = DEFAULT_PHASE1_TIMEOUT_S,
) -> dict[str, Any]:
    claude_bin = _resolve_claude_bin()
    cmd = [
        claude_bin,
        "-p",
        prompt,
        "--output-format",
        "json",
        "--permission-mode",
        permission_mode,
    ]
    if system_prompt:
        cmd.extend(["--system-prompt", system_prompt])
    if model:
        cmd.extend(["--model", model])
    if allowed_tools:
        for tool in allowed_tools:
            cmd.extend(["--allowedTools", tool])

    start = time.time()
    log.info(
        "Running claude -p (cwd=%s, tools=%s, timeout=%ss)",
        cwd,
        allowed_tools or [],
        timeout,
    )
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=_build_env(),
            cwd=str(cwd),
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Claude CLI timed out after {timeout}s (cwd={cwd})"
        ) from exc

    duration = time.time() - start
    if proc.returncode != 0:
        stderr_text = (proc.stderr or "").strip()[:800]
        stdout_text = (proc.stdout or "").strip()[:800]
        raise RuntimeError(
            f"Claude CLI exited with code {proc.returncode} "
            f"(duration={duration:.1f}s)\nstderr: {stderr_text}\nstdout: {stdout_text}"
        )

    raw = (proc.stdout or "").strip()
    try:
        response = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Failed to parse Claude CLI JSON output: {exc}\nRaw: {raw[:500]}"
        ) from exc

    if response.get("is_error"):
        raise RuntimeError(
            f"Claude CLI error: {response.get('result', 'unknown')}. "
            "Run 'claude login' if not authenticated."
        )

    response["_duration_s"] = duration
    response["_num_turns"] = response.get("num_turns", 1)
    return response


def _format_analysis_as_json_cli(
    analysis: str,
    issue_id: str,
    *,
    cwd: Path,
    model: str | None = None,
) -> dict[str, Any]:
    if not analysis:
        raise RuntimeError("No analysis text to format as JSON")

    format_prompt = _load_prompt("odf_issue_reproduction_verify_format.txt").format(
        issue_id=issue_id,
        analysis=analysis[:30000],
    )
    system_prompt = (
        "You convert cluster verification reports into JSON. "
        "Return ONLY valid JSON matching the schema. No markdown, no explanation.\n\n"
        f"Schema keys: {', '.join(VERIFY_OUTPUT_SCHEMA['required'])}"
    )
    response = _run_claude_cli(
        format_prompt,
        system_prompt=system_prompt,
        cwd=cwd,
        allowed_tools=None,
        model=model,
        timeout=DEFAULT_PHASE2_TIMEOUT_S,
    )
    result_text = response.get("result", "")
    parsed = _extract_json_from_text(result_text)
    return _normalize_verdict(parsed)


def verify_issue_with_claude_cli(
    issue: dict[str, Any],
    *,
    cluster_profile: dict[str, Any],
    kubeconfig_path: str,
    oc_command_path: str = "oc",
    model: str | None = None,
    work_dir: Path | None = None,
    permission_mode: str = "bypassPermissions",
    timeout: int = DEFAULT_PHASE1_TIMEOUT_S,
) -> dict[str, Any]:
    """
    Run live cluster verification using ``claude -p`` (Claude Code subscription auth).

    Returns:
        dict: Stage data for run record

    """
    issue_key = issue.get("key", "")
    verify_dir = work_dir or _verification_work_dir(issue_key, cluster_profile)
    verify_dir.mkdir(parents=True, exist_ok=True)
    output_log_path = str(verify_dir / "verification.log")

    system_prompt, user_prompt = build_verify_prompt(
        issue,
        cluster_profile=cluster_profile,
        kubeconfig_path=kubeconfig_path,
        oc_command_path=oc_command_path,
        output_log_path=output_log_path,
    )

    log.info(
        "Claude CLI cluster verify phase 1 (live) for %s (kubeconfig=%s)",
        issue_key,
        kubeconfig_path,
    )
    phase1 = _run_claude_cli(
        user_prompt,
        system_prompt=system_prompt,
        cwd=verify_dir,
        allowed_tools=["Bash", "Read"],
        model=model,
        permission_mode=permission_mode,
        timeout=timeout,
    )
    analysis = (phase1.get("result") or "").strip()
    if not analysis:
        raise RuntimeError(
            f"Claude CLI returned no verification report for {issue_key}"
        )

    log.info("Claude CLI cluster verify phase 2 (JSON format) for %s", issue_key)
    try:
        parsed = _format_analysis_as_json_cli(
            analysis, issue_key, cwd=verify_dir, model=model
        )
    except (json.JSONDecodeError, RuntimeError) as exc:
        log.warning(
            "JSON format phase failed for %s (%s); inline parse", issue_key, exc
        )
        parsed = _extract_json_from_text(analysis)
        parsed = _normalize_verdict(parsed)

    parsed.setdefault("issue_id", issue_key)
    parsed.setdefault("output_log_path", output_log_path)
    parsed.setdefault("resources_created", [])
    parsed.setdefault("reproduction_steps_summary", [])
    parsed.setdefault("expected_results_validation", [])
    parsed.setdefault("cleanup_status", {"all_deleted": False, "details": ""})
    parsed["matcher"] = VERIFIER_LIVE_CLI
    parsed["backend"] = "claude-cli"
    parsed["dry_run"] = False
    parsed["issue_summary"] = issue.get("summary", "")
    parsed["verification_report"] = analysis
    parsed["work_dir"] = str(verify_dir)
    parsed["claude_cli_turns"] = phase1.get("_num_turns")
    parsed["claude_cli_duration_s"] = phase1.get("_duration_s")
    parsed["claude_cli_cost_usd"] = phase1.get("total_cost_usd")
    return parsed
