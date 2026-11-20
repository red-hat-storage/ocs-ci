"""
Match ocs-ci tests via Claude Code CLI (``claude -p``) — agent search over tests/.

Uses reproduction + **verification steps** as context. Claude searches the repo
with Read/Glob/Grep; no heuristic coverage mapper.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

_AGENT_DIR = Path(__file__).resolve().parent
_WORKFLOW_DIR = Path(__file__).resolve().parents[2] / "workflow"
if str(_AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENT_DIR))
if str(_WORKFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_DIR))

from workflow_lib.claude_session import extend_claude_cli_cmd, resolve_issue_session

from claude_matcher import (
    MATCH_TESTS_OUTPUT_SCHEMA,
    _extract_json_from_text,
    _load_prompt,
    _normalize_match_payload,
    build_match_tests_prompt,
)
from models import MATCHER_CLAUDE_CLI

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]

_CLAUDE_ENV_VARS = (
    "CLAUDECODE",
    "CLAUDE_CODE",
    "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_CODE_SSE_PORT",
)

DEFAULT_SEARCH_TIMEOUT_S = 1200
DEFAULT_FORMAT_TIMEOUT_S = 120

_SEARCH_TOOLS = ("Read", "Glob", "Grep", "Bash")


def _build_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in _CLAUDE_ENV_VARS:
        env.pop(key, None)
    return env


def _resolve_claude_bin() -> str:
    path = shutil.which("claude", path=_build_env().get("PATH"))
    if not path:
        raise RuntimeError(
            "Claude Code CLI ('claude') not found. Install Claude Code and run "
            "'claude login', or set test_match_backend: claude-sdk."
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
    allowed_tools: tuple[str, ...] | None = None,
    model: str | None = None,
    timeout: int = DEFAULT_SEARCH_TIMEOUT_S,
    session_id: str | None = None,
    resume_session: bool = False,
) -> dict[str, Any]:
    claude_bin = _resolve_claude_bin()
    cmd = [
        claude_bin,
        "-p",
        prompt,
        "--output-format",
        "json",
        "--permission-mode",
        "bypassPermissions",
    ]
    if system_prompt:
        cmd.extend(["--system-prompt", system_prompt])
    if model:
        cmd.extend(["--model", model])
    if allowed_tools:
        for tool in allowed_tools:
            cmd.extend(["--allowedTools", tool])
    if session_id:
        extend_claude_cli_cmd(cmd, session_id, resume=resume_session)

    log.info(
        "Running claude -p test match (cwd=%s, tools=%s, timeout=%ss, session=%s, resume=%s)",
        cwd,
        list(allowed_tools or []),
        timeout,
        session_id,
        resume_session,
    )
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_build_env(),
        cwd=str(cwd),
        stdin=subprocess.DEVNULL,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Claude CLI failed (code {proc.returncode}): "
            f"{(proc.stderr or proc.stdout or '')[:800]}"
        )

    response = json.loads((proc.stdout or "").strip())
    if response.get("is_error"):
        raise RuntimeError(response.get("result", "Claude CLI error"))
    return response


def _format_analysis_as_json_cli(
    analysis: str,
    issue_id: str,
    *,
    top_n: int,
    cwd: Path,
    model: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    if not analysis.strip():
        raise RuntimeError("No analysis text to format as JSON")

    format_prompt = _load_prompt("match_ocs_ci_tests_format.txt").format(
        issue_id=issue_id,
        top_n=top_n,
        analysis=analysis[:30000],
    )
    system_prompt = (
        "You convert test-matching analysis into JSON. "
        "Return ONLY valid JSON matching the schema. No markdown.\n\n"
        f"Required keys: {', '.join(MATCH_TESTS_OUTPUT_SCHEMA['required'])}"
    )
    response = _run_claude_cli(
        format_prompt,
        system_prompt=system_prompt,
        cwd=cwd,
        allowed_tools=None,
        model=model,
        timeout=DEFAULT_FORMAT_TIMEOUT_S,
        session_id=session_id,
        resume_session=bool(session_id),
    )
    return _extract_json_from_text(response.get("result", ""))


def match_tests_with_claude_cli(
    issue: dict[str, Any],
    *,
    top_n: int = 10,
    model: str | None = None,
) -> dict[str, Any]:
    """
    Claude agent: search tests/ and return matches for verification steps.

    Returns stage data for run record append_stage_bulk.
    """
    issue_key = issue.get("key", "unknown")
    system_prompt, user_prompt = build_match_tests_prompt(issue, top_n=top_n)
    session_id, resume_session = resolve_issue_session(issue)

    log.info("Claude CLI test match phase 1 (search) for %s", issue_key)
    phase1 = _run_claude_cli(
        user_prompt,
        system_prompt=system_prompt,
        cwd=REPO_ROOT,
        allowed_tools=_SEARCH_TOOLS,
        model=model,
        session_id=session_id,
        resume_session=resume_session,
    )
    analysis = (phase1.get("result") or "").strip()
    if not analysis:
        raise RuntimeError(
            f"Claude CLI returned no test-matching analysis for {issue_key}"
        )

    log.info("Claude CLI test match phase 2 (JSON) for %s", issue_key)
    try:
        parsed = _format_analysis_as_json_cli(
            analysis,
            issue_key,
            top_n=top_n,
            cwd=REPO_ROOT,
            model=model,
            session_id=session_id,
        )
    except (json.JSONDecodeError, RuntimeError) as exc:
        log.warning(
            "JSON format phase failed for %s (%s); inline parse", issue_key, exc
        )
        parsed = _extract_json_from_text(analysis)

    result = _normalize_match_payload(
        parsed,
        issue,
        matcher=MATCHER_CLAUDE_CLI,
        analysis_notes=parsed.get("analysis_notes")
        or "Claude CLI matched tests from verification steps via repo search.",
        verification_report=analysis,
    )
    result["claude_session_id"] = session_id
    return result
