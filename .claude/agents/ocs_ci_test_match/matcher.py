"""
Claude-based test matching for JIRA issues.

Routes to Claude Code CLI (default) or Claude Agent SDK. Claude searches tests/
using reproduction and verification steps from the repro_steps stage.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_AGENT_DIR = Path(__file__).resolve().parent

TestMatchBackend = str  # auto | claude-cli | claude-sdk


def _ensure_agent_path() -> None:
    agent_dir = str(_AGENT_DIR)
    if agent_dir not in sys.path:
        sys.path.insert(0, agent_dir)


def _resolve_test_match_backend(
    backend: str,
    *,
    use_claude_sdk: bool = False,
) -> str:
    if backend == "vector_db":
        raise RuntimeError(
            "test_match_backend=vector_db was removed. "
            "Use auto (Claude CLI), claude-cli, or claude-sdk."
        )
    if backend == "claude-cli":
        _ensure_agent_path()
        from claude_cli_matcher import is_claude_cli_available

        if not is_claude_cli_available():
            raise RuntimeError(
                "Claude Code CLI is required for test matching. "
                "Run 'claude login' or set test_match_backend: claude-sdk."
            )
        return "claude-cli"
    if backend in ("claude-sdk", "sdk") or use_claude_sdk:
        return "claude-sdk"

    _ensure_agent_path()
    from claude_cli_matcher import is_claude_cli_available

    if is_claude_cli_available():
        return "claude-cli"
    if use_claude_sdk:
        return "claude-sdk"
    raise RuntimeError(
        "Claude is required for test matching. "
        "Install Claude Code CLI ('claude login') or set test_match_backend: claude-sdk."
    )


def run_test_matching_stage(
    issues: list[dict[str, Any]],
    *,
    top_n: int = 10,
    backend: str = "auto",
    model: str | None = None,
    min_score: int = 35,
) -> dict[str, dict[str, Any]]:
    """
    Find matching ocs-ci tests for all issues via Claude agent.

    Claude searches tests/ using reproduction + verification steps from stage 2.
    """
    del min_score  # unused; kept for backward-compatible call sites
    per_issue: dict[str, dict[str, Any]] = {}

    try:
        resolved = _resolve_test_match_backend(backend, use_claude_sdk=False)
    except RuntimeError as exc:
        log.error("Test matching backend unavailable: %s", exc)
        for issue in issues:
            key = issue.get("key")
            if key:
                per_issue[key] = _failed_match_payload(key, issue, str(exc))
        return per_issue

    for issue in issues:
        key = issue.get("key")
        if not key:
            continue

        repro_stage = issue.get("stages", {}).get("repro_steps")
        if not repro_stage or repro_stage.get("status") != "completed":
            log.warning(
                "Issue %s missing completed repro_steps stage; "
                "matching with intake data only",
                key,
            )

        if resolved == "claude-cli":
            _ensure_agent_path()
            from claude_cli_matcher import match_tests_with_claude_cli

            try:
                per_issue[key] = match_tests_with_claude_cli(
                    issue, top_n=top_n, model=model
                )
            except Exception as exc:
                log.error("Claude CLI test matching failed for %s: %s", key, exc)
                per_issue[key] = _failed_match_payload(key, issue, str(exc))
            continue

        _ensure_agent_path()
        from claude_matcher import match_tests_with_claude_agent_sync

        try:
            per_issue[key] = match_tests_with_claude_agent_sync(
                issue, top_n=top_n, model=model
            )
        except Exception as exc:
            log.error("Claude SDK test matching failed for %s: %s", key, exc)
            per_issue[key] = _failed_match_payload(key, issue, str(exc))

    return per_issue


def _failed_match_payload(
    key: str, issue: dict[str, Any], error: str
) -> dict[str, Any]:
    return {
        "issue_id": key,
        "issue_summary": issue.get("summary", ""),
        "matcher": "claude_agent",
        "status": "failed",
        "error": error,
        "matching_test_count": 0,
        "matching_tests": [],
    }
