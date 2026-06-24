"""
Per-issue Claude Code CLI session continuity (``--session-id`` / ``--resume``).

One session per JIRA issue across workflow stages (repro → live verify → test match)
so Claude retains context without bleeding across unrelated issues.
"""

from __future__ import annotations

import uuid
from typing import Any

ISSUE_SESSION_FIELD = "claude_session_id"


def get_issue_session_id(issue: dict[str, Any]) -> str | None:
    """Return persisted session id from the issue dict, if any."""
    session_id = issue.get(ISSUE_SESSION_FIELD)
    if isinstance(session_id, str) and session_id.strip():
        return session_id.strip()
    return None


def resolve_issue_session(issue: dict[str, Any]) -> tuple[str, bool]:
    """
    Resolve Claude CLI session for an issue.

    Returns:
        tuple: (session_id, resume) — ``resume`` is True when ``--resume`` should be used.

    """
    existing = get_issue_session_id(issue)
    if existing:
        return existing, True
    return str(uuid.uuid4()), False


def extend_claude_cli_cmd(cmd: list[str], session_id: str, *, resume: bool) -> None:
    """Append ``--session-id`` or ``--resume`` to a ``claude`` CLI argv list."""
    if resume:
        cmd.extend(["--resume", session_id])
    else:
        cmd.extend(["--session-id", session_id])


def apply_session_to_issue(issue: dict[str, Any], session_id: str) -> None:
    """Persist session id on the issue dict (run record top level)."""
    issue[ISSUE_SESSION_FIELD] = session_id


def promote_session_from_stage_data(
    issue: dict[str, Any], stage_data: dict[str, Any]
) -> None:
    """Copy ``claude_session_id`` from stage output onto the issue root."""
    session_id = stage_data.get(ISSUE_SESSION_FIELD)
    if isinstance(session_id, str) and session_id.strip():
        apply_session_to_issue(issue, session_id.strip())
