#!/usr/bin/env python3
"""Issue state tracking via JSON file (replaces SQLite)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def _state_path(workspace: Path) -> Path:
    return workspace / "run-state.json"


def load_state(workspace: Path) -> dict:
    path = _state_path(workspace)
    if not path.is_file():
        return {"issues": {}}
    return json.loads(path.read_text())


def save_state(state: dict, workspace: Path) -> Path:
    workspace.mkdir(parents=True, exist_ok=True)
    path = _state_path(workspace)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(state, indent=2) + "\n")
    return path


def mark_issue(
    workspace: Path,
    issue_key: str,
    *,
    status: str | None = None,
    processed: bool | None = None,
    confidence: float | None = None,
    notes: str | None = None,
) -> dict:
    state = load_state(workspace)
    entry = state["issues"].setdefault(issue_key, {})
    if status is not None:
        entry["status"] = status
    if processed is not None:
        entry["processed"] = processed
    if confidence is not None:
        entry["confidence"] = confidence
    if notes is not None:
        entry["notes"] = notes
    entry["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_state(state, workspace)
    return entry


def get_issue(workspace: Path, issue_key: str) -> dict | None:
    state = load_state(workspace)
    return state["issues"].get(issue_key)
