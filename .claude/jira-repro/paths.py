"""Shared path constants for the jira-repro tooling."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # ocs-ci repo root


def workspace_path() -> Path:
    ws = os.environ.get("JIRA_AGENT_WORKSPACE", "").strip()
    return Path(ws) if ws else ROOT / ".claude" / "workspace"
