"""Dry-run mode: execute workload locally; skip JIRA/GitHub mutations."""

from __future__ import annotations

import json
import os
from pathlib import Path

TRUTHY = frozenset({"1", "true", "yes", "on"})


def _workspace() -> Path | None:
    ws = os.environ.get("JIRA_AGENT_WORKSPACE", "").strip()
    return Path(ws) if ws else None


def is_dry_run(workspace: Path | None = None) -> bool:
    env = os.environ.get("DFBUGS_DRY_RUN", "").strip().lower()
    if env in TRUTHY:
        return True
    ws = workspace or _workspace()
    if ws and (ws / ".dry-run").is_file():
        return True
    if ws:
        cfg = ws / "run-config.json"
        if cfg.is_file():
            try:
                data = json.loads(cfg.read_text())
                if data.get("dry_run") is True:
                    return True
            except json.JSONDecodeError:
                pass
    return False


def enable_dry_run(workspace: Path) -> None:
    """Mark workspace as dry-run (called by orchestrator bootstrap)."""
    os.environ["DFBUGS_DRY_RUN"] = "1"
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / ".dry-run").write_text("enabled\n")
    cfg_path = workspace / "run-config.json"
    data: dict = {}
    if cfg_path.is_file():
        try:
            data = json.loads(cfg_path.read_text())
        except json.JSONDecodeError:
            data = {}
    data["dry_run"] = True
    cfg_path.write_text(json.dumps(data, indent=2) + "\n")


def disable_dry_run(workspace: Path) -> None:
    os.environ.pop("DFBUGS_DRY_RUN", None)
    marker = workspace / ".dry-run"
    if marker.is_file():
        marker.unlink()
    cfg_path = workspace / "run-config.json"
    if cfg_path.is_file():
        data = json.loads(cfg_path.read_text())
        data["dry_run"] = False
        cfg_path.write_text(json.dumps(data, indent=2) + "\n")


def jira_github_writes_allowed() -> bool:
    return not is_dry_run()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "check":
        raise SystemExit(0 if is_dry_run() else 1)
    print("dry-run" if is_dry_run() else "live")
