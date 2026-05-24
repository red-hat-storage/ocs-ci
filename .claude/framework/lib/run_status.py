#!/usr/bin/env python3
"""Read/update workspace/run-status.json for watch.sh and agents."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jira-repro"))
from paths import ROOT, workspace_path


def status_path(workspace: Path | None = None) -> Path:
    return (workspace or workspace_path()) / "run-status.json"


def load(workspace: Path | None = None) -> dict:
    path = status_path(workspace)
    if not path.is_file():
        return {
            "phase": "idle",
            "last_message": "No run-status yet — bootstrap with run.sh",
        }
    return json.loads(path.read_text())


def save(data: dict, workspace: Path | None = None) -> Path:
    ws = workspace or workspace_path()
    ws.mkdir(parents=True, exist_ok=True)
    path = status_path(ws)
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(data, indent=2) + "\n")
    return path


def merge(patch: dict, workspace: Path | None = None) -> dict:
    data = load(workspace)
    data.update(patch)
    save(data, workspace)
    return data


def print_summary(workspace: Path | None = None) -> None:
    ws = workspace or workspace_path()
    active = ws / "active-run.json"
    st = load(ws)
    disc_file = ws / "discovery" / "issues.json"

    print("=" * 60)
    print("DFBUGS RUN PROGRESS")
    print("=" * 60)

    mcp_ready = ws / "mcp-ready.json"
    if mcp_ready.is_file():
        mcp = json.loads(mcp_ready.read_text())
        print(f"MCP (env):   redhat-jira OK — {mcp.get('jira_mcp_url', '?')}")
    else:
        print("MCP (env):   NOT READY — run setup_mcp.sh or run.sh")

    if active.is_file():
        ar = json.loads(active.read_text())
        print(f"Workflow:    {ar.get('workflow_id', '?')}")
        print(f"ODF version: {ar.get('odf_version', '?')}")
        print(f"Run ID:      {ar.get('run_id', '?')}")
        print(f"Dry-run:     {ar.get('dry_run', False)}")
    else:
        print("Active run:  (none — run run.sh first)")

    print(f"Phase:       {st.get('phase', 'idle')}")
    print(f"Message:     {st.get('last_message', '—')}")

    if disc_file.is_file():
        disc = json.loads(disc_file.read_text())
        keys = disc.get("issue_keys", [])
        err = disc.get("error")
        if err and not keys:
            print(f"Discovery:   FAILED — {err}")
            if disc.get("hint"):
                print(f"Hint:        {disc['hint']}")
            if disc.get("jql_used"):
                print(f"Last JQL:    {disc['jql_used']}")
        else:
            print(
                f"Discovery:   DONE — {len(keys)} issue(s) for ODF {disc.get('odf_version', '?')}"
            )
            if keys:
                preview = ", ".join(keys[:8])
                if len(keys) > 8:
                    preview += f", ... (+{len(keys) - 8} more)"
                print(f"Issue keys:  {preview}")
            elif not err:
                print(
                    "Issue keys:  (none — try: jira-repro/discovery/run.sh or edit configs/jira-discovery.yaml)"
                )
    else:
        print("Discovery:   NOT RUN (missing discovery/issues.json)")

    outcomes = (
        list((ws / "outcomes").glob("*.json")) if (ws / "outcomes").is_dir() else []
    )
    artifacts = (
        list((ws / "artifacts").glob("DFBUGS-*")) if (ws / "artifacts").is_dir() else []
    )
    print(f"Outcomes:    {len(outcomes)} file(s) in outcomes/")
    print(f"Artifacts:   {len(artifacts)} issue dir(s) in artifacts/")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("show", help="Print human-readable progress")
    p_set = sub.add_parser("set")
    p_set.add_argument("--phase")
    p_set.add_argument("--message")

    args = parser.parse_args()
    if args.cmd == "show" or args.cmd is None:
        print_summary()
    elif args.cmd == "set":
        patch: dict = {}
        if args.phase:
            patch["phase"] = args.phase
        if args.message:
            patch["last_message"] = args.message
        merge(patch)
        print_summary()


if __name__ == "__main__":
    main()
