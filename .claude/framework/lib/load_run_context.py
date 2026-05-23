#!/usr/bin/env python3
"""Load ODF version and run metadata from the active workspace."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def workspace_path() -> Path:
    ws = os.environ.get("JIRA_AGENT_WORKSPACE", "").strip()
    if ws:
        return Path(ws)
    return ROOT / ".claude" / "workspace"


def load_context(workspace: Path | None = None) -> dict:
    ws = workspace or workspace_path()
    for name in ("active-run.json", "run-config.json"):
        path = ws / name
        if path.is_file():
            data = json.loads(path.read_text())
            data["_source"] = str(path)
            data.setdefault("jira_status", "ON_QA")
            data.setdefault("jira_project", "DFBUGS")
            if not data.get("odf_version"):
                raise ValueError(
                    f"{path} missing odf_version — re-run run.sh with <odf-version>"
                )
            return data
    raise FileNotFoundError(
        f"No active-run.json in {ws}. Bootstrap: .claude/framework/orchestrator/run.sh <odf-version>"
    )


def shell_exports(ctx: dict) -> str:
    lines = [
        f'export ODF_VERSION="{ctx["odf_version"]}"',
        f'export WORKFLOW_ID="{ctx.get("workflow_id", "")}"',
        f'export RUN_ID="{ctx.get("run_id", "")}"',
        f'export JIRA_STATUS="{ctx.get("jira_status", "ON_QA")}"',
        f'export JIRA_PROJECT="{ctx.get("jira_project", "DFBUGS")}"',
    ]
    if ctx.get("dry_run"):
        lines.append("export DFBUGS_DRY_RUN=1")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load run context from active-run.json"
    )
    parser.add_argument("--workspace", type=Path, help="Override JIRA_AGENT_WORKSPACE")
    parser.add_argument(
        "--shell", action="store_true", help="Print export statements for eval"
    )
    parser.add_argument("--field", help="Print single field (e.g. odf_version)")
    args = parser.parse_args()

    try:
        ctx = load_context(args.workspace)
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    if args.field:
        val = ctx.get(args.field)
        if val is None:
            print(f"unknown field: {args.field}", file=sys.stderr)
            sys.exit(1)
        print(val)
        return

    if args.shell:
        print(shell_exports(ctx), end="")
        return

    print(json.dumps(ctx, indent=2))


if __name__ == "__main__":
    main()
