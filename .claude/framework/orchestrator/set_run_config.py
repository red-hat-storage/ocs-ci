#!/usr/bin/env python3
"""Write run-config.json and active-run.json for the current workflow invocation."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import sys

LIB = Path(__file__).resolve().parents[1] / "lib"
sys.path.insert(0, str(LIB))
from dry_run import disable_dry_run, enable_dry_run  # noqa: E402
from workflow_registry import load_workflow, prompt_filename  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--odf-version", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    wf = load_workflow(args.workflow)
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    prompt_name = prompt_filename(args.workflow)
    prompt_path = args.workspace / prompt_name

    args.workspace.mkdir(parents=True, exist_ok=True)

    cfg = {
        "run_id": run_id,
        "workflow_id": wf["id"],
        "workflow_name": wf.get("name", wf["id"]),
        "odf_version": args.odf_version,
        "dry_run": args.dry_run,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "coordinator_agent": wf.get("coordinator_agent", "orchestrator-coordinator"),
        "prompt_path": str(prompt_path),
        "registry_file": f".claude/framework/registry/workflows/{wf['id']}.yaml",
    }
    (args.workspace / "run-config.json").write_text(json.dumps(cfg, indent=2) + "\n")
    (args.workspace / "active-run.json").write_text(json.dumps(cfg, indent=2) + "\n")
    (args.workspace / ".active-workflow").write_text(wf["id"] + "\n")

    if args.dry_run:
        enable_dry_run(args.workspace)
    else:
        disable_dry_run(args.workspace)

    print(json.dumps(cfg))


if __name__ == "__main__":
    main()
