#!/usr/bin/env python3
"""Print registered workflows (for run.sh --list-workflows)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))
from workflow_registry import DEFAULT_WORKFLOW, list_workflows  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="List Claude orchestrator workflows")
    parser.add_argument("--json", action="store_true", help="Machine-readable output")
    args = parser.parse_args()

    workflows = list_workflows()
    if args.json:
        import json

        print(json.dumps(workflows, indent=2))
        return

    if not workflows:
        print("No workflows registered under .claude/framework/registry/workflows/")
        return

    print("Registered workflows:\n")
    for wf in workflows:
        mark = " (default)" if wf["default"] else ""
        print(f"  {wf['id']}{mark}")
        print(f"    Name: {wf['name']}")
        if wf["description"]:
            print(f"    About: {wf['description']}")
        print(f"    File:  {wf['file']}")
        print()
    print("Run example:")
    print(
        f"  .claude/framework/orchestrator/run.sh --workflow {DEFAULT_WORKFLOW} <odf-version>"
    )


if __name__ == "__main__":
    main()
