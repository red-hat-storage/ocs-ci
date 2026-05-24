#!/usr/bin/env python3
"""Backward-compat wrapper — delegates to build_repro_context.enrich()."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from build_repro_context import enrich


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("issue_key")
    parser.add_argument("--art", type=Path, required=True)
    args = parser.parse_args()

    raw = json.loads((args.art / "jira-raw.json").read_text())
    plan = enrich(raw, issue_key=args.issue_key.upper())
    (args.art / "analysis.json").write_text(json.dumps(plan, indent=2) + "\n")
    print("skipped" if plan.get("skipped_by_label") else "ok")


if __name__ == "__main__":
    main()
