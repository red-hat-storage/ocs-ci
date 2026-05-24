#!/usr/bin/env python3
"""Build analysis.json from JIRA raw issue (facts only — no hardcoded scenarios)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from build_repro_steps import adf_text, extract_fix_snippet, extract_jira_context


def enrich(raw: dict, *, issue_key: str) -> dict:
    fields = raw.get("fields") or {}
    desc = fields.get("description")
    text = adf_text(desc) if isinstance(desc, dict) else str(desc or "")
    labels = fields.get("labels") or []
    blocked = "skip-ocsci-agent" in labels
    ctx = extract_jira_context(raw)
    fix = extract_fix_snippet(text)

    return {
        "issue_key": issue_key,
        "summary": fields.get("summary"),
        "status": (fields.get("status") or {}).get("name"),
        "labels": labels,
        "description_excerpt": text[:8000],
        "fix_snippets": [fix] if fix else [],
        "jira_target_release": ctx.get("target_release"),
        "jira_components": ctx.get("components", []),
        "linked_issues": ctx.get("linked_issues", []),
        "feasible": not blocked,
        "skipped_by_label": blocked,
        "confidence": 0.0 if blocked else 0.5,
        "ai_generation_required": True,
        "root_cause_summary": None,
        "expected_behavior": None,
        "verification_strategy": None,
        "note": "Interpretation and test plan come from Claude via verification-generation-prompt.md",
    }


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
