#!/usr/bin/env python3
"""Build repro-steps.yaml from JIRA analysis.json."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def adf_text(node) -> str:
    if isinstance(node, dict):
        if node.get("type") == "text":
            return str(node.get("text", ""))
        return "".join(adf_text(c) for c in node.get("content") or [])
    if isinstance(node, list):
        return "".join(adf_text(x) for x in node)
    return ""


_SECTION_STOP = re.compile(
    r"^(actual results|expected results|logs collected|additional info|"
    r"the exact date|can this issue|is there any workaround)",
    re.IGNORECASE,
)


def extract_numbered_steps(description: str) -> list[dict]:
    steps: list[dict] = []
    if not description:
        return steps

    section = description
    m = re.search(
        r"steps\s+to\s+reproduce:?\s*(.*)",
        description,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        section = m.group(1)

    for line in section.splitlines():
        line = line.strip()
        if not line:
            continue
        if _SECTION_STOP.match(line):
            break
        num = re.match(r"^(\d+)[.)]\s+(.+)$", line)
        if not num:
            continue
        content = num.group(2).strip()
        if not content or content in (".", "-"):
            continue
        if len(content) > 240 or _SECTION_STOP.search(content):
            continue
        steps.append(
            {
                "action": f"step_{num.group(1)}",
                "details": content,
            }
        )
    return steps


def steps_from_issue(analysis: dict, raw_jira: dict | None) -> list[dict]:
    summary = (analysis.get("summary") or "").lower()
    desc = analysis.get("description_excerpt") or ""
    if raw_jira:
        fields = raw_jira.get("fields") or {}
        d = fields.get("description")
        if isinstance(d, dict):
            desc = adf_text(d) or desc

    numbered = extract_numbered_steps(desc)
    # Use JIRA numbered steps only when they contain real instructions (not empty 1. 2. 3.)
    if len(numbered) >= 1 and all(len(s.get("details", "")) >= 10 for s in numbered):
        return numbered

    if "md_blow" in summary or "noobaa" in summary.lower():
        fix_line = ""
        m = re.search(
            r"await\s+require\([^)]+\)[^;]*load_root_keys_from_mount\(\)[^;]*;?",
            desc,
        )
        if m:
            fix_line = m.group(0).strip()
        else:
            for line in desc.splitlines():
                if "load_root_keys_from_mount" in line:
                    fix_line = line.strip()[:200]
                    break
        steps = [
            {
                "action": "patch_noobaa_core_resources",
                "details": "Ensure noobaa-core has sufficient CPU/memory (see ocs_ci.ocs.md_blow.MdBlow).",
            },
            {
                "action": "run_md_blow",
                "details": (
                    "From noobaa-core pod, run md_blow.js to fill the NooBaa DB. "
                    "Expected fix: call load_root_keys_from_mount() before md_blow "
                    "to avoid NO_SUCH_KEY." + (f" Fix: {fix_line}" if fix_line else "")
                ),
            },
            {
                "action": "check_logs",
                "details": "Confirm no NO_SUCH_KEY in noobaa-core logs during DB fill.",
            },
        ]
        return steps

    strategy = analysis.get("verification_strategy") or analysis.get(
        "root_cause_summary"
    )
    if strategy:
        return [{"action": "verify_per_jira", "details": strategy}]

    return [
        {
            "action": "manual_review",
            "details": "Insufficient structured repro in JIRA — review description_excerpt in analysis.json.",
        }
    ]


def write_repro_yaml(
    path: Path, steps: list[dict], confidence: float, missing: list
) -> None:
    lines = ["steps:"]
    for step in steps:
        lines.append(f"  - action: {step['action']}")
        details = (step.get("details") or "").strip()
        if "\n" in details:
            lines.append("    details: |")
            for dl in details.splitlines():
                lines.append(f"      {dl}")
        else:
            lines.append(f"    details: {details}")
    lines.append(f"confidence: {confidence}")
    lines.append("missing_info: []" if not missing else f"missing_info: {missing}")
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--issue", required=True)
    parser.add_argument("--art", type=Path, required=True)
    args = parser.parse_args()

    art = args.art
    analysis_path = art / "analysis.json"
    if not analysis_path.is_file():
        raise SystemExit(f"missing {analysis_path}")

    analysis = json.loads(analysis_path.read_text())
    raw = None
    raw_path = art / "jira-raw.json"
    if raw_path.is_file():
        raw = json.loads(raw_path.read_text())

    steps = steps_from_issue(analysis, raw)
    missing = analysis.get("missing_info") or []
    if not extract_numbered_steps(analysis.get("description_excerpt") or ""):
        missing = list(missing) + ["numbered_steps_to_reproduce_empty_in_jira"]

    conf = float(analysis.get("confidence") or 0.5)
    if missing:
        conf = min(conf, 0.6)

    write_repro_yaml(art / "repro-steps.yaml", steps, conf, missing)
    print(f"wrote {len(steps)} step(s) to {art / 'repro-steps.yaml'}")


if __name__ == "__main__":
    main()
