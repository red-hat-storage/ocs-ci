#!/usr/bin/env python3
"""Write reproduction steps for an issue into workspace/logs/run.log."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jira-repro"))
from paths import ROOT, workspace_path


def parse_repro_yaml(text: str) -> dict:
    """Parse repro-steps.yaml using PyYAML."""
    import yaml

    raw = yaml.safe_load(text) or {}
    data: dict = {
        "steps": [],
        "verification_checks": [],
        "prerequisites": [],
        "pass_criteria": [],
        "confidence": raw.get("confidence"),
        "missing_info": [],
        "issue_key": raw.get("issue_key"),
        "source": raw.get("source"),
    }
    for key in ("steps", "verification_checks", "prerequisites", "pass_criteria", "missing_info"):
        val = raw.get(key)
        if isinstance(val, list):
            data[key] = val
    return data


def log_via_script(level: str, message: str) -> None:
    log_sh = ROOT / ".claude/framework/lib/log_run.sh"
    subprocess.run([str(log_sh), level, message], check=False, env=os.environ.copy())


def _log_step_block(
    issue_key: str, label: str, items: list[dict], *, prefix: str
) -> None:
    if not items:
        return
    log_via_script("INFO", f"repro-steps {issue_key}: {label} ({len(items)})")
    for i, step in enumerate(items, 1):
        action = step.get("action", "unknown")
        details = (step.get("details") or "").strip()
        head = f"repro-steps {issue_key}: {prefix}[{i}] {action}"
        if details:
            for j, detail_line in enumerate(details.splitlines()):
                log_via_script(
                    "INFO",
                    (
                        f"{head} — {detail_line}"
                        if j == 0
                        else f"repro-steps {issue_key}:     {detail_line}"
                    ),
                )
        else:
            log_via_script("INFO", head)


def log_repro(issue_key: str, repro_path: Path) -> None:
    if not repro_path.is_file():
        log_via_script("WARN", f"repro-steps {issue_key}: file missing — {repro_path}")
        return

    data = parse_repro_yaml(repro_path.read_text())
    steps = data.get("steps") or []
    checks = data.get("verification_checks") or []
    conf = data.get("confidence")
    source = data.get("source")

    header = (
        f"repro-steps {issue_key}: {len(steps)} repro step(s), "
        f"{len(checks)} verification check(s)"
    )
    if conf is not None:
        header += f", confidence={conf}"
    if source:
        header += f", source={source}"
    log_via_script("INFO", header)

    for i, pre in enumerate(data.get("prerequisites") or [], 1):
        for j, line in enumerate(pre.splitlines()):
            if j == 0:
                log_via_script(
                    "INFO", f"repro-steps {issue_key}: prerequisite [{i}] — {line}"
                )
            else:
                log_via_script("INFO", f"repro-steps {issue_key}:     {line}")

    _log_step_block(issue_key, "reproduction steps", steps, prefix="")
    _log_step_block(
        issue_key, "additional verification checks", checks, prefix="verify "
    )

    for i, crit in enumerate(data.get("pass_criteria") or [], 1):
        log_via_script(
            "INFO", f"repro-steps {issue_key}: pass_criterion [{i}] — {crit}"
        )

    missing = data.get("missing_info") or []
    if missing:
        log_via_script(
            "WARN",
            f"repro-steps {issue_key}: missing_info — {', '.join(missing)}",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Log reproduction steps to run.log")
    parser.add_argument("--issue", required=True, help="Issue key, e.g. DFBUGS-3742")
    parser.add_argument("--file", type=Path, help="Path to repro-steps.yaml")
    args = parser.parse_args()

    issue = args.issue.upper()
    repro_file = (
        args.file or workspace_path() / "artifacts" / issue / "repro-steps.yaml"
    )
    log_repro(issue, repro_file)


if __name__ == "__main__":
    main()
