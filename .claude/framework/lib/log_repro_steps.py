#!/usr/bin/env python3
"""Write reproduction steps for an issue into workspace/logs/run.log."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def workspace_path() -> Path:
    ws = os.environ.get("JIRA_AGENT_WORKSPACE", "").strip()
    return Path(ws) if ws else ROOT / ".claude" / "workspace"


def parse_repro_yaml(text: str) -> dict:
    """Minimal parser for repro-steps.yaml (no PyYAML required)."""
    data: dict = {"steps": [], "confidence": None, "missing_info": []}
    step: dict | None = None
    details_lines: list[str] = []
    in_details_block = False

    def flush_step() -> None:
        nonlocal step, details_lines, in_details_block
        if step is not None:
            if details_lines:
                step["details"] = "\n".join(details_lines).strip()
            data["steps"].append(step)
        step = None
        details_lines = []
        in_details_block = False

    for raw in text.splitlines():
        line = raw.rstrip()
        if re.match(r"^confidence:\s*", line):
            flush_step()
            data["confidence"] = line.split(":", 1)[1].strip()
            continue
        if re.match(r"^missing_info:", line):
            flush_step()
            continue
        m = re.match(r"^\s*-\s+action:\s*(.+)$", line)
        if m:
            flush_step()
            step = {"action": m.group(1).strip()}
            continue
        m = re.match(r"^\s*details:\s*\|\s*$", line)
        if m and step is not None:
            in_details_block = True
            continue
        m = re.match(r"^\s*details:\s*(.+)$", line)
        if m and step is not None:
            step["details"] = m.group(1).strip()
            in_details_block = False
            continue
        if in_details_block and step is not None:
            details_lines.append(line.strip())
            continue
    flush_step()
    return data


def log_via_script(level: str, message: str) -> None:
    log_sh = ROOT / ".claude/framework/lib/log_run.sh"
    subprocess.run([str(log_sh), level, message], check=False, env=os.environ.copy())


def log_repro(issue_key: str, repro_path: Path) -> None:
    if not repro_path.is_file():
        log_via_script("WARN", f"repro-steps {issue_key}: file missing — {repro_path}")
        return

    data = parse_repro_yaml(repro_path.read_text())
    steps = data.get("steps") or []
    conf = data.get("confidence")
    header = f"repro-steps {issue_key}: {len(steps)} step(s)"
    if conf is not None:
        header += f", confidence={conf}"
    log_via_script("INFO", header)

    if not steps:
        log_via_script("WARN", f"repro-steps {issue_key}: (no steps defined)")
        return

    for i, step in enumerate(steps, 1):
        action = step.get("action", "unknown")
        details = (step.get("details") or "").strip()
        if details:
            for j, detail_line in enumerate(details.splitlines()):
                prefix = f"repro-steps {issue_key}: [{i}] {action}"
                if j == 0:
                    log_via_script("INFO", f"{prefix} — {detail_line}")
                else:
                    log_via_script(
                        "INFO", f"repro-steps {issue_key}:     {detail_line}"
                    )
        else:
            log_via_script("INFO", f"repro-steps {issue_key}: [{i}] {action}")


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
