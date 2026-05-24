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
    data: dict = {
        "steps": [],
        "verification_checks": [],
        "prerequisites": [],
        "pass_criteria": [],
        "confidence": None,
        "missing_info": [],
        "issue_key": None,
        "source": None,
    }
    step: dict | None = None
    details_lines: list[str] = []
    in_details_block = False
    current_list: str | None = None

    def flush_step() -> None:
        nonlocal step, details_lines, in_details_block
        if step is not None:
            if details_lines:
                step["details"] = "\n".join(details_lines).strip()
            target = (
                data["verification_checks"]
                if current_list == "verification_checks"
                else data["steps"]
            )
            target.append(step)
        step = None
        details_lines = []
        in_details_block = False

    def flush_list_item(text: str) -> None:
        if current_list == "prerequisites":
            data["prerequisites"].append(text.strip())
        elif current_list == "pass_criteria":
            data["pass_criteria"].append(text.strip())

    for raw in text.splitlines():
        line = raw.rstrip()

        if re.match(r"^issue_key:", line):
            flush_step()
            data["issue_key"] = line.split(":", 1)[1].strip()
            current_list = None
            continue
        if re.match(r"^source:", line):
            data["source"] = line.split(":", 1)[1].strip()
            continue
        if re.match(r"^prerequisites:", line):
            flush_step()
            current_list = "prerequisites"
            continue
        if re.match(r"^steps:", line):
            flush_step()
            current_list = "steps"
            continue
        if re.match(r"^verification_checks:", line):
            flush_step()
            current_list = "verification_checks"
            continue
        if re.match(r"^pass_criteria:", line):
            flush_step()
            current_list = "pass_criteria"
            continue
        if re.match(r"^confidence:", line):
            flush_step()
            current_list = None
            data["confidence"] = line.split(":", 1)[1].strip()
            continue
        if re.match(r"^missing_info:", line):
            flush_step()
            current_list = "missing_info"
            continue
        if re.match(r"^jira_context:", line):
            flush_step()
            current_list = None
            continue

        if current_list == "missing_info":
            m = re.match(r"^\s+-\s+(.+)$", line)
            if m:
                data["missing_info"].append(m.group(1).strip())
            continue

        if current_list in ("prerequisites", "pass_criteria"):
            m = re.match(r"^\s+-\s+\|\s*$", line)
            if m:
                in_details_block = True
                details_lines = []
                continue
            m = re.match(r"^\s+-\s+(.+)$", line)
            if m:
                if in_details_block:
                    flush_list_item("\n".join(details_lines))
                    details_lines = []
                    in_details_block = False
                flush_list_item(m.group(1))
                continue
            if in_details_block:
                details_lines.append(line.strip())
                continue

        m = re.match(r"^\s*-\s+action:\s*(.+)$", line)
        if m:
            flush_step()
            step = {"action": m.group(1).strip()}
            continue
        m = re.match(r"^\s*kind:\s*(.+)$", line)
        if m and step is not None:
            step["kind"] = m.group(1).strip()
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
    if in_details_block and current_list in ("prerequisites", "pass_criteria"):
        flush_list_item("\n".join(details_lines))

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
