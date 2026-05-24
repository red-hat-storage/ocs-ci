#!/usr/bin/env python3
"""Render orchestrator prompt from workflow registry."""

from __future__ import annotations

import argparse
from pathlib import Path

import sys

import yaml

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / ".claude" / "framework" / "registry"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))
from workflow_registry import (  # noqa: E402
    DEFAULT_WORKFLOW,
    load_workflow,
    workflow_param,
)


def _find_pipeline(workflow: dict) -> list[dict]:
    """Find the ``per_issue`` phase pipeline by id, not by index."""
    for phase in workflow.get("phases", []):
        if phase.get("id") == "per_issue":
            return phase.get("pipeline", [])
    return []


def _collect_step_names(workflow: dict) -> list[str]:
    """Collect all agent/hook names from every phase for DRY-RUN listing."""
    names: list[str] = []
    for phase in workflow.get("phases", []):
        if phase.get("agent"):
            names.append(phase["agent"])
        for step in phase.get("pipeline", []):
            if step.get("agent"):
                names.append(step["agent"])
            elif step.get("hook"):
                names.append(Path(step["hook"]).stem)
    return names


def render(workflow: dict, version: str, *, dry_run: bool = False) -> str:
    agents_path = REGISTRY / "agents.yaml"
    with agents_path.open() as f:
        agents = yaml.safe_load(f)["agents"]

    pipeline = _find_pipeline(workflow)
    wf_id = workflow.get("id", "unknown")
    jira_status = workflow_param(workflow, "jira_status", "")
    lines = [
        f"# Workflow: {workflow['name']}",
        "",
        f"**Workflow ID:** `{wf_id}`",
        f"**Registry:** `.claude/framework/registry/workflows/{wf_id}.yaml`",
        f"**Version:** {version}",
    ]
    if jira_status:
        lines.append(f"**JIRA status:** {jira_status}")
    lines.extend(
        [
            f"**Mode:** {'DRY-RUN (no JIRA/GitHub writes)' if dry_run else 'LIVE'}",
            "",
            "You are the orchestrator-coordinator. Run discovery, then for each issue:",
            "",
        ]
    )
    for step in pipeline:
        agent = step.get("agent")
        if not agent:
            hook = step.get("hook", "step")
            lines.append(f"- hook: `{hook}`")
            continue
        desc = agents.get(agent, {}).get("description", agent)
        lines.append(f"- `{agent}` — {desc}")
    if dry_run:
        step_names = _collect_step_names(workflow)
        lines.extend(
            [
                "",
                "## DRY-RUN rules",
                "",
                f"- **Run:** {', '.join(step_names) or 'all pipeline steps'}.",
                "- **Skip:** JIRA/GitHub write operations (comments, transitions, labels,",
                "  issue create/update). Write drafts under `artifacts/{KEY}/planned-actions/`.",
                "- Set `dry_run: true` on every `outcomes/{KEY}.json`.",
            ]
        )
    lines.extend(
        [
            "",
            "Read `.claude/agents/orchestrator-coordinator.md` for escalation and outputs.",
            "Workspace: `$JIRA_AGENT_WORKSPACE`",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    parser.add_argument("--odf-version", required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render prompt for dry-run (no JIRA/GitHub mutations)",
    )
    args = parser.parse_args()

    wf = load_workflow(args.workflow)
    text = render(wf, args.odf_version, dry_run=args.dry_run)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
        print(args.out)
    else:
        print(text)


if __name__ == "__main__":
    main()
