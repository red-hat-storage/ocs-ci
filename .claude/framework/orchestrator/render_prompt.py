#!/usr/bin/env python3
"""Render orchestrator prompt from workflow registry."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / ".claude" / "framework" / "registry"


def _param(workflow: dict, key: str, default: str) -> str:
    val = workflow.get("params", {}).get(key, default)
    if isinstance(val, dict):
        return str(val.get("default", default))
    return str(val)


def load_workflow(workflow_id: str) -> dict:
    path = REGISTRY / "workflows" / f"{workflow_id}.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


def render(workflow: dict, odf_version: str) -> str:
    agents_path = REGISTRY / "agents.yaml"
    with agents_path.open() as f:
        agents = yaml.safe_load(f)["agents"]

    pipeline = workflow["phases"][2]["pipeline"]
    lines = [
        f"# Workflow: {workflow['name']}",
        "",
        f"**ODF version:** {odf_version}",
        f"**JIRA status:** {_param(workflow, 'jira_status', 'ON_QA')}",
        "",
        "You are the orchestrator-coordinator. Run discovery, then for each issue:",
        "",
    ]
    for step in pipeline:
        agent = step.get("agent")
        if not agent:
            hook = step.get("hook", "step")
            lines.append(f"- hook: `{hook}`")
            continue
        desc = agents.get(agent, {}).get("description", agent)
        lines.append(f"- `{agent}` — {desc}")
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
    parser.add_argument("--workflow", default="zstream-issue-verification")
    parser.add_argument("--odf-version", required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    wf = load_workflow(args.workflow)
    text = render(wf, args.odf_version)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
        print(args.out)
    else:
        print(text)


if __name__ == "__main__":
    main()
