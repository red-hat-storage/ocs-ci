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
from workflow_registry import load_workflow  # noqa: E402


def _param(workflow: dict, key: str, default: str) -> str:
    val = workflow.get("params", {}).get(key, default)
    if isinstance(val, dict):
        return str(val.get("default", default))
    return str(val)


def render(workflow: dict, odf_version: str, *, dry_run: bool = False) -> str:
    agents_path = REGISTRY / "agents.yaml"
    with agents_path.open() as f:
        agents = yaml.safe_load(f)["agents"]

    pipeline = workflow["phases"][2]["pipeline"]
    wf_id = workflow.get("id", "unknown")
    lines = [
        f"# Workflow: {workflow['name']}",
        "",
        f"**Workflow ID:** `{wf_id}`",
        f"**Registry:** `.claude/framework/registry/workflows/{wf_id}.yaml`",
        f"**ODF version:** {odf_version}",
        f"**JIRA status:** {_param(workflow, 'jira_status', 'ON_QA')}",
        f"**Mode:** {'DRY-RUN (no JIRA/GitHub writes)' if dry_run else 'LIVE'}",
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
    if dry_run:
        lines.extend(
            [
                "",
                "## DRY-RUN rules",
                "",
                "- **Run:** discovery, analysis, cluster-compat, repro, scripts, safety hook,",
                "  verification-execution, cluster-health, infra-diagnosis, local reporting.",
                "- **Skip:** `jira_comment_add`, `jira_workflow_transition`, label changes,",
                "  `github` issue create/update. Write drafts under `artifacts/{KEY}/planned-actions/`.",
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
    parser.add_argument("--workflow", default="zstream-issue-verification")
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
