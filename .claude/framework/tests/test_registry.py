"""Registry sanity checks."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / ".claude" / "framework" / "registry"


def test_all_agents_have_markdown():
    with (REGISTRY / "agents.yaml").open() as f:
        agents = yaml.safe_load(f)["agents"]
    for agent_id, meta in agents.items():
        path = ROOT / meta["path"]
        assert path.is_file(), f"missing agent file for {agent_id}: {path}"


def test_workflow_references_known_agents():
    with (REGISTRY / "agents.yaml").open() as f:
        known = set(yaml.safe_load(f)["agents"])
    with (REGISTRY / "workflows" / "zstream-issue-verification.yaml").open() as f:
        wf = yaml.safe_load(f)
    for step in wf["phases"][2]["pipeline"]:
        agent = step.get("agent")
        if not agent:
            continue
        assert agent in known
