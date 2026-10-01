"""Check that every agent folder can be discovered and registered."""

import importlib.util
import json

import pytest

from ocs_ci.agents.mcp.servers import SERVER_TOOLS, cluster, jira, reportportal
from ocs_ci.agents.runtime import discover

REQUIRED_FILES = discover.REQUIRED_FILES


def _agent_dirs():
    return list(discover.iter_agent_dirs())


def test_server_catalog_matches_tool_modules():
    """The package catalog stays aligned with each server module."""
    assert SERVER_TOOLS["jira"] == jira.TOOL_NAMES
    assert SERVER_TOOLS["cluster"] == cluster.TOOL_NAMES
    assert SERVER_TOOLS["reportportal"] == reportportal.TOOL_NAMES


def test_template_is_not_registered():
    """The copy-me template stays out of langgraph.json."""
    names = [path.name for path in _agent_dirs()]
    assert "_template" not in names
    assert "jira_verification" in names


def test_template_has_required_files():
    """Copying _template gives a new agent its yaml, prompt, and graph."""
    template = discover.AGENTS_ROOT / "_template"
    for name in REQUIRED_FILES:
        assert (template / name).is_file()
    discover.load_agent_spec(template)


@pytest.mark.parametrize("agent_dir", _agent_dirs(), ids=lambda path: path.name)
def test_agent_folder_loads(agent_dir):
    """Each agent folder has a valid spec and a make_graph entrypoint."""
    spec = discover.load_agent_spec(agent_dir)
    assert spec["name"] == agent_dir.name
    module_spec = importlib.util.spec_from_file_location(
        f"{agent_dir.name}_graph", agent_dir / "graph.py"
    )
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    assert callable(module.make_graph)


def test_jira_verification_uses_only_the_jira_api():
    """The Jira API retrieves each issue and supplies the report text."""
    spec = discover.load_agent_spec(discover.AGENTS_ROOT / "jira_verification")
    assert spec["mcp_servers"] == ["jira"]
    assert spec["tools"]["allow"] == [
        "jira_search_issues",
        "jira_get_issue",
        "jira_save_verification_report",
    ]


def test_rovo_tools_are_allowed_without_a_local_catalog(tmp_path):
    """A remote Rovo tool name is valid because Atlassian owns that catalog."""
    agent_dir = tmp_path / "rovo_lookup"
    agent_dir.mkdir()
    (agent_dir / "prompt.md").write_text("Look up the issue.\n", encoding="utf-8")
    (agent_dir / "graph.py").write_text("make_graph = None\n", encoding="utf-8")
    (agent_dir / "agent.yaml").write_text(
        "name: rovo_lookup\n"
        "description: Look up an issue through Rovo\n"
        "mcp_servers:\n"
        "  - rovo\n"
        "tools:\n"
        "  allow:\n"
        "    - search\n"
        "prompt: prompt.md\n",
        encoding="utf-8",
    )
    spec = discover.load_agent_spec(agent_dir)
    assert spec["mcp_servers"] == ["rovo"]
    assert spec["tools"]["allow"] == ["search"]


def test_langgraph_json_matches_discovered_agents():
    """langgraph.json lists the same graphs discovery would write."""
    written = json.loads(discover.LANGGRAPH_JSON.read_text(encoding="utf-8"))
    assert written == discover.build_langgraph_config()
