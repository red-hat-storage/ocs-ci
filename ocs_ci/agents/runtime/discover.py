"""Find agent folders and write the LangGraph graph registry."""

import json
from pathlib import Path

import yaml

from ocs_ci.agents.mcp.registry import servers_for
from ocs_ci.agents.mcp.servers import SERVER_TOOLS

AGENTS_ROOT = Path(__file__).resolve().parents[1]
LANGGRAPH_JSON = AGENTS_ROOT / "langgraph.json"
SKIP_DIRS = {
    "runtime",
    "mcp",
    "workflows",
    "tests",
    "__pycache__",
}
REQUIRED_FILES = ("agent.yaml", "prompt.md", "graph.py")
REQUIRED_KEYS = ("name", "description", "mcp_servers", "tools", "prompt")


def iter_agent_dirs(root=None):
    """
    Yield agent directories that contain an agent.yaml file.

    Args:
        root (Path): Directory to scan. Defaults to ocs_ci/agents.

    Yields:
        Path: Agent directory. Folders whose names start with "_" are skipped
            so the template is not registered.
    """
    root = Path(root) if root else AGENTS_ROOT
    for path in sorted(root.iterdir()):
        if not path.is_dir():
            continue
        if path.name.startswith("_") or path.name in SKIP_DIRS:
            continue
        if (path / "agent.yaml").is_file():
            yield path


def load_agent_spec(agent_dir):
    """
    Load and check one agent folder.

    Args:
        agent_dir (Path): Directory that holds agent.yaml, prompt.md, and graph.py.

    Returns:
        dict: Parsed agent.yaml contents.

    Raises:
        FileNotFoundError: A required file or the prompt path is missing.
        ValueError: agent.yaml is missing required keys or names an unknown tool.
    """
    agent_dir = Path(agent_dir)
    for name in REQUIRED_FILES:
        required = agent_dir / name
        if not required.is_file():
            raise FileNotFoundError(required)
    with open(agent_dir / "agent.yaml", encoding="utf-8") as handle:
        spec = yaml.safe_load(handle)
    if not isinstance(spec, dict):
        raise ValueError(f"{agent_dir / 'agent.yaml'} must be a mapping")
    missing = [key for key in REQUIRED_KEYS if key not in spec]
    if missing:
        raise ValueError(
            f"{agent_dir.name} agent.yaml missing keys: {', '.join(missing)}"
        )
    tools = spec["tools"]
    if not isinstance(tools, dict) or not isinstance(tools.get("allow"), list):
        raise ValueError(f"{agent_dir.name} agent.yaml needs tools.allow as a list")
    prompt = agent_dir / spec["prompt"]
    if not prompt.is_file():
        raise FileNotFoundError(prompt)
    _validate_tool_allowlist(agent_dir.name, spec)
    return spec


def build_langgraph_config(root=None):
    """
    Build the langgraph.json document from discovered agents.

    Args:
        root (Path): Directory to scan. Defaults to ocs_ci/agents.

    Returns:
        dict: LangGraph config whose graphs map each agent name to make_graph.
    """
    graphs = {}
    for agent_dir in iter_agent_dirs(root):
        spec = load_agent_spec(agent_dir)
        graphs[spec["name"]] = f"./{agent_dir.name}/graph.py:make_graph"
    return {"dependencies": ["."], "graphs": graphs}


def render_langgraph_config(config):
    """
    Serialize a LangGraph config.

    Args:
        config (dict): Value returned by build_langgraph_config.

    Returns:
        str: JSON text with a trailing newline.
    """
    return json.dumps(config, indent=2) + "\n"


def write_langgraph_json(root=None):
    """
    Rewrite langgraph.json from the agent folders on disk.

    Args:
        root (Path): Directory to scan. Defaults to ocs_ci/agents.

    Returns:
        Path: The written langgraph.json path.
    """
    root = Path(root) if root else AGENTS_ROOT
    target = root / "langgraph.json"
    target.write_text(
        render_langgraph_config(build_langgraph_config(root)), encoding="utf-8"
    )
    return target


def _validate_tool_allowlist(agent_name, spec):
    """
    Check that every allowed tool belongs to a server this agent connects to.

    Args:
        agent_name (str): Agent directory or spec name, used in the error.
        spec (dict): Parsed agent.yaml contents.

    Raises:
        ValueError: An MCP server or tool name is not registered.
    """
    try:
        connections = servers_for(spec["mcp_servers"])
    except KeyError as error:
        raise ValueError(
            f"{agent_name} names an unknown MCP server: {error}"
        ) from error
    selected_local_tools = set()
    has_remote = False
    for server_name, connection in connections.items():
        if connection.get("transport") == "stdio":
            selected_local_tools.update(SERVER_TOOLS[server_name])
        else:
            has_remote = True
    all_local_tools = set()
    for tool_names in SERVER_TOOLS.values():
        all_local_tools.update(tool_names)
    unknown = []
    for tool_name in spec["tools"]["allow"]:
        if tool_name in selected_local_tools:
            continue
        if tool_name in all_local_tools or not has_remote:
            unknown.append(tool_name)
    if unknown:
        raise ValueError(
            f"{agent_name} allows unknown tools: {', '.join(unknown)}. "
            f"Known tools for its local servers: {', '.join(sorted(selected_local_tools))}"
        )


def main():
    """Write langgraph.json for the agents in this package."""
    target = write_langgraph_json()
    print(target)


if __name__ == "__main__":
    main()
