"""Load MCP servers as LangChain tools."""

import os
from pathlib import Path

from ocs_ci.agents.mcp.registry import servers_for

REPO_ROOT = Path(__file__).resolve().parents[3]


async def load_tools(server_names, allow=None):
    """
    Load LangChain tools from the named MCP servers.

    Args:
        server_names (list): MCP server names declared by an agent.
        allow (list): Tool names the agent may bind. None keeps every tool
            the servers expose.

    Returns:
        list: LangChain tools for those servers.

    Raises:
        NotImplementedError: langchain-mcp-adapters is not installed.
        ValueError: An allowed tool was not provided by the servers.
    """
    connections = _prepare_connections(servers_for(server_names))
    try:
        from langchain_mcp_adapters.client import MultiServerMCPClient
    except ImportError as error:
        raise NotImplementedError(
            "MCP tool loading requires langchain-mcp-adapters in the agents extra. "
            f"Requested servers: {', '.join(server_names)}. "
            f"Allow list: {', '.join(allow or [])}"
        ) from error
    client = MultiServerMCPClient(connections)
    tools = await client.get_tools()
    if allow is None:
        return tools
    allowed = set(allow)
    selected = [tool for tool in tools if tool.name in allowed]
    missing = sorted(allowed - {tool.name for tool in selected})
    if missing:
        raise ValueError(
            "MCP servers did not provide tools: "
            + ", ".join(missing)
            + ". Available: "
            + ", ".join(sorted(tool.name for tool in tools))
        )
    return selected


def _prepare_connections(connections):
    """
    Give local MCP servers the job environment and the repository as cwd.

    Args:
        connections (dict): Connection settings from servers_for.

    Returns:
        dict: Connections with stdio processes rooted at the repository.
    """
    prepared = {}
    for name, connection in connections.items():
        if connection.get("transport") == "stdio":
            connection = {
                **connection,
                "cwd": str(REPO_ROOT),
                "env": dict(os.environ),
            }
        prepared[name] = connection
    return prepared
