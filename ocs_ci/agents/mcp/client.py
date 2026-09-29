"""Load MCP servers as LangChain tools."""

from ocs_ci.agents.mcp.registry import servers_for


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
        NotImplementedError: langchain-mcp-adapters is not connected yet.
    """
    servers_for(server_names)
    raise NotImplementedError(
        "MCP tool loading requires langchain-mcp-adapters in the agents extra. "
        f"Requested servers: {', '.join(server_names)}. "
        f"Allow list: {', '.join(allow or [])}"
    )
