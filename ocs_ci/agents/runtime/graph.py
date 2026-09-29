"""Build the standard LangGraph tool loop for one agent folder."""

from pathlib import Path

from ocs_ci.agents.runtime.discover import load_agent_spec


def make_agent_graph(graph_file):
    """
    Return the async LangGraph entrypoint for the agent beside graph_file.

    Copy this call into a new agent's graph.py. The shared builder reads that
    folder's agent.yaml and prompt.md, loads its MCP tools, and compiles the
    StateGraph.

    Args:
        graph_file (str): __file__ from the agent's graph.py.

    Returns:
        callable: Async make_graph entrypoint named in langgraph.json.
    """
    agent_dir = Path(graph_file).resolve().parent

    async def make_graph():
        spec = load_agent_spec(agent_dir)
        raise NotImplementedError(
            f"LangGraph runtime for {spec['name']} requires langgraph, langchain, "
            "and langchain-mcp-adapters in the agents extra"
        )

    return make_graph
