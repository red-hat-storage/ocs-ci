"""Build the standard LangGraph tool loop for one agent folder."""

import logging
from pathlib import Path

from ocs_ci.agents.mcp.client import load_tools
from ocs_ci.agents.runtime.discover import load_agent_spec
from ocs_ci.agents.runtime.llm import get_chat_model
from ocs_ci.agents.runtime.tool_calls import limit_tool_calls_for_model

logger = logging.getLogger(__name__)


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
        try:
            from typing import Annotated

            from langgraph.graph.message import add_messages
            from langgraph.managed import RemainingSteps
            from langgraph.prebuilt import create_react_agent
            from typing_extensions import NotRequired, TypedDict
        except ImportError as error:
            raise NotImplementedError(
                f"LangGraph runtime for {spec['name']} requires langgraph, langchain, "
                "and langchain-mcp-adapters in the agents extra"
            ) from error

        class AgentRunState(TypedDict):
            messages: Annotated[list, add_messages]
            remaining_steps: NotRequired[RemainingSteps]
            jenkins: NotRequired[dict]
            args: NotRequired[dict]
            llm_input_messages: NotRequired[list]

        tools = await load_tools(spec["mcp_servers"], spec["tools"]["allow"])
        logger.info(
            f"Loaded {len(tools)} tools for {spec['name']}: "
            + ", ".join(tool.name for tool in tools)
        )
        prompt = (agent_dir / spec["prompt"]).read_text(encoding="utf-8")
        return create_react_agent(
            get_chat_model(),
            tools,
            prompt=prompt,
            state_schema=AgentRunState,
            pre_model_hook=limit_tool_calls_for_model,
        )

    return make_graph
