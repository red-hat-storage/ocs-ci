"""Run one ODF agent from a Jenkins job or a shell."""

import argparse
import asyncio
import importlib.util
import json
import logging
import os
import sys
from pathlib import Path

from ocs_ci.agents.runtime.discover import iter_agent_dirs, load_agent_spec

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_AGENT_ERROR = 1
EXIT_USAGE = 2


def build_request(argv, env):
    """
    Read the agent run from CLI flags and the Jenkins job environment.

    Args:
        argv (list): Arguments after the program name. Jenkins may pass none
            and set OCS_AGENT_NAME and OCS_AGENT_MESSAGE instead.
        env (dict): Process environment. JOB_NAME, BUILD_NUMBER, BUILD_URL,
            and WORKSPACE are recorded for the agent and the archived result.

    Returns:
        dict: Agent name, user message, result path, and Jenkins build fields.

    Raises:
        ValueError: No agent name was given.
    """
    parser = argparse.ArgumentParser(
        description="Run one ODF agent. A Jenkins job can pass the agent and message as environment variables."
    )
    parser.add_argument(
        "--agent", default=None, help="Agent directory name. Overrides OCS_AGENT_NAME."
    )
    parser.add_argument(
        "--message",
        default=None,
        help="Text sent to the agent. Overrides OCS_AGENT_MESSAGE.",
    )
    parser.add_argument(
        "--result-file",
        default=None,
        help="JSON result path. Overrides OCS_AGENT_RESULT_FILE. Defaults to WORKSPACE/agent-result.json.",
    )
    args = parser.parse_args(argv)
    agent = args.agent or env.get("OCS_AGENT_NAME")
    if not agent:
        raise ValueError("agent name is required via --agent or OCS_AGENT_NAME")
    if args.message is None:
        message = env.get("OCS_AGENT_MESSAGE", "")
    else:
        message = args.message
    result_file = args.result_file or env.get("OCS_AGENT_RESULT_FILE")
    if not result_file and env.get("WORKSPACE"):
        result_file = str(Path(env["WORKSPACE"]) / "agent-result.json")
    return {
        "agent": agent,
        "message": message,
        "result_file": result_file,
        "jenkins": {
            "job_name": env.get("JOB_NAME"),
            "build_number": env.get("BUILD_NUMBER"),
            "build_url": env.get("BUILD_URL"),
            "workspace": env.get("WORKSPACE"),
        },
    }


def resolve_agent_dir(name, root=None):
    """
    Find a registered agent directory by name.

    Args:
        name (str): Agent directory name, such as jira_verification.
        root (Path): Directory to scan. Defaults to ocs_ci/agents.

    Returns:
        Path: The agent directory.

    Raises:
        LookupError: No registered agent uses that name.
    """
    matches = [path for path in iter_agent_dirs(root) if path.name == name]
    if not matches:
        known = ", ".join(path.name for path in iter_agent_dirs(root)) or "(none)"
        raise LookupError(f"unknown agent {name}. Known agents: {known}")
    return matches[0]


def load_make_graph(agent_dir):
    """
    Load the make_graph callable from an agent folder.

    Args:
        agent_dir (Path): Agent directory that contains graph.py.

    Returns:
        callable: Async factory that returns a compiled LangGraph.
    """
    graph_file = Path(agent_dir) / "graph.py"
    module_spec = importlib.util.spec_from_file_location(
        f"{Path(agent_dir).name}_graph", graph_file
    )
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module.make_graph


def agent_state(request):
    """
    Build the graph input for one Jenkins run.

    Args:
        request (dict): Value returned by build_request.

    Returns:
        dict: Messages plus the Jenkins build fields.
    """
    return {
        "messages": [{"role": "user", "content": request["message"]}],
        "jenkins": request["jenkins"],
    }


def invoke_agent(agent_dir, request):
    """
    Compile the agent graph and run it once.

    Args:
        agent_dir (Path): Agent directory.
        request (dict): Value returned by build_request.

    Returns:
        dict: JSON-ready result with ok, the agent name, and the graph state.

    Raises:
        NotImplementedError: The shared LangGraph builder is not connected yet.
    """
    make_graph = load_make_graph(agent_dir)
    graph = asyncio.run(make_graph())
    state = asyncio.run(graph.ainvoke(agent_state(request)))
    return {
        "ok": True,
        "agent": request["agent"],
        "jenkins": request["jenkins"],
        "state": state,
    }


def main(argv=None, env=None, invoke=None):
    """
    Run one agent and write a JSON result for Jenkins to archive.

    Args:
        argv (list): Arguments after the program name. Defaults to sys.argv.
        env (dict): Environment mapping. Defaults to os.environ.
        invoke (callable): Replacement for invoke_agent, used by tests.

    Returns:
        int: 0 when the agent result is ok, 1 when the agent fails, 2 when the
            agent name is missing or unknown.
    """
    if argv is None:
        argv = sys.argv[1:]
    if env is None:
        env = os.environ
    _ensure_console_logging()
    try:
        request = build_request(argv, env)
    except ValueError as exc:
        _emit({"ok": False, "error": str(exc)}, None)
        return EXIT_USAGE
    try:
        agent_dir = resolve_agent_dir(request["agent"])
        load_agent_spec(agent_dir)
        runner = invoke or invoke_agent
        result = runner(agent_dir, request)
        result.setdefault("ok", True)
        code = EXIT_OK if result["ok"] else EXIT_AGENT_ERROR
    except LookupError as exc:
        result = {"ok": False, "agent": request["agent"], "error": str(exc)}
        code = EXIT_USAGE
    except Exception as exc:
        logger.error("Agent %s failed: %s", request["agent"], exc)
        result = {
            "ok": False,
            "agent": request["agent"],
            "error": str(exc),
            "jenkins": request["jenkins"],
        }
        code = EXIT_AGENT_ERROR
    _emit(result, request.get("result_file"))
    return code


def _ensure_console_logging():
    """Show agent logs in the Jenkins console when the framework logger is not configured."""
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def _emit(result, result_file):
    """
    Print the result and, when a path is set, write it for Jenkins to archive.

    Args:
        result (dict): JSON-ready agent result.
        result_file (str): Destination path, or None to print only.
    """
    payload = json.dumps(result, default=str)
    print(payload)
    if result_file:
        path = Path(result_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n", encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
