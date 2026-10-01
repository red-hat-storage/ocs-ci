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
from ocs_ci.agents.runtime.dry_run import activate_dry_run, env_requests_dry_run

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
    parser.add_argument(
        "--issue",
        action="append",
        default=None,
        help="Verify only this Jira issue. Repeat for more than one. Overrides OCS_AGENT_ISSUE.",
    )
    parser.add_argument(
        "--cluster",
        default=None,
        help="Kube context where verification commands run. Overrides OCS_AGENT_CLUSTER.",
    )
    parser.add_argument(
        "--release",
        default=None,
        help="Target Release whose ON_QA bugs are retrieved. Overrides OCS_AGENT_RELEASE.",
    )
    parser.add_argument(
        "--arg",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help="Extra argument passed to the agent. Repeat for more than one.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Read and write the local verification report only. "
            "Do not update Jira or any other application. "
            "Also enabled by OCS_AGENT_DRY_RUN=1."
        ),
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
        "args": _agent_args(args, env),
        "result_file": result_file,
        "jenkins": {
            "job_name": env.get("JOB_NAME"),
            "build_number": env.get("BUILD_NUMBER"),
            "build_url": env.get("BUILD_URL"),
            "workspace": env.get("WORKSPACE"),
        },
    }


def _agent_args(args, env):
    """
    Collect named arguments from the environment and the CLI.

    OCS_AGENT_ARGS is a whitespace-separated list of KEY=VALUE pairs.
    OCS_AGENT_ISSUE is a comma-separated list of issue keys. OCS_AGENT_CLUSTER
    is one kube context. OCS_AGENT_RELEASE is a Target Release name.
    OCS_AGENT_DRY_RUN=1 is a dry run. --arg, --issue, --cluster, --release,
    and --dry-run replace those values. A dry run reads Jira and may write
    the local verification report. It does not update Jira or any other
    application.

    Args:
        args: Parsed CLI arguments.
        env (dict): Process environment.

    Returns:
        dict: Argument names and values. issues is a list when any issue was given.

    Raises:
        ValueError: A pair is not KEY=VALUE.
    """
    collected = {}
    for token in str(env.get("OCS_AGENT_ARGS") or "").split():
        key, value = _split_arg(token)
        collected[key] = value
    issues = [
        part.strip()
        for part in str(env.get("OCS_AGENT_ISSUE") or "").split(",")
        if part.strip()
    ]
    cluster = str(env.get("OCS_AGENT_CLUSTER") or "").strip()
    release = str(env.get("OCS_AGENT_RELEASE") or "").strip()
    if issues:
        collected["issues"] = issues
    if cluster:
        collected["cluster"] = cluster
    if release:
        collected["release"] = release
    for token in args.arg or []:
        key, value = _split_arg(token)
        collected[key] = value
    if args.cluster:
        collected["cluster"] = args.cluster
    if args.release:
        collected["release"] = args.release
    if args.issue:
        collected["issues"] = [part.strip() for part in args.issue if part.strip()]
    if args.dry_run or env_requests_dry_run(env) or _truthy(collected.get("dry_run")):
        collected["dry_run"] = True
    elif "dry_run" in collected:
        del collected["dry_run"]
    return collected


def _truthy(value):
    """
    Return whether an argument value turns dry-run on.

    Args:
        value: Raw argument value.

    Returns:
        bool: True for True and for 1, true, or yes.
    """
    if value is True:
        return True
    return isinstance(value, str) and value.strip().lower() in {"1", "true", "yes"}


def _split_arg(token):
    """
    Split one KEY=VALUE argument.

    Args:
        token (str): Raw pair.

    Returns:
        tuple: Key and value.

    Raises:
        ValueError: The token has no equals sign or no key.
    """
    if "=" not in token:
        raise ValueError(f"argument must be KEY=VALUE: {token}")
    key, value = token.split("=", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"argument must be KEY=VALUE: {token}")
    return key, value.strip()


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
    message = request["message"] or ""
    agent_args = request.get("args") or {}
    if agent_args:
        message = (
            f"{message.rstrip()}\n{json.dumps(agent_args, sort_keys=True)}".strip()
        )
    state = {
        "messages": [{"role": "user", "content": message}],
        "jenkins": request["jenkins"],
    }
    if agent_args:
        state["args"] = agent_args
    return state


def invoke_agent(agent_dir, request):
    """
    Compile the agent graph and run it once.

    Args:
        agent_dir (Path): Agent directory.
        request (dict): Value returned by build_request.

    Returns:
        dict: JSON-ready result with ok, the agent name, the final reply, and
            the graph state.

    Raises:
        NotImplementedError: The shared LangGraph builder is not connected yet.
    """

    async def _run():
        make_graph = load_make_graph(agent_dir)
        graph = await make_graph()
        return await graph.ainvoke(
            agent_state(request),
            config={"recursion_limit": _recursion_limit()},
        )

    with activate_dry_run(request):
        state = asyncio.run(_run())
    return {
        "ok": True,
        "agent": request["agent"],
        "jenkins": request["jenkins"],
        "reply": _final_reply(state),
        "state": _public_state(state),
    }


def _recursion_limit():
    """
    Return how many graph steps one run may take.

    OCS_AGENT_RECURSION_LIMIT overrides the default. The default covers one
    search plus a get for every ON_QA issue in a release.

    Returns:
        int: LangGraph recursion limit.
    """
    raw = os.environ.get("OCS_AGENT_RECURSION_LIMIT", "1000")
    try:
        return int(raw)
    except ValueError:
        return 1000


def _public_state(state):
    """
    Convert graph state into JSON-ready values.

    Args:
        state (dict): Value returned by the compiled graph.

    Returns:
        dict: State with messages reduced to role and content.
    """
    if not isinstance(state, dict):
        return state
    public = {}
    for key, value in state.items():
        if key == "messages":
            public[key] = [_message_dict(message) for message in value]
        else:
            public[key] = value
    return public


def _final_reply(state):
    """
    Return the text of the last graph message.

    Args:
        state (dict): Value returned by the compiled graph.

    Returns:
        str: Final assistant text, or an empty string when there is no message.
    """
    if not isinstance(state, dict):
        return ""
    messages = state.get("messages") or []
    if not messages:
        return ""
    return _message_dict(messages[-1]).get("content") or ""


def _message_dict(message):
    """
    Reduce one chat message to role and text.

    Args:
        message: A LangChain message or a role/content dict.

    Returns:
        dict: role and content.
    """
    if isinstance(message, dict):
        return {
            "role": message.get("role") or message.get("type"),
            "content": message.get("content"),
        }
    content = getattr(message, "content", "")
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text") or "")
        content = "\n".join(part for part in parts if part)
    return {
        "role": getattr(message, "type", None) or getattr(message, "role", None),
        "content": content,
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
