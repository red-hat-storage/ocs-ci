"""Console logging for an agent run.

Logs go to stderr so a Jenkins console shows them and an MCP stdio server
does not write them onto the protocol stream.
"""

import json
import logging
import os
import re
import sys

logger = logging.getLogger(__name__)

_CONFIGURED = False
_SECRET_KEY = re.compile(
    r"password|secret|token|private|api_key|authorization",
    re.IGNORECASE,
)
_LONG_INPUTS = {"report_json", "parameters_json"}


def configure_agent_logging():
    """
    Attach one stderr handler to the agent loggers.

    OCS_AGENT_LOG_LEVEL selects the level. The default is INFO. Calling this
    again in the same process does not add a second handler.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    level_name = os.environ.get("OCS_AGENT_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        level = logging.INFO
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    agent_logger = logging.getLogger("ocs_ci.agents")
    agent_logger.setLevel(level)
    agent_logger.addHandler(handler)
    agent_logger.propagate = False
    _CONFIGURED = True


def describe_run(request):
    """
    Return a one-line description of an agent run.

    Args:
        request (dict): Value returned by build_request.

    Returns:
        str: Agent, provider, and the issue, release, cluster, and dry-run
            arguments when they were set.
    """
    args = request.get("args") or {}
    provider = (
        request.get("provider") or os.environ.get("OCS_AGENT_PROVIDER") or "openai"
    )
    parts = [f"agent {request.get('agent')}", f"provider {provider}"]
    issues = args.get("issues") or []
    if issues:
        parts.append("issues " + ", ".join(str(issue) for issue in issues))
    if args.get("release"):
        parts.append(f"release {args['release']}")
    if args.get("cluster"):
        parts.append(f"cluster {args['cluster']}")
    if args.get("dry_run"):
        parts.append("dry-run")
    return ", ".join(parts)


def agent_run_callbacks():
    """
    Return the callback that logs model and tool calls for one run.

    Returns:
        list: One LangChain callback handler.
    """
    return [_handler_type()()]


def _handler_type():
    """
    Return the callback class, importing LangChain on first use.

    Returns:
        type: Callback handler class.
    """
    cached = getattr(_handler_type, "cls", None)
    if cached is not None:
        return cached
    from langchain_core.callbacks import BaseCallbackHandler

    class AgentLogHandler(BaseCallbackHandler):
        """Log each model call and tool call without secrets or full reports."""

        def __init__(self):
            super().__init__()
            self._tools = {}

        def on_chat_model_start(self, serialized, messages, *, run_id, **kwargs):
            """Log that the agent is waiting on the chat model."""
            model = _model_name(serialized)
            if model:
                logger.info(f"Calling the chat model {model}")
            else:
                logger.info("Calling the chat model")

        def on_tool_start(
            self, serialized, input_str, *, run_id, inputs=None, **kwargs
        ):
            """Log the tool name and a short copy of its arguments."""
            name = (serialized or {}).get("name") or "tool"
            self._tools[run_id] = name
            brief = _brief_inputs(inputs if inputs is not None else input_str)
            if brief:
                logger.info(f"Calling {name} {brief}")
            else:
                logger.info(f"Calling {name}")

        def on_tool_end(self, output, *, run_id, **kwargs):
            """Log a short tool result."""
            name = self._tools.pop(run_id, "tool")
            logger.info(f"{name} returned {_brief_result(output)}")

        def on_tool_error(self, error, *, run_id, **kwargs):
            """Log a tool failure."""
            name = self._tools.pop(run_id, "tool")
            logger.error(f"{name} failed: {error}")

    _handler_type.cls = AgentLogHandler
    return AgentLogHandler


def _model_name(serialized):
    """
    Return the model name from a LangChain serialized model.

    Args:
        serialized (dict): Callback serialized payload.

    Returns:
        str: Model name, or an empty string.
    """
    if not isinstance(serialized, dict):
        return ""
    kwargs = serialized.get("kwargs") or {}
    if isinstance(kwargs, dict):
        name = kwargs.get("model") or kwargs.get("model_name") or ""
        if name:
            return str(name)
    return str(serialized.get("name") or "")


def _brief_inputs(value):
    """
    Return tool arguments that are safe and short enough for one log line.

    Args:
        value: Tool inputs from the callback.

    Returns:
        str: key=value pairs, with secrets redacted and long values clipped.
    """
    if isinstance(value, str):
        parsed = _json_object(value)
        if isinstance(parsed, dict):
            value = parsed
        else:
            return _clip(value)
    if not isinstance(value, dict):
        return _clip(value)
    parts = []
    for key, item in value.items():
        if _SECRET_KEY.search(str(key)):
            parts.append(f"{key}=<redacted>")
            continue
        text = item if isinstance(item, str) else json.dumps(item, default=str)
        if key in _LONG_INPUTS:
            text = f"<{len(text)} characters>"
        parts.append(f"{key}={_clip(text, 80)}")
    return " ".join(parts)


def _brief_result(output):
    """
    Return a short description of a tool result.

    Args:
        output: Tool output from the callback.

    Returns:
        str: Saved path, issue count, or a clipped result.
    """
    content = getattr(output, "content", output)
    if isinstance(content, list):
        content = " ".join(str(part) for part in content)
    text = content if isinstance(content, str) else str(content)
    payload = _json_object(text)
    if not isinstance(payload, dict):
        return _clip(text)
    if payload.get("saved"):
        return f"saved {payload['saved']}"
    if "count" in payload:
        return f"{payload['count']} issues"
    if payload.get("key") and payload.get("status"):
        return f"{payload['key']} status {payload['status']}"
    if payload.get("key"):
        return str(payload["key"])
    if "queued" in payload:
        return f"queued {payload.get('queue_url') or payload.get('job') or ''}".rstrip()
    if "agent_online" in payload:
        return (
            f"build {payload.get('build')} {payload.get('result') or 'unknown'} "
            f"agent_online={payload.get('agent_online')}"
        )
    return _clip(text)


def _json_object(text):
    """
    Parse a JSON object.

    Args:
        text (str): Possible JSON text.

    Returns:
        dict: Parsed object, or None.
    """
    try:
        payload = json.loads(text)
    except (TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _clip(value, limit=160):
    """
    Collapse whitespace and clip a value to one log line.

    Args:
        value: Value to describe.
        limit (int): Maximum characters.

    Returns:
        str: Clipped text.
    """
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[:limit] + "..."
