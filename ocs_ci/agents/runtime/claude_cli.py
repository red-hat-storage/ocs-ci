"""Chat model that calls the local Claude CLI, including tool calls."""

import json
import subprocess
import uuid

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult


class ClaudeCliChat(BaseChatModel):
    """
    Claude Code print mode as a LangChain chat model.

    With use_vertex, the CLI authenticates to Vertex from
    agents_credentials.claude_code. Otherwise the CLI is already authenticated
    on the machine. Each turn asks for either tool calls or a final reply as JSON.
    """

    model_name: str = ""
    use_vertex: bool = False

    @property
    def _llm_type(self):
        return "claude-cli"

    def bind_tools(self, tools, *, tool_choice=None, **kwargs):
        """
        Attach tools for the next model call.

        Args:
            tools (list): LangChain tools the agent may call.
            tool_choice (str): Unused. Claude chooses from the provided tools.

        Returns:
            Runnable: This model with the tools bound.
        """
        return self.bind(tools=list(tools), **kwargs)

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        """
        Send the conversation to Claude and parse a tool call or final reply.

        Args:
            messages (list): LangChain messages for this turn.
            stop (list): Unused stop sequences.
            run_manager: LangChain callback manager.

        Returns:
            ChatResult: One assistant message.
        """
        prompt = _conversation_prompt(messages, kwargs.get("tools") or [])
        text = _run_claude(prompt, self.model_name, vertex=self.use_vertex)
        return ChatResult(
            generations=[ChatGeneration(message=parse_agent_message(text))]
        )


def parse_agent_message(text):
    """
    Turn Claude's JSON reply into an assistant message.

    Args:
        text (str): Raw CLI stdout.

    Returns:
        AIMessage: Tool calls, or the final reply text.
    """
    payload = _json_object(text)
    tool_calls = []
    if isinstance(payload, dict):
        raw_calls = payload.get("tool_calls")
        if isinstance(raw_calls, dict):
            raw_calls = [raw_calls]
        if isinstance(raw_calls, list):
            for call in raw_calls:
                if not isinstance(call, dict) or not call.get("name"):
                    continue
                args = call.get("args") if isinstance(call.get("args"), dict) else {}
                tool_calls.append(
                    {
                        "name": call["name"],
                        "args": args,
                        "id": call.get("id") or f"call_{uuid.uuid4().hex[:12]}",
                        "type": "tool_call",
                    }
                )
        if tool_calls:
            return AIMessage(content="", tool_calls=tool_calls)
        final = payload.get("final")
        if isinstance(final, str):
            return AIMessage(content=final)
        if isinstance(final, (dict, list)):
            return AIMessage(content=json.dumps(final))
    return AIMessage(content=(text or "").strip())


def _conversation_prompt(messages, tools):
    """
    Build the print-mode prompt for one agent turn.

    Args:
        messages (list): Conversation so far.
        tools (list): Tools the agent may call.

    Returns:
        str: Prompt text.
    """
    tool_specs = []
    for tool in tools:
        name = getattr(tool, "name", None) or tool.get("name")
        description = (
            getattr(tool, "description", None) or tool.get("description") or ""
        )
        args = getattr(tool, "args", None) or tool.get("args") or {}
        tool_specs.append({"name": name, "description": description, "args": args})
    lines = [
        "Reply with one JSON object and no other text.",
        'To call tools: {"tool_calls":[{"name":"<tool>","args":{}}]}',
        'When the task is finished: {"final":"<reply>"}',
        "",
        "Tools:",
        json.dumps(tool_specs),
        "",
        "Conversation:",
    ]
    for message in messages:
        lines.append(f"{_role(message)}: {_content(message)}")
    return "\n".join(lines)


def _run_claude(prompt, model_name, vertex=False):
    """
    Run Claude print mode and return stdout.

    Args:
        prompt (str): Full prompt, passed on stdin.
        model_name (str): Optional --model value.
        vertex (bool): Authenticate with the Claude Code service account.

    Returns:
        str: CLI stdout.

    Raises:
        RuntimeError: The CLI exits non-zero.
    """
    command = ["claude", "-p", "--output-format", "text"]
    if model_name:
        command.extend(["--model", model_name])
    env = None
    if vertex:
        from ocs_ci.agents.runtime.claude_code import claude_code_environment

        env = claude_code_environment()
    completed = subprocess.run(
        command,
        input=prompt,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
        env=env,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"claude exited {completed.returncode}: {detail}")
    return completed.stdout


def _role(message):
    if isinstance(message, dict):
        return message.get("role") or message.get("type") or "message"
    return getattr(message, "type", None) or getattr(message, "role", None) or "message"


def _content(message):
    if isinstance(message, dict):
        content = message.get("content")
    else:
        content = getattr(message, "content", "")
    if isinstance(content, list):
        return json.dumps(content)
    return "" if content is None else str(content)


def _json_object(text):
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        lines = [
            line for line in cleaned.splitlines() if not line.strip().startswith("```")
        ]
        cleaned = "\n".join(lines).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end <= start:
            return None
        try:
            return json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError:
            return None
