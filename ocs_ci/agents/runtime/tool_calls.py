"""Keep OpenAI tool-call messages inside the API limit."""

import logging

logger = logging.getLogger(__name__)

OPENAI_MAX_TOOL_CALLS = 128


def limit_tool_calls_for_model(state):
    """
    Give the model a history whose tool-call arrays fit the OpenAI limit.

    OpenAI rejects an assistant message with more than 128 tool calls. The
    stored history is left as the model wrote it. Only the model input is split.

    Args:
        state (dict): Agent state. messages is the chat history.

    Returns:
        dict: llm_input_messages for the next model call.
    """
    messages = state.get("messages") if isinstance(state, dict) else None
    return {"llm_input_messages": split_tool_call_messages(list(messages or []))}


def split_tool_call_messages(messages, limit=OPENAI_MAX_TOOL_CALLS):
    """
    Split assistant messages that request more tool calls than OpenAI accepts.

    Tool results stay with the assistant message that requested them.

    Args:
        messages (list): Chat history.
        limit (int): Maximum tool calls on one assistant message.

    Returns:
        list: History safe to send to the chat API.
    """
    if limit < 1:
        raise ValueError("tool call limit must be at least 1")
    split = []
    index = 0
    while index < len(messages):
        message = messages[index]
        tool_calls = _tool_calls(message)
        if len(tool_calls) <= limit:
            split.append(message)
            index += 1
            continue
        cursor = index + 1
        pending = {call.get("id") for call in tool_calls if call.get("id")}
        results = {}
        while cursor < len(messages) and _tool_call_id(messages[cursor]) in pending:
            results[_tool_call_id(messages[cursor])] = messages[cursor]
            cursor += 1
        logger.warning(f"Splitting {len(tool_calls)} tool calls into groups of {limit}")
        first = True
        for start in range(0, len(tool_calls), limit):
            chunk = tool_calls[start : start + limit]
            split.append(_with_tool_calls(message, chunk, keep_content=first))
            first = False
            for call in chunk:
                result = results.get(call.get("id"))
                if result is not None:
                    split.append(result)
        index = cursor
    return split


def _tool_calls(message):
    """
    Return the tool calls on one message.

    Args:
        message: A LangChain message or a role/content dict.

    Returns:
        list: Tool calls, or an empty list.
    """
    calls = getattr(message, "tool_calls", None)
    if calls is None and isinstance(message, dict):
        calls = message.get("tool_calls")
    return list(calls or [])


def _tool_call_id(message):
    """
    Return the tool call id of a tool result, or None.

    Args:
        message: A LangChain message or a role/content dict.

    Returns:
        str: Tool call id, or None when the message is not a tool result.
    """
    call_id = getattr(message, "tool_call_id", None)
    if call_id is None and isinstance(message, dict):
        call_id = message.get("tool_call_id")
    return call_id or None


def _with_tool_calls(message, tool_calls, keep_content):
    """
    Copy an assistant message with one batch of tool calls.

    Args:
        message: Assistant message that requested too many tools.
        tool_calls (list): Tool calls that fit in one API message.
        keep_content (bool): Keep the original text on the first batch only.

    Returns:
        object: Assistant message whose tool_calls are the batch.
    """
    copied = message.model_copy(deep=True)
    copied.tool_calls = list(tool_calls)
    copied.invalid_tool_calls = []
    extra = dict(copied.additional_kwargs or {})
    extra.pop("tool_calls", None)
    copied.additional_kwargs = extra
    if not keep_content:
        copied.content = ""
        copied.id = None
    return copied
