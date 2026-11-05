"""Keep OpenAI tool-call messages inside the API limit."""

import copy
import json
import logging

logger = logging.getLogger(__name__)

OPENAI_MAX_TOOL_CALLS = 128
# gpt-4o accepts 128000 tokens, including the agent prompt and tool schemas
# that are added after this hook. The history sent to the model stays lower.
OPENAI_INPUT_TOKEN_BUDGET = 100_000
_ISSUE_JSON_CHARS = 8_000
_ENCODER = None


def limit_tool_calls_for_model(state):
    """
    Give the model a history that fits the OpenAI tool-call and context limits.

    OpenAI rejects an assistant message with more than 128 tool calls, and
    gpt-4o rejects a request above 128000 tokens. Finished Jira issue bodies
    are omitted from the model input. The stored history is left unchanged.

    Args:
        state (dict): Agent state. messages is the chat history.

    Returns:
        dict: llm_input_messages for the next model call.
    """
    messages = state.get("messages") if isinstance(state, dict) else None
    copied = [_copy_message(message) for message in list(messages or [])]
    return {"llm_input_messages": fit_model_input(split_tool_call_messages(copied))}


def fit_model_input(messages, budget=OPENAI_INPUT_TOKEN_BUDGET):
    """
    Omit Jira text the model no longer needs and keep the rest under budget.

    A saved issue's jira_get_issue body and report_json are replaced with
    short markers. The save result, including the report path, is kept. An
    issue that is still being read is clipped to the fields the report uses.

    Args:
        messages (list): Chat history copied for the model call.
        budget (int): Maximum tokens for that history.

    Returns:
        list: History that can be sent to gpt-4o.
    """
    calls = _calls_by_id(messages)
    saved_keys, finished_save_ids = _saved_issues(messages, calls)
    if saved_keys:
        logger.info(
            "Omitting saved Jira issue text from the model input "
            f"({len(saved_keys)})"
        )
    for message in messages:
        _omit_saved_report(message, finished_save_ids)
        _shrink_issue_result(message, calls, saved_keys)
    _enforce_token_budget(messages, calls, budget)
    return messages


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


def _copy_message(message):
    """
    Copy one chat message so the stored history stays unchanged.

    Args:
        message: A LangChain message or a role/content dict.

    Returns:
        object: A deep copy.
    """
    if isinstance(message, dict):
        return copy.deepcopy(message)
    return message.model_copy(deep=True)


def _calls_by_id(messages):
    """
    Index tool calls by id.

    Args:
        messages (list): Chat history.

    Returns:
        dict: Tool call id to the tool call dict.
    """
    calls = {}
    for message in messages:
        for call in _tool_calls(message):
            call_id = call.get("id")
            if call_id:
                calls[call_id] = call
    return calls


def _saved_issues(messages, calls):
    """
    Find issues whose verification report was saved.

    Args:
        messages (list): Chat history.
        calls (dict): Tool call id to tool call.

    Returns:
        tuple: Issue keys that were saved, and the save tool-call ids.
    """
    saved = set()
    finished = set()
    for message in messages:
        call_id = _tool_call_id(message)
        if not call_id:
            continue
        call = calls.get(call_id) or {}
        name = _message_name(message) or call.get("name")
        if name != "jira_save_verification_report":
            continue
        finished.add(call_id)
        payload = _json_object(_content(message))
        if not isinstance(payload, dict) or not payload.get("saved"):
            continue
        key = payload.get("key") or (call.get("args") or {}).get("issue_key")
        if key:
            saved.add(str(key))
    return saved, finished


def _omit_saved_report(message, finished_save_ids):
    """
    Drop report_json from a save call whose result is already in the history.

    Args:
        message: One chat message.
        finished_save_ids (set): Save tool-call ids that have a result.
    """
    calls = _tool_calls(message)
    if not calls:
        return
    updated = []
    changed = False
    for call in calls:
        copied = dict(call)
        args = dict(copied.get("args") or {})
        if (
            copied.get("name") == "jira_save_verification_report"
            and copied.get("id") in finished_save_ids
            and args.get("report_json")
        ):
            args["report_json"] = "{}"
            copied["args"] = args
            changed = True
        updated.append(copied)
    if changed:
        _set_tool_calls(message, updated)


def _shrink_issue_result(message, calls, saved_keys):
    """
    Replace a finished issue body and clip an issue that is still being read.

    Args:
        message: One chat message.
        calls (dict): Tool call id to tool call.
        saved_keys (set): Issue keys whose reports were saved.
    """
    call_id = _tool_call_id(message)
    if not call_id:
        return
    call = calls.get(call_id) or {}
    name = _message_name(message) or call.get("name")
    if name != "jira_get_issue":
        return
    payload = _json_object(_content(message))
    if not isinstance(payload, dict):
        _set_content(message, _clip_text(_content(message), _ISSUE_JSON_CHARS))
        return
    key = str(payload.get("key") or (call.get("args") or {}).get("issue_key") or "")
    if key and key in saved_keys:
        _set_content(message, json.dumps({"key": key, "omitted": "already saved"}))
        return
    _set_content(message, _bounded_issue_json(payload, _ISSUE_JSON_CHARS))


def _enforce_token_budget(messages, calls, budget):
    """
    Stub the oldest large tool results until the history fits.

    The newest issue read is kept until nothing else is left to remove.

    Args:
        messages (list): Model input, modified in place.
        calls (dict): Tool call id to tool call.
        budget (int): Maximum tokens.
    """
    if _token_count(messages) <= budget:
        return
    logger.warning(f"Model input exceeds {budget} tokens; omitting older tool results")
    get_indexes = []
    other_indexes = []
    for index, message in enumerate(messages):
        call_id = _tool_call_id(message)
        if not call_id:
            continue
        call = calls.get(call_id) or {}
        name = _message_name(message) or call.get("name")
        if name == "jira_get_issue":
            get_indexes.append(index)
        else:
            other_indexes.append(index)
    for index in get_indexes[:-1]:
        _stub_tool_message(messages[index])
        if _token_count(messages) <= budget:
            return
    for index in other_indexes:
        _stub_tool_message(messages[index])
        if _token_count(messages) <= budget:
            return
    if get_indexes:
        _stub_tool_message(messages[get_indexes[-1]])


def _stub_tool_message(message):
    """
    Replace a large tool result with the issue key.

    Args:
        message: Tool result message.
    """
    text = _content(message)
    if len(text) <= 500:
        return
    payload = _json_object(text)
    key = ""
    if isinstance(payload, dict) and payload.get("key"):
        key = str(payload["key"])
    if key:
        _set_content(message, json.dumps({"key": key, "omitted": "context limit"}))
        return
    _set_content(message, _clip_text(text, 200))


def _bounded_issue_json(payload, max_chars):
    """
    Return one issue as JSON no longer than max_chars.

    Comments are omitted. verification_notes already contain the comments
    that change how the fix is checked. Long strings are shortened from the
    end so the start of the description remains. When the JSON still does
    not fit, the key and the start of the description are kept.

    Args:
        payload (dict): jira_get_issue JSON.
        max_chars (int): Maximum length of the returned JSON.

    Returns:
        str: JSON for the model.
    """
    value = _prepare_issue(payload)
    text = json.dumps(value, ensure_ascii=False)
    for _ in range(40):
        if len(text) <= max_chars:
            return text
        if not _shrink_longest_string(value):
            break
        text = json.dumps(value, ensure_ascii=False)
    if len(text) <= max_chars:
        return text
    return json.dumps(
        {
            "key": str(payload.get("key") or ""),
            "description": str(payload.get("description") or "")[:1500],
        },
        ensure_ascii=False,
    )


def _prepare_issue(payload):
    """
    Drop raw comments and cap nested issue lists.

    Args:
        payload (dict): Issue JSON.

    Returns:
        dict: Issue text the model uses to write a report.
    """
    value = {}
    for key, item in payload.items():
        if key == "comments":
            continue
        if key == "verification_notes" and isinstance(item, list):
            value[key] = item[:8]
        elif key == "source_issues" and isinstance(item, list):
            value[key] = [
                _prepare_issue(source) if isinstance(source, dict) else source
                for source in item[:3]
            ]
        else:
            value[key] = item
    return value


def _shrink_longest_string(value):
    """
    Cut the longest string in a JSON value in half.

    Args:
        value: Parsed JSON.

    Returns:
        bool: True when a string was shortened.
    """
    best = None

    def visit(node, path):
        nonlocal best
        if isinstance(node, str):
            if best is None or len(node) > best[0]:
                best = (len(node), path, node)
            return
        if isinstance(node, dict):
            for key, item in node.items():
                visit(item, path + (("dict", key),))
        elif isinstance(node, list):
            for index, item in enumerate(node):
                visit(item, path + (("list", index),))

    visit(value, ())
    if best is None or best[0] <= 80:
        return False
    _assign_path(value, best[1], best[2][: max(80, len(best[2]) // 2)])
    return True


def _assign_path(value, path, text):
    """
    Replace the string at path.

    Args:
        value: Parsed JSON root.
        path (tuple): dict and list steps to the string.
        text (str): Replacement text.
    """
    node = value
    for _, key in path[:-1]:
        node = node[key]
    node[path[-1][1]] = text


def _token_count(messages):
    """
    Count gpt-4o tokens in message text and tool arguments.

    Args:
        messages (list): Chat history.

    Returns:
        int: Token count, or a character estimate when the tokenizer is absent.
    """
    parts = []
    for message in messages:
        parts.append(_content(message))
        for call in _tool_calls(message):
            parts.append(json.dumps(call.get("args") or {}, default=str))
    text = "\n".join(parts)
    encoder = _token_encoder()
    if encoder is None:
        return len(text) // 4
    return len(encoder.encode(text))


def _token_encoder():
    """
    Return the gpt-4o tokenizer, or None when it is not installed.

    Returns:
        object: tiktoken encoder, or None.
    """
    global _ENCODER
    if _ENCODER is not None:
        return _ENCODER
    try:
        import tiktoken
    except ImportError:
        return None
    _ENCODER = tiktoken.encoding_for_model("gpt-4o")
    return _ENCODER


def _json_object(value):
    """
    Parse a JSON object.

    Args:
        value: Tool content or an already parsed dict.

    Returns:
        dict: Parsed object, or None.
    """
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = json.loads(value)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _content(message):
    """
    Return the text of one message.

    MCP tool results arrive as a list of content blocks. The JSON is the
    text of those blocks.

    Args:
        message: A LangChain message or a role/content dict.

    Returns:
        str: Message text.
    """
    content = message.get("content") if isinstance(message, dict) else message.content
    return _content_text(content)


def _content_text(content):
    """
    Return the text inside a message content value.

    Args:
        content: A string, or a list of strings and content blocks.

    Returns:
        str: Combined text.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(_block_text(part) for part in content)
    text = getattr(content, "text", None)
    if isinstance(text, str):
        return text
    return str(content or "")


def _block_text(part):
    """
    Return the text of one content block.

    Args:
        part: A string or a text block.

    Returns:
        str: Block text.
    """
    if isinstance(part, str):
        return part
    if isinstance(part, dict):
        if "text" in part:
            return str(part.get("text") or "")
        return json.dumps(part, default=str)
    text = getattr(part, "text", None)
    if isinstance(text, str):
        return text
    return str(part)


def _set_content(message, content):
    """
    Replace the text of one message.

    Args:
        message: A LangChain message or a role/content dict.
        content (str): New text.
    """
    if isinstance(message, dict):
        message["content"] = content
        return
    message.content = content


def _set_tool_calls(message, tool_calls):
    """
    Replace the tool calls on one assistant message.

    The raw OpenAI tool-call copy is dropped so a finished report_json is
    not sent beside the shortened arguments.

    Args:
        message: Assistant message.
        tool_calls (list): Replacement tool calls.
    """
    if isinstance(message, dict):
        message["tool_calls"] = tool_calls
        extra = message.get("additional_kwargs")
        if isinstance(extra, dict):
            extra.pop("tool_calls", None)
        return
    message.tool_calls = tool_calls
    extra = dict(message.additional_kwargs or {})
    if "tool_calls" in extra:
        extra.pop("tool_calls", None)
        message.additional_kwargs = extra


def _message_name(message):
    """
    Return the tool name stored on a tool result.

    Args:
        message: A LangChain message or a role/content dict.

    Returns:
        str: Tool name, or an empty string.
    """
    name = (
        message.get("name")
        if isinstance(message, dict)
        else getattr(message, "name", None)
    )
    return name or ""


def _clip_text(text, limit):
    """
    Return text trimmed to a maximum length.

    Args:
        text (str): Source text.
        limit (int): Maximum characters.

    Returns:
        str: Trimmed text.
    """
    value = str(text or "")
    if len(value) <= limit:
        return value
    return value[:limit]
