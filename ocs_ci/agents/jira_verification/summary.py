"""OpenAI summary of one Jira issue for a verification report."""

import json
import logging
import re

logger = logging.getLogger(__name__)

_SUMMARY_INSTRUCTIONS = (
    "You summarize one ODF Jira bug for a verification report. "
    "Return one JSON object and no other text:\n"
    '{"issue":"<what is broken>","reproduction_steps":["<how to produce it>"],'
    '"expected_results":["<what must be true when the fix works>"]}\n'
    "issue is one sentence and states only the failure. Do not repeat versions, "
    "platform, environment, parent key, parent status, or the parent title. "
    "reproduction_steps are the actions that produce the failure. Do not restate "
    "the issue, the actual result, or 'run regression'. Name the test only when "
    "it identifies the case. expected_results is one sentence for the pass "
    "condition and must not repeat the issue. "
    "Use only the JSON input. Do not invent pull requests, commands, versions, "
    "or a status."
)
_STEP_INSTRUCTIONS = (
    "You write oc commands that verify one ODF bug. The saved verification "
    "steps have no command. Build steps from the summary so the same method "
    "works for any bug whose Jira text only says to run regression. "
    "Return one JSON object and no other text:\n"
    '{"verification_steps":[{"step":1,"action":"<what this checks>",'
    '"command":"oc ...","manifest":""}]}\n'
    "Follow reproduction_steps, then check expected_results. Each command is "
    "one oc invocation. No shell operators, pipes, or redirects. Do not write "
    "pytest or tell the reader to run regression. A step passes only when oc "
    "exits 0. To confirm a deleted object is gone, use "
    "oc delete <kind> <name> --ignore-not-found=true --wait=true --timeout=180s. "
    "Do not use oc get of a missing object as the pass check. "
    "When a step creates or deletes an object, that object name must be the "
    "verify token in the user message. Do not delete, patch, or replace an "
    "object that already belongs to the cluster. When a step needs a manifest, "
    "put the YAML in manifest and set command to oc apply -f {{manifest}} or "
    "oc delete -f {{manifest}}. The manifest metadata.name must be the verify "
    "token. Use only kinds and behavior named in the issue."
)
_FENCE = re.compile(r"^```[a-zA-Z]*\n?|```$")
_SHELL = re.compile(r"[;&|`$<>\n]|\$\(")
_MUTATING = re.compile(
    r"^oc\s+(delete|apply|create|patch|replace|label|annotate|scale|adm)\b",
    re.IGNORECASE,
)


def summarize_issue(issue):
    """
    Ask OpenAI for a short summary of one issue.

    Args:
        issue (dict): Verification payload, or the report fields when the
            payload was not cached.

    Returns:
        dict: issue, reproduction_steps, and expected_results.

    Raises:
        ValueError: OpenAI returned an empty summary.

    """
    from langchain_core.messages import HumanMessage, SystemMessage

    from ocs_ci.agents.runtime.llm import _openai_model

    source = issue_text_for_summary(issue)
    logger.info(f"Asking OpenAI to summarize {source.get('key') or 'issue'}")
    response = _openai_model().invoke(
        [
            SystemMessage(content=_SUMMARY_INSTRUCTIONS),
            HumanMessage(content=json.dumps(source, ensure_ascii=False)),
        ]
    )
    summary = _summary_mapping(_json_object(_message_text(response)))
    if not summary["issue"]:
        raise ValueError("OpenAI returned an empty issue summary")
    return summary


def verification_steps_for_report(issue, summary, steps, issue_key=""):
    """
    Fill verification commands from the summary when the report has none.

    Steps that already include a command are kept. A report that only says to
    run regression gets oc commands derived from issue, reproduction_steps,
    and expected_results.

    Args:
        issue (dict): Verification payload or report fields.
        summary (dict): issue, reproduction_steps, and expected_results.
        steps (list): Verification steps already on the report.
        issue_key (str): Jira issue key used in temporary object names.

    Returns:
        list: step, action, command, and manifest when a step needs one.

    Raises:
        ValueError: OpenAI did not return a usable command.

    """
    if _has_command(steps):
        logger.info("Keeping verification steps that already include a command")
        return steps
    key = str(issue_key or (issue or {}).get("key") or "").strip()
    token = f"verify-{key.lower()}" if key else "verify-bug"
    logger.info(f"Writing verification steps from the summary for {key or 'issue'}")
    from langchain_core.messages import HumanMessage, SystemMessage

    from ocs_ci.agents.runtime.llm import _openai_model

    payload = {
        "key": key,
        "verify_token": token,
        "summary": summary,
        "bug_description": (issue or {}).get("bug_description")
        or (issue or {}).get("description")
        or "",
    }
    response = _openai_model().invoke(
        [
            SystemMessage(content=_STEP_INSTRUCTIONS),
            HumanMessage(content=json.dumps(payload, ensure_ascii=False)),
        ]
    )
    parsed = _json_object(_message_text(response))
    written = _usable_steps(parsed.get("verification_steps"), token)
    if not written:
        raise ValueError("OpenAI did not return a verification command")
    return written


def summary_text(summary):
    """
    Return the summary as plain text.

    Args:
        summary (dict or str): Structured summary, or an older plain summary.

    Returns:
        str: Issue, reproduction steps, and expected results.

    """
    if isinstance(summary, dict):
        lines = [str(summary.get("issue") or "").strip()]
        for label, name in (
            ("Reproduction steps", "reproduction_steps"),
            ("Expected results", "expected_results"),
        ):
            items = [
                str(item).strip()
                for item in (summary.get(name) or [])
                if str(item).strip()
            ]
            if not items:
                continue
            lines.append(label)
            lines.extend(f"- {item}" for item in items)
        return "\n".join(line for line in lines if line)
    return str(summary or "").strip()


def issue_text_for_summary(issue):
    """
    Keep the issue text that belongs in a summary prompt.

    Args:
        issue (dict): Verification payload or report fields.

    Returns:
        dict: Key, title, description, versions, environment, parents, and
            verification notes. Raw process comments are omitted.

    """
    issue = issue or {}
    return {
        "key": issue.get("key") or "",
        "title": issue.get("summary") or issue.get("bug_description") or "",
        "status": issue.get("status") or "",
        "description": _clip(issue.get("description") or issue.get("bug_description")),
        "environment": issue.get("environment")
        or issue.get("environment_reported")
        or "",
        "affected_version": issue.get("affected_version") or [],
        "fix_version": issue.get("fix_version") or [],
        "sections": issue.get("sections") or {},
        "components": issue.get("components") or [],
        "parent_issues": issue.get("parent_issues") or [],
        "verification_notes": _notes(issue.get("verification_notes")),
        "source_issues": [
            _source_text(source) for source in (issue.get("source_issues") or [])[:3]
        ],
    }


def _source_text(source):
    """
    Reduce one original issue to the text a summary needs.

    Args:
        source (dict): Followed source issue.

    Returns:
        dict: Key, status, environment, sections, and notes.

    """
    return {
        "key": source.get("key") or "",
        "title": source.get("summary") or "",
        "status": source.get("status") or "",
        "environment": source.get("environment") or "",
        "sections": source.get("sections") or {},
        "description": _clip(source.get("description")),
        "verification_notes": _notes(source.get("verification_notes")),
    }


def _notes(notes):
    """
    Return verification-note text, capped so the prompt stays small.

    Args:
        notes (list): verification_notes entries.

    Returns:
        list: Reason and body for up to 8 notes.

    """
    kept = []
    for note in notes or []:
        if not isinstance(note, dict):
            continue
        body = _clip(note.get("body"), limit=800)
        if not body:
            continue
        kept.append({"reasons": note.get("reasons") or [], "body": body})
        if len(kept) == 8:
            break
    return kept


def _clip(text, limit=4000):
    """
    Return text trimmed to a maximum length.

    Args:
        text (str): Source text.
        limit (int): Maximum characters.

    Returns:
        str: Trimmed text.

    """
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[:limit]


def _message_text(response):
    """
    Return the text content of a chat response.

    Args:
        response: LangChain message, or a plain string.

    Returns:
        str: Response text.

    """
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(item.get("text") or "")
        return "".join(parts)
    return str(content or "")


def _summary_mapping(payload):
    """
    Normalize the summary JSON from OpenAI.

    Args:
        payload (dict): Model JSON.

    Returns:
        dict: issue, reproduction_steps, and expected_results.

    """
    if not isinstance(payload, dict):
        return {"issue": "", "reproduction_steps": [], "expected_results": []}
    return {
        "issue": str(_pick(payload, "issue", "Issue") or "").strip(),
        "reproduction_steps": _string_list(
            _pick(
                payload,
                "reproduction_steps",
                "reproduction steps",
                "steps_to_reproduce",
            )
        ),
        "expected_results": _string_list(
            _pick(
                payload,
                "expected_results",
                "expected results",
                "Expected results",
                "expected_result",
            )
        ),
    }


def _usable_steps(steps, token):
    """
    Keep generated steps whose commands are safe to store on the report.

    Args:
        steps (list): verification_steps from the model.
        token (str): Temporary object name that mutating commands must use.

    Returns:
        list: Steps with a command. Empty when none are usable.

    """
    if not isinstance(steps, list):
        return []
    kept = []
    for item in steps:
        if not isinstance(item, dict):
            continue
        command = " ".join(str(item.get("command") or "").split())
        manifest = str(item.get("manifest") or "").strip()
        if not command.startswith("oc ") or _SHELL.search(command):
            continue
        if re.search(r"\b(pytest|regression)\b", command, re.IGNORECASE):
            continue
        mutating = _MUTATING.search(command) and not command.lower().startswith(
            "oc adm top"
        )
        if mutating and token not in command and token not in manifest:
            continue
        if manifest and token not in manifest:
            continue
        step = {
            "step": len(kept) + 1,
            "action": str(item.get("action") or "").strip(),
            "command": command,
        }
        if manifest:
            step["manifest"] = manifest
        kept.append(step)
    return kept


def _has_command(steps):
    """
    Return whether any saved step already has an oc command.

    Args:
        steps (list): Verification steps.

    Returns:
        bool: True when a step command starts with oc.

    """
    for step in steps or []:
        if not isinstance(step, dict):
            continue
        command = " ".join(str(step.get("command") or "").split())
        if command.startswith("oc ") and not _SHELL.search(command):
            return True
    return False


def _json_object(text):
    """
    Parse one JSON object from model text.

    Args:
        text (str): Model output, optionally wrapped in a markdown fence.

    Returns:
        dict: Parsed object, or an empty dict when the text is not JSON.

    """
    cleaned = _plain_summary(text)
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _pick(payload, *names):
    """
    Return the first present value among equivalent field names.

    Args:
        payload (dict): Model JSON.
        names (str): Field names to try.

    Returns:
        The first present value, or None.

    """
    for name in names:
        if name in payload:
            return payload[name]
    return None


def _string_list(value):
    """
    Return a list of non-empty strings.

    Args:
        value: A string, or a list of strings.

    Returns:
        list: Trimmed strings.

    """
    if isinstance(value, str):
        value = [line.strip(" -") for line in value.splitlines()]
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _plain_summary(text):
    """
    Drop a markdown fence around the summary.

    Args:
        text (str): Model output.

    Returns:
        str: Plain summary.

    """
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = _FENCE.sub("", cleaned).strip()
    return cleaned.replace("`", "")
