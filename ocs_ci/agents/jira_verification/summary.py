"""OpenAI summary of one Jira issue for a verification report."""

import json
import re
from logging import getLogger

log = getLogger(__name__)

_SUMMARY_INSTRUCTIONS = (
    "You summarize one ODF Jira bug for a verification report. "
    "Write 3 to 6 sentences of plain text. "
    "State what failed, which product versions are affected and fixed, "
    "where it was seen, and the parent or original issue with its status "
    "when one is present. Use only the JSON. Do not invent pull requests, "
    "commands, versions, or a status. No markdown and no heading."
)
_FENCE = re.compile(r"^```[a-zA-Z]*\n?|```$")


def summarize_issue(issue):
    """
    Ask OpenAI for a short summary of one issue.

    Args:
        issue (dict): Verification payload, or the report fields when the
            payload was not cached.

    Returns:
        str: Plain-text summary.

    Raises:
        ValueError: OpenAI returned an empty summary.

    """
    from langchain_core.messages import HumanMessage, SystemMessage

    from ocs_ci.agents.runtime.llm import _openai_model

    source = issue_text_for_summary(issue)
    log.info(f"Asking OpenAI to summarize {source.get('key') or 'issue'}")
    response = _openai_model().invoke(
        [
            SystemMessage(content=_SUMMARY_INSTRUCTIONS),
            HumanMessage(content=json.dumps(source, ensure_ascii=False)),
        ]
    )
    summary = _plain_summary(_message_text(response))
    if not summary:
        raise ValueError("OpenAI returned an empty issue summary")
    return summary


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
