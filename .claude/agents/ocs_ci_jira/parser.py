"""Parse JIRA API issue payloads into agent-friendly dicts."""

from __future__ import annotations

from typing import Any


def adf_to_text(node: Any) -> str:
    """Convert JIRA ADF description to plain text."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(adf_to_text(item) for item in node)
    if not isinstance(node, dict):
        return str(node)
    if node.get("type") == "text":
        return node.get("text", "")
    content = node.get("content", [])
    text = adf_to_text(content)
    if node.get("type") in ("paragraph", "heading", "listItem"):
        return f"{text}\n"
    return text


def field_name(value: Any) -> str:
    """Extract display name from a JIRA field value."""
    if value is None:
        return ""
    if isinstance(value, dict):
        return value.get("name") or value.get("displayName") or value.get("key", "")
    return str(value)


def field_names(values: Any) -> list[str]:
    """Extract display names from a JIRA list field."""
    if not values:
        return []
    if isinstance(values, list):
        return [field_name(item) for item in values if item]
    return [field_name(values)]


def parse_jira_issue(issue: dict[str, Any]) -> dict[str, Any]:
    """
    Parse a raw JIRA issue into a structured details dict.

    Args:
        issue (dict): Raw JIRA issue from API

    Returns:
        dict: Structured issue details

    """
    fields = issue.get("fields", issue)
    description = fields.get("description")
    if isinstance(description, dict):
        description = adf_to_text(description).strip()
    elif description is None:
        description = ""

    return {
        "key": issue.get("key", ""),
        "summary": fields.get("summary", ""),
        "description": description,
        "status": field_name(fields.get("status")),
        "priority": field_name(fields.get("priority")),
        "issue_type": field_name(fields.get("issuetype")),
        "components": field_names(fields.get("components")),
        "labels": list(fields.get("labels") or []),
        "fix_versions": field_names(fields.get("fixVersions")),
        "affected_versions": field_names(fields.get("versions")),
        "assignee": field_name(fields.get("assignee")),
        "reporter": field_name(fields.get("reporter")),
        "created": fields.get("created", ""),
        "updated": fields.get("updated", ""),
        "url": issue.get("self", ""),
    }
