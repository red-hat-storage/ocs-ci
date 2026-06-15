"""
JIRA helper methods for z-stream Lane C fix intake.

Fetches ON_QA bugs for a parameterized ODF target version, e.g.:

    project = "Data Foundation Bugs" AND issuetype = Bug
    AND "target version" = odf-4.21.7 AND status = ON_QA
"""

import logging
from typing import Any

from ocs_ci.utility.jira import JiraHelper

log = logging.getLogger(__name__)

JIRA_PROJECT = "Data Foundation Bugs"
JIRA_ISSUE_TYPE = "Bug"
JIRA_STATUS = "ON_QA"

DEFAULT_JIRA_FIELDS = [
    "summary",
    "description",
    "status",
    "priority",
    "components",
    "labels",
    "fixVersions",
    "versions",
    "issuetype",
    "assignee",
    "reporter",
    "created",
    "updated",
]


def normalize_odf_version(odf_version: str) -> str:
    """
    Normalize ODF version to JIRA target-version format.

    Args:
        odf_version (str): e.g. "4.21.7" or "odf-4.21.7"

    Returns:
        str: e.g. "odf-4.21.7"

    """
    version = odf_version.strip()
    if not version.startswith("odf-"):
        version = f"odf-{version}"
    return version


def build_on_qa_jql(odf_version: str) -> str:
    """
    Build JQL for ON_QA bugs in a specific ODF z-stream.

    Args:
        odf_version (str): ODF z-stream version (e.g. 4.21.7 or odf-4.21.7)

    Returns:
        str: JQL query string

    """
    target_version = normalize_odf_version(odf_version)
    return (
        f'project = "{JIRA_PROJECT}" AND issuetype = {JIRA_ISSUE_TYPE} '
        f'AND "target version" = {target_version} AND status = {JIRA_STATUS}'
    )


def _adf_to_text(node: Any) -> str:
    """Convert JIRA ADF description to plain text."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(_adf_to_text(item) for item in node)
    if not isinstance(node, dict):
        return str(node)
    if node.get("type") == "text":
        return node.get("text", "")
    content = node.get("content", [])
    text = _adf_to_text(content)
    if node.get("type") in ("paragraph", "heading", "listItem"):
        return f"{text}\n"
    return text


def _field_name(value: Any) -> str:
    """Extract display name from a JIRA field value."""
    if value is None:
        return ""
    if isinstance(value, dict):
        return value.get("name") or value.get("displayName") or value.get("key", "")
    return str(value)


def _field_names(values: Any) -> list[str]:
    """Extract display names from a JIRA list field."""
    if not values:
        return []
    if isinstance(values, list):
        return [_field_name(item) for item in values if item]
    return [_field_name(values)]


def parse_jira_issue(issue: dict[str, Any]) -> dict[str, Any]:
    """
    Parse a raw JIRA issue into a structured details dict.

    Args:
        issue (dict): Raw JIRA issue from API or MCP

    Returns:
        dict: Structured issue details

    """
    fields = issue.get("fields", issue)
    description = fields.get("description")
    if isinstance(description, dict):
        description = _adf_to_text(description).strip()
    elif description is None:
        description = ""

    return {
        "key": issue.get("key", ""),
        "summary": fields.get("summary", ""),
        "description": description,
        "status": _field_name(fields.get("status")),
        "priority": _field_name(fields.get("priority")),
        "issue_type": _field_name(fields.get("issuetype")),
        "components": _field_names(fields.get("components")),
        "labels": list(fields.get("labels") or []),
        "fix_versions": _field_names(fields.get("fixVersions")),
        "affected_versions": _field_names(fields.get("versions")),
        "assignee": _field_name(fields.get("assignee")),
        "reporter": _field_name(fields.get("reporter")),
        "created": fields.get("created", ""),
        "updated": fields.get("updated", ""),
        "url": issue.get("self", ""),
    }


def fetch_jira_issues_by_jql(
    jql: str,
    fields: list[str] | None = None,
    jira_config: str | None = None,
) -> list[dict[str, Any]]:
    """
    Execute a JQL query and return raw JIRA issue dicts.

    Args:
        jql (str): JQL query string
        fields (list[str] | None): JIRA fields to return
        jira_config (str | None): Optional path to jira.cfg

    Returns:
        list[dict]: Raw JIRA issues

    """
    log.info("Fetching JIRA issues with JQL: %s", jql)
    jira_helper = JiraHelper(config_path=jira_config, allow_extended_sources=True)
    return jira_helper.search_issues_by_jql(jql, fields=fields or DEFAULT_JIRA_FIELDS)


def fetch_on_qa_zstream_issues(
    odf_version: str,
    jira_config: str | None = None,
    fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch raw ON_QA bugs for the given ODF z-stream version.

    Args:
        odf_version (str): ODF z-stream version (e.g. 4.21.7)
        jira_config (str | None): Optional path to jira.cfg
        fields (list[str] | None): JIRA fields to return

    Returns:
        list[dict]: Raw JIRA issues

    """
    jql = build_on_qa_jql(odf_version)
    return fetch_jira_issues_by_jql(jql, fields=fields, jira_config=jira_config)


def fetch_on_qa_zstream_bug_details(
    odf_version: str,
    jira_config: str | None = None,
    fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch and parse ON_QA bug details for the given ODF z-stream version.

    Args:
        odf_version (str): ODF z-stream version (e.g. 4.21.7)
        jira_config (str | None): Optional path to jira.cfg
        fields (list[str] | None): JIRA fields to return

    Returns:
        list[dict]: Parsed issue details per bug

    """
    issues = fetch_on_qa_zstream_issues(
        odf_version, jira_config=jira_config, fields=fields
    )
    details = [parse_jira_issue(issue) for issue in issues]
    log.info(
        "Fetched %d ON_QA bugs for %s",
        len(details),
        normalize_odf_version(odf_version),
    )
    return details


def get_jira_issue_details(
    issue_key: str,
    jira_config: str | None = None,
) -> dict[str, Any]:
    """
    Fetch full details for a single JIRA issue by key.

    Args:
        issue_key (str): JIRA issue key (e.g. DFBUGS-1234)
        jira_config (str | None): Optional path to jira.cfg

    Returns:
        dict: Parsed issue details

    """
    jira_helper = JiraHelper(config_path=jira_config, allow_extended_sources=True)
    issue = jira_helper.get_issue(issue_key)
    return parse_jira_issue(issue)
