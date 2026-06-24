"""Public API for JIRA fetch, search, and comment operations."""

from __future__ import annotations

import logging
from typing import Any

from client import get_jira_client
from jql import build_jql, build_on_qa_jql
from models import (
    DEFAULT_JIRA_FIELDS,
    JIRA_ISSUE_TYPE_BUG,
    JIRA_PROJECT_DFBUGS,
    JIRA_STATUS_ON_QA,
    WRITE_DRY_RUN_DEFAULT,
)
from parser import parse_jira_issue

log = logging.getLogger(__name__)

__all__ = [
    "add_comment",
    "build_jql",
    "build_on_qa_jql",
    "get_issue",
    "get_issue_with_comments",
    "get_issue_with_fix_context",
    "get_issues_by_keys",
    "parse_jira_issue",
    "search",
    "search_and_parse",
    "search_by_params",
]


def search(
    jql: str,
    *,
    jira_config: str | None = None,
    fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Execute JQL and return raw JIRA issue dicts.

    Args:
        jql (str): JQL query
        jira_config (str | None): Optional jira.cfg path
        fields (list[str] | None): Fields to return

    Returns:
        list[dict]: Raw issues from JIRA API

    """
    log.info("JIRA search: %s", jql)
    client = get_jira_client(jira_config)
    return client.search_issues_by_jql(jql, fields=fields or DEFAULT_JIRA_FIELDS)


def search_and_parse(
    jql: str,
    *,
    jira_config: str | None = None,
    fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Search JIRA and return parsed issue details."""
    issues = search(jql, jira_config=jira_config, fields=fields)
    parsed = [parse_jira_issue(issue) for issue in issues]
    log.info("Parsed %d JIRA issues", len(parsed))
    return parsed


def get_issue(
    issue_key: str,
    *,
    jira_config: str | None = None,
) -> dict[str, Any]:
    """
    Fetch and parse a single JIRA issue.

    Args:
        issue_key (str): e.g. DFBUGS-784
        jira_config (str | None): Optional jira.cfg path

    Returns:
        dict: Parsed issue details

    """
    client = get_jira_client(jira_config)
    raw = client.get_issue(issue_key)
    return parse_jira_issue(raw)


def get_issue_with_comments(
    issue_key: str,
    *,
    jira_config: str | None = None,
) -> dict[str, Any]:
    """Fetch JIRA issue plus comment thread (for Claude/Rovo-style repro analysis)."""
    from parser import adf_to_text, field_name, parse_jira_issue

    client = get_jira_client(jira_config)
    raw = client.get_issue(issue_key)
    issue = parse_jira_issue(raw)

    comments: list[dict[str, Any]] = []
    try:
        raw_data = client.jira.issue_get_comments(issue_key)
        for comment in raw_data.get("comments", []):
            body = comment.get("body", "")
            if isinstance(body, dict):
                body = adf_to_text(body)
            comments.append(
                {
                    "author": field_name(comment.get("author")),
                    "created": comment.get("created", "") or "",
                    "body": str(body).strip(),
                }
            )
    except Exception as exc:
        log.warning("Could not fetch comments for %s: %s", issue_key, exc)

    issue["comments"] = comments
    return issue


def get_issue_with_fix_context(
    issue_key: str,
    *,
    jira_config: str | None = None,
    include_fix_prs: bool = True,
) -> dict[str, Any]:
    """
    Fetch JIRA issue with comments and linked fix pull requests.

    Fix PRs are discovered from JIRA remote links plus GitHub URLs in
    description/comments. PR title/body/files are enriched via ``gh`` or
    GitHub API when credentials are available.
    """
    issue = get_issue_with_comments(issue_key, jira_config=jira_config)
    if not include_fix_prs:
        issue["fix_pull_requests"] = []
        return issue

    from pr_context import collect_fix_pull_requests

    client = get_jira_client(jira_config)
    issue["fix_pull_requests"] = collect_fix_pull_requests(issue_key, issue, client)
    return issue


def get_issues_by_keys(
    issue_keys: list[str] | str,
    *,
    jira_config: str | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """
    Fetch and parse explicit JIRA issue keys (skips JQL search).

    Args:
        issue_keys: One key or a list of keys (e.g. DFBUGS-784)
        jira_config: Optional jira.cfg path

    Returns:
        tuple: (parsed issues, synthetic JQL for run record)

    """
    if isinstance(issue_keys, str):
        if "," in issue_keys:
            keys = [part.strip() for part in issue_keys.split(",") if part.strip()]
        else:
            keys = [issue_keys.strip()] if issue_keys.strip() else []
    else:
        keys = [str(key).strip() for key in issue_keys if str(key).strip()]

    if not keys:
        raise ValueError("issues list is empty")

    parsed = [get_issue(key, jira_config=jira_config) for key in keys]
    jql = f"key in ({', '.join(keys)})"
    log.info("Fetched %d explicit JIRA issues", len(parsed))
    return parsed, jql


def search_by_params(
    params: dict[str, Any],
    *,
    jira_config: str | None = None,
    fields: list[str] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """
    Search using pipeline-style parameters (project, issue_type, odf_version, status).

    Returns:
        tuple: (parsed issues, JQL used)

    """
    jql = build_jql(
        project=params.get("project", JIRA_PROJECT_DFBUGS),
        issue_type=params.get("issue_type", JIRA_ISSUE_TYPE_BUG),
        odf_version=params.get("odf_version"),
        status=params.get("status", JIRA_STATUS_ON_QA),
        extra_jql=params.get("extra_jql"),
    )
    if not params.get("odf_version") and params.get("jql"):
        jql = params["jql"]
    parsed = search_and_parse(jql, jira_config=jira_config, fields=fields)
    return parsed, jql


def add_comment(
    issue_key: str,
    text: str,
    *,
    jira_config: str | None = None,
    dry_run: bool = WRITE_DRY_RUN_DEFAULT,
) -> dict[str, Any]:
    """
    Add a comment to a JIRA issue.

    Args:
        issue_key (str): JIRA key
        text (str): Comment body
        jira_config (str | None): Optional jira.cfg path
        dry_run (bool): If True, do not call JIRA (default True)

    Returns:
        dict: Result metadata

    """
    if dry_run:
        log.info("Dry run: would comment on %s", issue_key)
        return {
            "dry_run": True,
            "issue_key": issue_key,
            "comment": text,
            "message": "Dry run: comment not posted",
        }

    client = get_jira_client(jira_config)
    result = client.add_comment(issue_key, text)
    return {
        "dry_run": False,
        "issue_key": issue_key,
        "comment": text,
        "result": result,
        "message": "Comment posted",
    }
