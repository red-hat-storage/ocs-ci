"""JQL builders for JIRA search."""

from __future__ import annotations

from models import JIRA_ISSUE_TYPE_BUG, JIRA_PROJECT_DFBUGS, JIRA_STATUS_ON_QA


def normalize_odf_version(odf_version: str) -> str:
    """Normalize ODF version to JIRA target-version format (e.g. odf-4.21.7)."""
    version = odf_version.strip()
    if not version.startswith("odf-"):
        version = f"odf-{version}"
    return version


def build_jql(
    *,
    project: str = JIRA_PROJECT_DFBUGS,
    issue_type: str = JIRA_ISSUE_TYPE_BUG,
    odf_version: str | None = None,
    status: str | None = None,
    extra_jql: str | None = None,
) -> str:
    """
    Build a JQL query from structured parameters.

    Args:
        project (str): JIRA project name
        issue_type (str): Issue type
        odf_version (str | None): Target ODF version (adds target version clause)
        status (str | None): Status filter
        extra_jql (str | None): Additional AND clause

    Returns:
        str: JQL string

    """
    clauses = [f'project = "{project}"', f"issuetype = {issue_type}"]
    if odf_version:
        target = normalize_odf_version(odf_version)
        clauses.append(f'"target version" = {target}')
    if status:
        clauses.append(f"status = {status}")
    if extra_jql:
        clauses.append(f"({extra_jql})")
    return " AND ".join(clauses)


def build_on_qa_jql(odf_version: str) -> str:
    """JQL for ON_QA bugs in a specific ODF z-stream."""
    return build_jql(
        odf_version=odf_version,
        status=JIRA_STATUS_ON_QA,
    )
