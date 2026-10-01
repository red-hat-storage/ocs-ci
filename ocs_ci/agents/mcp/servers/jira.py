"""Jira MCP server.

Tools call ocs_ci.utility.jira.JiraHelper. Start this server with:

    python -m ocs_ci.agents.mcp.servers.jira
"""

import json

TOOL_NAMES = (
    "jira_get_issue",
    "jira_save_verification_report",
    "jira_search_issues",
)
_fetched_issues = {}


def jira_search_issues(
    version: str = "", project: str = "DFBUGS", release: str = ""
) -> str:
    """
    List Jira issues in ON_QA for one version or one Target Release.

    Without release, an issue matches when its Target Version, Target Release,
    or Fix Version equals version. With release, only that Target Release matches.

    Args:
        version (str): Version name, for example odf-5.0.
        project (str): Jira project key. Defaults to DFBUGS.
        release (str): Target Release name. When set, this selects the bugs.

    Returns:
        str: JSON with count and issues. Each issue has key, summary, status,
            and versions.

    """
    issues = _jira_helper().search_on_qa(
        version, project=project, release=release or None
    )
    return json.dumps(
        {
            "version": version,
            "release": release,
            "project": project,
            "count": len(issues),
            "issues": issues,
        }
    )


def jira_get_issue(issue_key: str) -> str:
    """
    Return one issue's description, headed sections, links, and comments.

    Use this to read how the fix should be verified. Headed sections include
    verification_steps, steps_to_reproduce, and expected_results when those
    headings are present. verification_notes are the comments that change how
    the fix is checked. source_issues are the original bugs for a backport or
    clone, including their sections and verification notes. parent_issues lists
    each parent or original issue with its key, summary, status, and relation.

    Args:
        issue_key (str): Jira issue key, for example DFBUGS-10425.

    Returns:
        str: JSON for that issue.

    """
    payload = _jira_helper().issue_for_verification(issue_key)
    _fetched_issues[issue_key] = payload
    return json.dumps(payload)


def jira_save_verification_report(
    version: str, issue_key: str, report_json: str
) -> str:
    """
    Write the verification report for one issue that was just read.

    Call this after jira_get_issue, once bug_description, versions,
    environments, upgrade_scenario, verification_steps, additional_info, and
    git_prs are filled. parent_issues is copied from the issue just read,
    including each parent's status. OpenAI writes the summary stored on the
    report. tests lists pytest tests under tests/ whose names, paths, or
    Polarion ids appear exactly in the bug. The file is
    reports/<version>/<issue_key>.yaml.
    This writes a local file. It does not update Jira. During --dry-run the
    saved report records dry_run so a later run can see that Jira and other
    applications were left unchanged.

    Args:
        version (str): Version name, for example odf-5.0.
        issue_key (str): Jira issue key.
        report_json (str): JSON object with the report fields.

    Returns:
        str: JSON with the saved path.

    """
    from ocs_ci.agents.jira_verification.report_store import write_verification_report
    from ocs_ci.agents.jira_verification.summary import summarize_issue
    from ocs_ci.agents.jira_verification.test_index import matching_tests
    from ocs_ci.agents.runtime.dry_run import dry_run_enabled

    report = json.loads(report_json)
    fetched = _fetched_issues.get(issue_key)
    if isinstance(fetched, dict) and "parent_issues" in fetched:
        report["parent_issues"] = fetched["parent_issues"]
    source = fetched if isinstance(fetched, dict) else report
    report["summary"] = summarize_issue(source)
    report["tests"] = matching_tests(source)
    if dry_run_enabled():
        report["dry_run"] = True
    path = write_verification_report(version, issue_key, report)
    return json.dumps(
        {
            "key": issue_key,
            "saved": str(path),
            "dry_run": bool(report.get("dry_run")),
        }
    )


def _jira_helper():
    """
    Return a Jira helper using the agent credentials.

    During --dry-run the client can search and fetch issues. Comment, edit,
    transition, and every other Jira write is refused.

    Returns:
        JiraHelper: Connected helper.

    """
    from ocs_ci.agents.runtime.dry_run import ReadOnlyJira, dry_run_enabled
    from ocs_ci.utility.jira import JiraHelper, resolve_agent_jira_auth

    helper = JiraHelper(auth=resolve_agent_jira_auth())
    if dry_run_enabled():
        helper.jira = ReadOnlyJira(helper.jira)
    return helper


def build_server():
    """
    Register the Jira tools on a FastMCP server.

    Returns:
        FastMCP: Server that exposes jira_search_issues and jira_get_issue.

    Raises:
        ImportError: The mcp package is not installed.

    """
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("jira")
    server.tool(name="jira_search_issues")(jira_search_issues)
    server.tool(name="jira_get_issue")(jira_get_issue)
    server.tool(name="jira_save_verification_report")(jira_save_verification_report)
    return server


def main():
    """Start the Jira MCP server on stdio."""
    try:
        server = build_server()
    except ImportError as error:
        raise NotImplementedError(
            "The Jira MCP server requires the mcp package in the agents extra"
        ) from error
    server.run()


if __name__ == "__main__":
    main()
