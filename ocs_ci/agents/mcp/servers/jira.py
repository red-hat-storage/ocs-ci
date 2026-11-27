"""Jira MCP server.

Tools call ocs_ci.utility.jira.JiraHelper. Start this server with:

    python -m ocs_ci.agents.mcp.servers.jira
"""

import json
import logging

logger = logging.getLogger(__name__)

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
    report, with issue, reproduction_steps, and expected_results. When every
    verification step has an empty command, OpenAI replaces those steps with
    oc commands taken from that summary. When a step uses an object that is
    not already present, the first commands create that object. A step that
    already has a command is kept. tests lists pytest tests under tests/ whose
    names, paths, or
    Polarion ids appear exactly in the bug. The file is
    reports/<version>/<issue_key>.yaml.
    This writes a local file. It does not update Jira. During --dry-run the
    saved report records dry_run so a later run can see that Jira and other
    applications were left unchanged. A dry run still writes that file when
    the OpenAI summary call fails, using the issue text already read.

    Args:
        version (str): Version name, for example odf-5.0.
        issue_key (str): Jira issue key.
        report_json (str): JSON object with the report fields.

    Returns:
        str: JSON with the saved path.

    """
    from ocs_ci.agents.jira_verification.report_store import write_verification_report
    from ocs_ci.agents.runtime.dry_run import dry_run_enabled

    report = json.loads(report_json)
    fetched = _fetched_issues.get(issue_key)
    if isinstance(fetched, dict) and "parent_issues" in fetched:
        report["parent_issues"] = fetched["parent_issues"]
    source = fetched if isinstance(fetched, dict) else report
    dry_run = dry_run_enabled()
    if dry_run:
        report["dry_run"] = True
    _enrich_report(report, source, issue_key, dry_run)
    if dry_run:
        _fill_missing_report_fields(report, source)
    path = write_verification_report(version, issue_key, report)
    logger.info(f"Saved verification report {path}")
    return json.dumps(
        {
            "key": issue_key,
            "saved": str(path),
            "dry_run": bool(report.get("dry_run")),
        }
    )


def _enrich_report(report, source, issue_key, dry_run):
    """
    Add the summary, verification commands, tests, and cluster check.

    A dry run writes the report from the collected issue when OpenAI cannot
    summarize it or cannot fill commands. Any other run still raises that
    error before a file is written.

    Args:
        report (dict): Report fields from the agent.
        source (dict): Fetched issue, or the report when the issue was not
            cached.
        issue_key (str): Jira issue key.
        dry_run (bool): True when this run must only write the local report.
    """
    from ocs_ci.agents.jira_verification.jenkins_cluster import cluster_check
    from ocs_ci.agents.jira_verification.summary import (
        summary_from_issue,
        summarize_issue,
        verification_steps_for_report,
    )
    from ocs_ci.agents.jira_verification.test_index import matching_tests

    try:
        report["summary"] = summarize_issue(source)
    except Exception as error:
        if not dry_run:
            raise
        logger.error(
            f"OpenAI summary failed for {issue_key} ({type(error).__name__}). "
            "Writing the dry-run report from the collected issue."
        )
        report["summary"] = summary_from_issue(source)
    try:
        report["verification_steps"] = verification_steps_for_report(
            source,
            report.get("summary"),
            report.get("verification_steps"),
            issue_key,
        )
    except Exception as error:
        if not dry_run:
            raise
        logger.error(
            f"OpenAI verification steps failed for {issue_key} "
            f"({type(error).__name__}). Keeping the steps already collected."
        )
    try:
        report["tests"] = matching_tests(source)
    except Exception as error:
        if not dry_run:
            raise
        logger.error(
            f"Test matching failed for {issue_key} ({type(error).__name__}). "
            "Saving the dry-run report without tests."
        )
        report["tests"] = []
    checked = cluster_check(report)
    if checked:
        report["cluster_check"] = checked


def _fill_missing_report_fields(report, source):
    """
    Fill report fields that the writer requires when the agent omitted them.

    Args:
        report (dict): Report fields about to be saved.
        source (dict): Fetched issue, or the report itself.
    """
    source = source if isinstance(source, dict) else {}
    if "bug_description" not in report:
        report["bug_description"] = (
            source.get("bug_description")
            or source.get("description")
            or source.get("summary")
            or ""
        )
    for name in ("affected_version", "fix_version", "git_prs"):
        if name not in report or report[name] is None:
            report[name] = list(source.get(name) or [])
    if "environment_reported" not in report:
        report["environment_reported"] = str(
            source.get("environment_reported") or source.get("environment") or ""
        )
    if "environment_verify" not in report:
        versions = report.get("fix_version") or []
        report["environment_verify"] = ", ".join(
            str(version) for version in versions if str(version).strip()
        )
    if "upgrade_scenario" not in report:
        report["upgrade_scenario"] = False
    if not isinstance(report.get("verification_steps"), list):
        report["verification_steps"] = []
    if "additional_info" not in report:
        report["additional_info"] = ""


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
    from ocs_ci.agents.runtime.logging import configure_agent_logging

    configure_agent_logging()
    try:
        server = build_server()
    except ImportError as error:
        raise NotImplementedError(
            "The Jira MCP server requires the mcp package in the agents extra"
        ) from error
    server.run()


if __name__ == "__main__":
    main()
