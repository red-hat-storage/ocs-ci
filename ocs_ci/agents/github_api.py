"""Open a GitHub issue for a verified bug that still has no test."""

import logging
from urllib.parse import urlparse

import requests

from ocs_ci.agents.jira_verification.summary import summary_text
from ocs_ci.agents.mcp.registry import _load_auth_config

logger = logging.getLogger(__name__)


def create_automation_issue(report, result):
    """
    Open an issue asking for a test of one verified bug.

    The issue is created on agents_credentials.github.upstream_repository.
    The token is not written to the log.

    Args:
        report (dict): Saved verification plan.
        result (dict): Execution result with status and steps.

    Returns:
        str: Issue URL, or an empty string when GitHub does not accept the issue.

    Raises:
        ValueError: GitHub credentials or the upstream repository are missing.
    """
    auth = _github_auth()
    owner, repo = _split_repo(auth["upstream_repository"])
    key = str(report.get("key") or result.get("key") or "").strip()
    title = f"Automate verification for {key}"
    response = requests.post(
        f"https://api.github.com/repos/{owner}/{repo}/issues",
        json={"title": title, "body": _issue_body(report, result)},
        headers={
            "Authorization": f"Bearer {auth['token']}",
            "Accept": "application/vnd.github+json",
        },
        timeout=60,
    )
    if response.status_code in {401, 403}:
        logger.error("GitHub rejected the API credentials")
        return ""
    if response.status_code not in {200, 201}:
        logger.error(
            f"GitHub returned HTTP {response.status_code} while opening {title}"
        )
        return ""
    url = ""
    try:
        url = str(response.json().get("html_url") or "")
    except ValueError:
        url = ""
    if url:
        logger.info(f"Opened GitHub issue {url}")
    return url


def _github_auth():
    """
    Read the agent GitHub credentials.

    Returns:
        dict: username, token, and upstream_repository.

    Raises:
        ValueError: The credentials are incomplete.
    """
    loaded = _load_auth_config()
    agents = loaded.get("agents_credentials") or {}
    raw = agents.get("github") if isinstance(agents, dict) else None
    if not isinstance(raw, dict):
        raw = {}
    token = str(raw.get("token") or "").strip()
    upstream = str(raw.get("upstream_repository") or "").strip()
    if not token or not upstream:
        raise ValueError(
            "Set agents_credentials.github.token and "
            "agents_credentials.github.upstream_repository in data/auth.yaml."
        )
    return {
        "username": str(raw.get("username") or "").strip(),
        "token": token,
        "upstream_repository": upstream,
    }


def _split_repo(url):
    """
    Return the owner and repository from a GitHub URL.

    Args:
        url (str): Repository URL such as https://github.com/org/ocs-ci.

    Returns:
        tuple: owner and repository name.

    Raises:
        ValueError: The URL does not name an owner and a repository.
    """
    path = urlparse(url).path.strip("/")
    parts = [part for part in path.split("/") if part]
    if len(parts) < 2:
        raise ValueError(f"GitHub repository URL is not usable: {url}")
    repo = parts[1]
    if repo.endswith(".git"):
        repo = repo[:-4]
    return parts[0], repo


def _issue_body(report, result):
    """
    Build the automation issue body from the plan and the execution result.

    Args:
        report (dict): Saved verification plan.
        result (dict): Execution result.

    Returns:
        str: Markdown body.
    """
    key = str(report.get("key") or result.get("key") or "")
    lines = [
        f"Verified `{key}` on cluster `{result.get('cluster') or report.get('cluster') or ''}`.",
        "",
        str(report.get("url") or ""),
        "",
        summary_text(report.get("summary"))
        or str(report.get("bug_description") or "").strip(),
        "",
        "Step results:",
    ]
    for step in result.get("steps") or []:
        command = step.get("command") or ""
        excerpt = step.get("output_excerpt") or ""
        lines.append(
            f"- step {step.get('step')}: exit {step.get('exit_code')} "
            f"`{command}` {excerpt}".rstrip()
        )
    return "\n".join(lines).strip() + "\n"
