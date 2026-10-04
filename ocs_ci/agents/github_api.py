"""Open a GitHub issue for a verified bug that still has no test."""

import logging
from pathlib import Path
from urllib.parse import quote
from urllib.parse import urlparse

import requests

from ocs_ci.agents.jira_verification.summary import summary_text
from ocs_ci.agents.mcp.registry import _load_auth_config
from ocs_ci.agents.runtime.dry_run import dry_run_enabled

logger = logging.getLogger(__name__)

_LABEL = "verification_agent_bug"
_BODY_LIMIT = 60000


def create_automation_issue(report, result, attachments=None):
    """
    Open an issue asking for a test of one verified bug.

    The title is Automation_<Jira key>. The body lists the steps to automate
    and includes the result YAML and verification Markdown. The issue is
    labeled verification_agent_bug and created on
    agents_credentials.github.upstream_repository. A dry run returns before
    any GitHub request. The token is not written to the log.

    Args:
        report (dict): Saved verification plan.
        result (dict): Execution result with status and steps.
        attachments (list): Result YAML and verification Markdown paths.

    Returns:
        str: Issue URL, or an empty string when GitHub does not accept the issue.

    Raises:
        ValueError: GitHub credentials or the upstream repository are missing.
    """
    if dry_run_enabled():
        logger.info("Dry run: GitHub issue was not opened")
        return ""
    auth = _github_auth()
    owner, repo = _split_repo(auth["upstream_repository"])
    headers = _headers(auth["token"])
    key = str(report.get("key") or result.get("key") or "").strip()
    title = f"Automation_{key}"
    existing = _existing_issue(owner, repo, title, headers)
    if existing:
        logger.info(f"GitHub issue already exists: {existing}")
        return existing
    _ensure_label(owner, repo, headers)
    files = [Path(path) for path in (attachments or []) if path]
    body, overflow = _issue_body(report, result, files)
    response = requests.post(
        f"https://api.github.com/repos/{owner}/{repo}/issues",
        json={"title": title, "body": body, "labels": [_LABEL]},
        headers=headers,
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
    payload = {}
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    url = str(payload.get("html_url") or "")
    number = payload.get("number")
    if overflow and number:
        _comment_files(owner, repo, number, overflow, headers)
    if url:
        logger.info(f"Opened GitHub issue {url}")
    return url


def _headers(token):
    """
    Return the GitHub API headers.

    Args:
        token (str): Personal access token.

    Returns:
        dict: Authorization and accept headers.
    """
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


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


def _existing_issue(owner, repo, title, headers):
    """
    Return the URL of an open issue with this exact title.

    Args:
        owner (str): Repository owner.
        repo (str): Repository name.
        title (str): Issue title.
        headers (dict): GitHub API headers.

    Returns:
        str: Issue URL, or an empty string.
    """
    query = f'repo:{owner}/{repo} "{title}" in:title is:issue'
    try:
        response = requests.get(
            "https://api.github.com/search/issues",
            params={"q": query},
            headers=headers,
            timeout=60,
        )
    except requests.RequestException as error:
        logger.error(f"Could not search GitHub issues: {error}")
        return ""
    if response.status_code != 200:
        return ""
    for item in response.json().get("items") or []:
        if item.get("title") == title and not item.get("pull_request"):
            return str(item.get("html_url") or "")
    return ""


def _ensure_label(owner, repo, headers):
    """
    Create the automation label when the repository does not have it.

    Args:
        owner (str): Repository owner.
        repo (str): Repository name.
        headers (dict): GitHub API headers.
    """
    name = quote(_LABEL, safe="")
    try:
        response = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/labels/{name}",
            headers=headers,
            timeout=60,
        )
    except requests.RequestException as error:
        logger.error(f"Could not read the GitHub label {_LABEL}: {error}")
        return
    if response.status_code == 200:
        return
    if response.status_code != 404:
        logger.error(
            f"GitHub returned HTTP {response.status_code} while reading label {_LABEL}"
        )
        return
    try:
        created = requests.post(
            f"https://api.github.com/repos/{owner}/{repo}/labels",
            json={
                "name": _LABEL,
                "color": "5319e7",
                "description": "Automation request opened by the verification agent",
            },
            headers=headers,
            timeout=60,
        )
    except requests.RequestException as error:
        logger.error(f"Could not create the GitHub label {_LABEL}: {error}")
        return
    if created.status_code not in {200, 201}:
        logger.error(
            f"GitHub returned HTTP {created.status_code} while creating label {_LABEL}"
        )


def _issue_body(report, result, files):
    """
    Build the automation issue body and any files that do not fit in it.

    Args:
        report (dict): Saved verification plan.
        result (dict): Execution result.
        files (list): Paths to include.

    Returns:
        tuple: Issue body, and files that must be posted as comments.
    """
    body = _steps_text(report, result)
    included = []
    overflow = []
    for path in files:
        block = _file_block(path)
        if not block:
            continue
        candidate = "\n\n".join(part for part in [body, *included, block] if part)
        if len(candidate) <= _BODY_LIMIT:
            included.append(block)
        else:
            overflow.append(path)
    if included:
        body = "\n\n".join([body, *included])
    elif overflow:
        names = ", ".join(path.name for path in overflow)
        body = f"{body}\n\nThe verification files follow in comments: {names}."
    return body.strip() + "\n", overflow


def _steps_text(report, result):
    """
    Describe the bug and the steps an automated test should perform.

    Args:
        report (dict): Saved verification plan.
        result (dict): Execution result.

    Returns:
        str: Markdown body without the attached files.
    """
    key = str(report.get("key") or result.get("key") or "")
    cluster = str(result.get("cluster") or report.get("cluster") or "").strip()
    lines = [
        f"Automate a test for `{key}`.",
        "",
        str(report.get("url") or "").strip(),
        "",
        f"Verification status: {result.get('status') or ''}",
    ]
    if cluster:
        lines.append(f"Cluster: {cluster}")
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    issue = str(summary.get("issue") or "").strip()
    if not issue:
        issue = (
            summary_text(report.get("summary"))
            or str(report.get("bug_description") or "").strip()
        )
    if issue:
        lines.extend(["", "## Bug", "", issue])
    steps = [
        step
        for step in (report.get("verification_steps") or [])
        if isinstance(step, dict)
    ]
    if steps:
        lines.extend(["", "## Steps to automate", ""])
        for index, step in enumerate(steps, start=1):
            number = step.get("step") or index
            action = str(step.get("action") or "").strip()
            command = str(step.get("command") or "").strip()
            lines.append(f"{number}. {action}".rstrip())
            if command:
                lines.append(f"   Command: `{command}`")
            manifest = str(step.get("manifest") or "").strip()
            if manifest:
                lines.extend(["", "   Manifest:", "", _fenced(manifest, "yaml"), ""])
    expected = [
        str(item).strip()
        for item in (summary.get("expected_results") or [])
        if str(item).strip()
    ]
    if expected:
        lines.extend(["", "## Expected result", ""])
        lines.extend(f"- {item}" for item in expected)
    parents = [
        parent
        for parent in (report.get("parent_issues") or [])
        if isinstance(parent, dict) and parent.get("key")
    ]
    if parents:
        lines.extend(["", "## Parent issues", ""])
        for parent in parents:
            lines.append(
                f"- {parent.get('key')} ({parent.get('status') or 'unknown'}): "
                f"{parent.get('summary') or ''}".rstrip()
            )
    pulls = [
        str(url).strip() for url in (report.get("git_prs") or []) if str(url).strip()
    ]
    if pulls:
        lines.extend(["", "## Pull requests", ""])
        lines.extend(f"- {url}" for url in pulls)
    notes = str(report.get("additional_info") or "").strip()
    if notes:
        lines.extend(["", "## Notes", "", notes])
    lines.extend(
        [
            "",
            "The result YAML and the verification Markdown are included below.",
        ]
    )
    return "\n".join(lines).strip()


def _file_block(path):
    """
    Return one attached file as a fenced Markdown block.

    Args:
        path (Path): File to include.

    Returns:
        str: Markdown section, or an empty string when the file is missing.
    """
    if not path.is_file():
        logger.error(f"Verification file is missing: {path}")
        return ""
    text = path.read_text(encoding="utf-8")
    language = "yaml" if path.suffix in {".yaml", ".yml"} else "markdown"
    return "\n".join(
        [
            f"## {path.name}",
            "",
            _fenced(text.rstrip("\n"), language),
        ]
    )


def _fenced(text, language):
    """
    Wrap text in a fence longer than any backtick run inside it.

    Args:
        text (str): File contents.
        language (str): Fence language tag.

    Returns:
        str: Fenced block.
    """
    longest = 0
    current = 0
    for character in text:
        if character == "`":
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    fence = "`" * max(3, longest + 1)
    return f"{fence}{language}\n{text}\n{fence}"


def _comment_files(owner, repo, number, files, headers):
    """
    Add one comment per file that did not fit in the issue body.

    Args:
        owner (str): Repository owner.
        repo (str): Repository name.
        number: Issue number.
        files (list): Paths to post.
        headers (dict): GitHub API headers.
    """
    for path in files:
        block = _file_block(path)
        if not block:
            continue
        if len(block) > _BODY_LIMIT:
            logger.error(f"{path.name} is too large to add to the GitHub issue")
            continue
        try:
            response = requests.post(
                f"https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments",
                json={"body": block},
                headers=headers,
                timeout=60,
            )
        except requests.RequestException as error:
            logger.error(f"Could not comment {path.name} on the GitHub issue: {error}")
            continue
        if response.status_code not in {200, 201}:
            logger.error(
                f"GitHub returned HTTP {response.status_code} while commenting {path.name}"
            )
