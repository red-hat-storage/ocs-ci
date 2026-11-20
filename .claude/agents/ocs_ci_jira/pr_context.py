"""Discover and enrich GitHub pull requests linked to JIRA issues (fix PRs)."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import urllib.error
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

_GITHUB_PR_RE = re.compile(
    r"https?://github\.com/(?P<owner>[^/\s>)]+)/(?P<repo>[^/\s>)]+)/pull/(?P<number>\d+)",
    re.IGNORECASE,
)

_MAX_BODY_CHARS = 4000
_MAX_FILES = 25


def _pr_ref_key(ref: dict[str, Any]) -> str:
    return f"{ref['owner']}/{ref['repo']}#{ref['number']}"


def extract_github_pr_refs_from_text(text: str) -> list[dict[str, Any]]:
    """Return unique GitHub PR refs found in free text."""
    if not text:
        return []

    seen: set[str] = set()
    refs: list[dict[str, Any]] = []
    for match in _GITHUB_PR_RE.finditer(text):
        owner = match.group("owner")
        repo = match.group("repo").removesuffix(".git")
        number = int(match.group("number"))
        key = f"{owner}/{repo}#{number}"
        if key in seen:
            continue
        seen.add(key)
        refs.append(
            {
                "owner": owner,
                "repo": repo,
                "number": number,
                "url": f"https://github.com/{owner}/{repo}/pull/{number}",
            }
        )
    return refs


def _urls_from_remote_link(link: dict[str, Any]) -> list[str]:
    obj = link.get("object") or {}
    chunks: list[str] = []
    for key in ("url", "title", "summary"):
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            chunks.append(value.strip())
    return chunks


def fetch_jira_remote_links(jira: Any, issue_key: str) -> list[dict[str, Any]]:
    """Fetch JIRA remote issue links (GitHub development panel / manual links)."""
    try:
        links = jira.get(f"rest/api/2/issue/{issue_key}/remotelink")
        return links if isinstance(links, list) else []
    except Exception as exc:
        log.warning("Could not fetch remote links for %s: %s", issue_key, exc)
        return []


def _resolve_github_token() -> str | None:
    for env_name in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_PAT"):
        token = os.environ.get(env_name)
        if token:
            return token

    try:
        from ocs_ci.utility.utils import load_auth_config

        auth_data = load_auth_config() or {}
        github_auth = auth_data.get("github") or auth_data.get("AUTH", {}).get("github")
        if isinstance(github_auth, dict):
            return github_auth.get("token") or github_auth.get("password")
    except Exception:
        pass
    return None


def _enrich_via_gh_cli(ref: dict[str, Any]) -> dict[str, Any] | None:
    gh_bin = shutil.which("gh")
    if not gh_bin:
        return None

    repo = f"{ref['owner']}/{ref['repo']}"
    cmd = [
        gh_bin,
        "pr",
        "view",
        str(ref["number"]),
        "-R",
        repo,
        "--json",
        "title,body,state,mergedAt,files,additions,deletions,url",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=90,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        log.debug("gh pr view failed for %s: %s", ref.get("url"), exc)
        return None

    if proc.returncode != 0:
        log.debug(
            "gh pr view failed for %s: %s",
            ref.get("url"),
            (proc.stderr or proc.stdout or "")[:300],
        )
        return None

    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        return None

    files = data.get("files") or []
    return {
        "title": data.get("title"),
        "body": (data.get("body") or "")[:_MAX_BODY_CHARS],
        "state": data.get("state"),
        "merged_at": data.get("mergedAt"),
        "url": data.get("url") or ref.get("url"),
        "files_changed": [
            {
                "path": item.get("path"),
                "additions": item.get("additions"),
                "deletions": item.get("deletions"),
            }
            for item in files[:_MAX_FILES]
        ],
        "enrichment": "gh_cli",
    }


def _github_api_request(path: str, token: str | None) -> Any:
    url = f"https://api.github.com{path}"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ocs-ci-issue-verification",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def _enrich_via_github_api(
    ref: dict[str, Any],
    token: str | None,
) -> dict[str, Any] | None:
    owner = ref["owner"]
    repo = ref["repo"]
    number = ref["number"]
    base = f"/repos/{owner}/{repo}/pulls/{number}"

    try:
        pr_data = _github_api_request(base, token)
        files_data = _github_api_request(f"{base}/files?per_page={_MAX_FILES}", token)
    except urllib.error.HTTPError as exc:
        log.debug("GitHub API failed for %s: HTTP %s", ref.get("url"), exc.code)
        return None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        log.debug("GitHub API failed for %s: %s", ref.get("url"), exc)
        return None

    files_changed = []
    if isinstance(files_data, list):
        for item in files_data[:_MAX_FILES]:
            files_changed.append(
                {
                    "path": item.get("filename"),
                    "additions": item.get("additions"),
                    "deletions": item.get("deletions"),
                }
            )

    return {
        "title": pr_data.get("title"),
        "body": (pr_data.get("body") or "")[:_MAX_BODY_CHARS],
        "state": pr_data.get("state"),
        "merged_at": pr_data.get("merged_at"),
        "url": pr_data.get("html_url") or ref.get("url"),
        "files_changed": files_changed,
        "enrichment": "github_api",
    }


def enrich_github_pr(ref: dict[str, Any], token: str | None = None) -> dict[str, Any]:
    """Attach PR title/body/files when gh CLI or GitHub API is available."""
    enriched = dict(ref)
    details = _enrich_via_gh_cli(ref)
    if details is None:
        details = _enrich_via_github_api(ref, token)
    if details:
        enriched.update(details)
    else:
        enriched["enrichment"] = "url_only"
    return enriched


def collect_fix_pull_requests(
    issue_key: str,
    issue: dict[str, Any],
    jira_client: Any,
) -> list[dict[str, Any]]:
    """
    Collect fix PRs from JIRA remote links, description, and comments.

    Returns enriched PR dicts (title/body/files when GitHub access is available).
    """
    refs: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add_ref(ref: dict[str, Any]) -> None:
        key = _pr_ref_key(ref)
        if key in seen:
            return
        seen.add(key)
        refs.append(ref)

    for link in fetch_jira_remote_links(jira_client.jira, issue_key):
        application = (link.get("application") or {}).get("name") or "remote_link"
        jira_title = (link.get("object") or {}).get("title")
        for chunk in _urls_from_remote_link(link):
            for ref in extract_github_pr_refs_from_text(chunk):
                ref = dict(ref)
                ref["source"] = f"jira_remote_link:{application}"
                if jira_title:
                    ref["jira_link_title"] = jira_title
                _add_ref(ref)

    texts = [issue.get("description") or ""]
    for comment in issue.get("comments") or []:
        texts.append(comment.get("body") or "")

    for text in texts:
        for ref in extract_github_pr_refs_from_text(text):
            ref = dict(ref)
            ref["source"] = "jira_text"
            _add_ref(ref)

    if not refs:
        log.info("No fix pull requests found for %s", issue_key)
        return []

    token = _resolve_github_token()
    enriched: list[dict[str, Any]] = []
    for ref in refs:
        try:
            enriched.append(enrich_github_pr(ref, token))
        except Exception as exc:
            log.warning("PR enrichment failed for %s: %s", ref.get("url"), exc)
            enriched.append(
                {**ref, "enrichment": "failed", "enrichment_error": str(exc)}
            )

    log.info("Found %d fix pull request(s) for %s", len(enriched), issue_key)
    return enriched


def format_pull_requests_for_prompt(pull_requests: list[dict[str, Any]]) -> str:
    """Format linked PRs for Claude repro/verification prompts."""
    if not pull_requests:
        return "(no linked fix pull requests found)"

    blocks: list[str] = []
    for index, pr in enumerate(pull_requests, start=1):
        lines = [f"--- Fix PR {index} ---"]
        lines.append(f"URL: {pr.get('url', '')}")
        if pr.get("source"):
            lines.append(f"Discovered via: {pr['source']}")
        if pr.get("jira_link_title"):
            lines.append(f"JIRA link title: {pr['jira_link_title']}")
        if pr.get("title"):
            lines.append(f"Title: {pr['title']}")
        if pr.get("state"):
            lines.append(f"State: {pr['state']}")
        if pr.get("merged_at"):
            lines.append(f"Merged at: {pr['merged_at']}")
        body = (pr.get("body") or "").strip()
        if body:
            lines.append(f"Description:\n{body}")
        files = pr.get("files_changed") or []
        if files:
            lines.append("Files changed:")
            for file_info in files[:_MAX_FILES]:
                path = file_info.get("path") or "?"
                additions = file_info.get("additions", 0)
                deletions = file_info.get("deletions", 0)
                lines.append(f"  - {path} (+{additions}/-{deletions})")
        blocks.append("\n".join(lines))

    return "\n\n".join(blocks)
