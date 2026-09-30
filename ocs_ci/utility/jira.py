import configparser
import os
from logging import getLogger

import yaml
from atlassian import Jira
from bs4 import BeautifulSoup

from ocs_ci.framework import config
from ocs_ci.ocs.constants import AUTHYAML, DATA_DIR

log = getLogger(__name__)

ON_QA_STATUS = "ON_QA"
DEFAULT_JIRA_PROJECT = "DFBUGS"
TARGET_VERSION_FIELD = "Target Version"
TARGET_RELEASE_FIELD = "Target Release"
# Field ids on redhat.atlassian.net for DFBUGS. JQL uses the field names above.
TARGET_VERSION_FIELD_ID = "customfield_10855"
TARGET_RELEASE_FIELD_ID = "customfield_10886"
ON_QA_SEARCH_FIELDS = (
    "summary",
    "status",
    "fixVersions",
    TARGET_VERSION_FIELD_ID,
    TARGET_RELEASE_FIELD_ID,
)
VERIFICATION_ISSUE_FIELDS = ON_QA_SEARCH_FIELDS + (
    "description",
    "environment",
    "versions",
)
_SECTION_HEADINGS = {
    "verification steps": "verification_steps",
    "steps to verify": "verification_steps",
    "how to verify": "verification_steps",
    "verification": "verification_steps",
    "test steps": "verification_steps",
    "steps to reproduce": "steps_to_reproduce",
    "expected results": "expected_results",
    "expected result": "expected_results",
}
_BLOCK_NODE_TYPES = {"paragraph", "heading", "blockquote", "listItem", "codeBlock"}


class JiraHelper:
    """
    Simple Jira integration for OCS-CI.
    Requires a dict with keys: url, username, password.
    """

    def __init__(self):
        """
        Initialize JiraHelper.

        Provide credentials in config.AUTH.jira, in data/auth.yaml under jira,
        or in /etc/jira.cfg. data/auth.yaml may use email and token in place of
        username and password.
        """
        jira_auth = resolve_jira_auth()
        self.url = jira_auth["url"]
        self.username = jira_auth["username"]
        self.password = jira_auth["password"]
        self.visibility = jira_auth.get(
            "visibility", {"type": "group", "value": "Red Hat Employee"}
        )

        log.debug(f"Initializing Jira: {self.url}")
        self.jira = Jira(
            url=self.url, username=self.username, password=self.password, cloud=True
        )

    @staticmethod
    def _load_from_file(path: str) -> dict:
        """
        Load an INI config file with a [DEFAULT] section
        containing url, username, password and optionally other values.

        Args:
            path (str): Path to the INI config file

        Returns:
            dict: A dictionary containing the URL, username and password for the Jira instance

        """
        config = configparser.ConfigParser()
        config.read(path)

        section = config["DEFAULT"]

        return {
            "url": section["url"],
            "username": section["username"],
            "password": section["password"],
        }

    def get_issue(self, issue_key: str) -> dict:
        """
        Return complete JSON of the Jira issue.

        Args:
            issue_key (str): The key of the Jira issue e.g. 'DFBUGS-2781'

        Returns:
            dict: A dictionary containing the complete JSON of the Jira issue

        """
        log.debug(f"Fetching Jira issue {issue_key}")
        return self.jira.issue(issue_key)

    def add_comment(self, issue_key: str, text: str):
        """
        Add a comment to an issue.

        Args:
            issue_key (str): The key of the Jira issue e.g. 'DFBUGS-2781'
            text (str): The text of the comment

        Returns:
            dict: A dictionary containing the complete JSON of the Jira issue

        """
        log.info(f"Adding comment to {issue_key}: {text}")
        return self.jira.issue_add_comment(issue_key, text, visibility=self.visibility)

    def search_on_qa(self, version, project=DEFAULT_JIRA_PROJECT):
        """
        List issues in ON_QA for one target, release, or fix version.

        Args:
            version (str): Version name, for example odf-5.0.
            project (str): Jira project key. Defaults to DFBUGS.

        Returns:
            list: One dict per issue, with key, summary, status, and versions.

        """
        jql = on_qa_jql(version, project)
        log.info(f"Searching ON_QA issues with JQL: {jql}")
        issues = self.jira.enhanced_jql_get_list_of_tickets(
            jql, fields=list(ON_QA_SEARCH_FIELDS)
        )
        return [issue_summary(issue) for issue in issues]

    def issue_for_verification(self, issue_key):
        """
        Return the description, headed sections, and comments for one issue.

        Args:
            issue_key (str): Jira issue key, for example DFBUGS-10425.

        Returns:
            dict: Issue text a reviewer uses to find verification steps.

        """
        log.info(f"Fetching verification content for {issue_key}")
        issue = self.jira.issue(
            issue_key,
            fields=",".join(VERIFICATION_ISSUE_FIELDS),
            expand="renderedFields",
        )
        comments = self._all_comments(issue_key)
        remote_links = self._remote_links(issue_key)
        return issue_verification_payload(issue, comments, remote_links=remote_links)

    def _all_comments(self, issue_key):
        """
        Return every comment on an issue.

        Args:
            issue_key (str): Jira issue key.

        Returns:
            list: Comment objects from the Jira REST API.

        """
        url = f"{self.jira.resource_url('issue')}/{issue_key}/comment"
        start = 0
        page_size = 100
        collected = []
        while True:
            page = self.jira.get(
                url,
                params={
                    "startAt": start,
                    "maxResults": page_size,
                    "expand": "renderedBody",
                },
            )
            batch = page.get("comments") or []
            collected.extend(batch)
            total = page.get("total", len(collected))
            start += len(batch)
            if not batch or start >= total:
                break
        return collected

    def _remote_links(self, issue_key):
        """
        Return remote links on an issue, including GitHub pull requests.

        Args:
            issue_key (str): Jira issue key.

        Returns:
            list: Remote-link objects from the Jira REST API.

        """
        url = f"{self.jira.resource_url('issue')}/{issue_key}/remotelink"
        links = self.jira.get(url)
        if isinstance(links, list):
            return links
        return []


def resolve_jira_auth():
    """
    Find Jira URL, username, and password.

    config.AUTH.jira wins. Otherwise the jira section of data/auth.yaml is
    used, then /etc/jira.cfg. email and token are accepted as username and
    password.

    Returns:
        dict: url, username, password, and visibility when set.

    Raises:
        ValueError: No complete credential set was found.

    """
    for candidate in (
        _normalize_jira_auth(config.AUTH.get("jira")),
        _jira_auth_from_auth_yaml(),
        _jira_auth_from_cfg("/etc/jira.cfg"),
    ):
        if candidate:
            return candidate
    raise ValueError(
        "Jira credentials not provided. Set config.AUTH.jira, data/auth.yaml "
        "jira.url with jira.email and jira.token, or /etc/jira.cfg."
    )


def _normalize_jira_auth(raw):
    """
    Accept username/password or email/token.

    Args:
        raw (dict): Credential mapping from config or auth.yaml.

    Returns:
        dict: url, username, and password, or None when the mapping is incomplete.

    """
    if not isinstance(raw, dict):
        return None
    url = raw.get("url")
    username = raw.get("username") or raw.get("email")
    password = raw.get("password") or raw.get("token")
    if not url or not username or not password:
        return None
    normalized = {"url": url, "username": username, "password": password}
    if raw.get("visibility"):
        normalized["visibility"] = raw["visibility"]
    return normalized


def _jira_auth_from_auth_yaml():
    """
    Read the jira section from data/auth.yaml.

    Returns:
        dict: Normalized credentials, or None when the file or section is absent.

    """
    auth_file = os.path.join(DATA_DIR, AUTHYAML)
    try:
        with open(auth_file, encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
    except (OSError, yaml.YAMLError):
        return None
    if not isinstance(loaded, dict):
        return None
    raw = loaded.get("jira")
    if not raw and isinstance(loaded.get("AUTH"), dict):
        raw = loaded["AUTH"].get("jira")
    return _normalize_jira_auth(raw)


def _jira_auth_from_cfg(path):
    """
    Read Jira credentials from an INI file.

    Args:
        path (str): Path to a config file with a DEFAULT section.

    Returns:
        dict: Normalized credentials, or None when the file is missing.

    """
    if not os.path.exists(path):
        return None
    return _normalize_jira_auth(JiraHelper._load_from_file(path))


def on_qa_jql(version, project=DEFAULT_JIRA_PROJECT):
    """
    Build JQL for ON_QA issues of one version.

    An issue matches when Target Version, Target Release, or Fix Version equals
    the given version name.

    Args:
        version (str): Version name, for example odf-5.0.
        project (str): Jira project key.

    Returns:
        str: JQL query.

    """
    version_name = str(version).strip()
    project_key = str(project).strip()
    if not version_name:
        raise ValueError("version is required")
    if not project_key:
        raise ValueError("project is required")
    version_jql = _jql_string(version_name)
    return (
        f"project = {_jql_string(project_key)} AND status = {_jql_string(ON_QA_STATUS)} "
        f"AND ({_jql_string(TARGET_VERSION_FIELD)} = {version_jql} "
        f"OR {_jql_string(TARGET_RELEASE_FIELD)} = {version_jql} "
        f"OR fixVersion = {version_jql}) "
        "ORDER BY key ASC"
    )


def issue_summary(issue):
    """
    Reduce a Jira search hit to the fields needed to choose issues to open.

    Args:
        issue (dict): One issue from a JQL search.

    Returns:
        dict: key, summary, status, and versions.

    """
    fields = issue.get("fields") or {}
    status = fields.get("status") or {}
    return {
        "key": issue.get("key"),
        "summary": fields.get("summary") or "",
        "status": status.get("name") if isinstance(status, dict) else status,
        "versions": issue_versions(fields),
    }


def issue_verification_payload(issue, comments=None, remote_links=None):
    """
    Collect the text that describes how to verify one issue.

    Args:
        issue (dict): Jira issue JSON, optionally with renderedFields.
        comments (list): Comment objects. Rendered HTML is preferred.
        remote_links (list): Remote-link objects. GitHub pull requests are
            copied into git_prs.

    Returns:
        dict: Description, versions, environment, headed sections, and comments.

    """
    fields = issue.get("fields") or {}
    rendered = issue.get("renderedFields") or {}
    if rendered.get("description"):
        description = html_to_text(rendered.get("description"))
    else:
        description = adf_to_text(fields.get("description")).strip()
    comment_entries = []
    for comment in comments or []:
        author = comment.get("author") or {}
        comment_entries.append(
            {
                "author": author.get("displayName"),
                "created": comment.get("created"),
                "body": comment_body_text(comment),
            }
        )
    return {
        "key": issue.get("key"),
        "summary": fields.get("summary") or "",
        "status": (fields.get("status") or {}).get("name"),
        "affected_version": _version_name_list(fields.get("versions")),
        "fix_version": _version_name_list(fields.get("fixVersions")),
        "versions": issue_versions(fields),
        "environment": _environment_text(fields, rendered),
        "git_prs": github_pull_requests(remote_links),
        "description": description.strip(),
        "sections": extract_headed_sections(description),
        "comments": comment_entries,
    }


def github_pull_requests(remote_links):
    """
    Keep GitHub pull request URLs from Jira remote links.

    Args:
        remote_links (list): Remote-link objects.

    Returns:
        list: Pull request URLs, in first-seen order.

    """
    found = []
    for remote in remote_links or []:
        obj = remote.get("object") or {}
        url = obj.get("url") or ""
        if "github.com" not in url or "/pull/" not in url:
            continue
        if url not in found:
            found.append(url)
    return found


def issue_versions(fields):
    """
    Collect fix, target, and release version names from issue fields.

    Args:
        fields (dict): Jira issue fields.

    Returns:
        list: Version names, in first-seen order.

    """
    names = []
    for key in ("fixVersions", TARGET_VERSION_FIELD_ID, TARGET_RELEASE_FIELD_ID):
        names.extend(_names_from_versions(fields.get(key)))
    seen = []
    for name in names:
        if name not in seen:
            seen.append(name)
    return seen


def extract_headed_sections(text):
    """
    Split issue text into verification-related sections.

    Args:
        text (str): Plain-text description.

    Returns:
        dict: Section name to body. Known names are verification_steps,
            steps_to_reproduce, and expected_results.

    """
    sections = {}
    current = None
    buffer = []

    def flush():
        nonlocal buffer
        if current is None:
            buffer = []
            return
        body = "\n".join(buffer).strip()
        if body:
            existing = sections.get(current)
            sections[current] = f"{existing}\n{body}".strip() if existing else body
        buffer = []

    for line in (text or "").splitlines():
        key = _heading_key(line)
        if key:
            flush()
            current = key
            continue
        if current is not None:
            buffer.append(line)
    flush()
    return sections


def html_to_text(html):
    """
    Convert rendered Jira HTML to plain text.

    Args:
        html (str): HTML from renderedFields or renderedBody.

    Returns:
        str: Text with one block per line.

    """
    if not html:
        return ""
    if not isinstance(html, str):
        return adf_to_text(html).strip()
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text("\n", strip=True)


def adf_to_text(node):
    """
    Convert an Atlassian document to plain text.

    Args:
        node (dict): ADF node, a list of nodes, or a plain string.

    Returns:
        str: Text content of the document.

    """
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(adf_to_text(item) for item in node)
    if not isinstance(node, dict):
        return ""
    node_type = node.get("type")
    if node_type == "text":
        return node.get("text") or ""
    if node_type == "hardBreak":
        return "\n"
    text = adf_to_text(node.get("content") or [])
    if node_type in _BLOCK_NODE_TYPES and text and not text.endswith("\n"):
        text += "\n"
    return text


def comment_body_text(comment):
    """
    Return the plain text of one Jira comment.

    Args:
        comment (dict): Comment JSON. renderedBody is used when present.

    Returns:
        str: Comment text.

    """
    if comment.get("renderedBody"):
        return html_to_text(comment.get("renderedBody")).strip()
    return adf_to_text(comment.get("body")).strip()


def _jql_string(value):
    """
    Quote a JQL string value.

    Args:
        value (str): Raw field or version text.

    Returns:
        str: Double-quoted JQL string.

    """
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _environment_text(fields, rendered):
    """
    Return the Jira environment field as plain text.

    Args:
        fields (dict): Issue fields.
        rendered (dict): Rendered issue fields.

    Returns:
        str: Environment text, or an empty string when the field is empty.

    """
    rendered_env = (rendered or {}).get("environment")
    if rendered_env:
        return html_to_text(rendered_env).strip()
    return adf_to_text((fields or {}).get("environment")).strip()


def _version_name_list(value):
    """
    Return unique version names from one Jira version field.

    Args:
        value: A version object, a list of version objects, or empty.

    Returns:
        list: Version names.

    """
    names = []
    for name in _names_from_versions(value):
        if name not in names:
            names.append(name)
    return names


def _names_from_versions(value):
    """
    Read version names from a Jira version field.

    Args:
        value: A version object, a list of version objects, or empty.

    Returns:
        list: Version name strings.

    """
    if isinstance(value, dict):
        name = value.get("name")
        return [name] if name else []
    if isinstance(value, list):
        names = []
        for item in value:
            names.extend(_names_from_versions(item))
        return names
    return []


def _heading_key(line):
    """
    Map a description line to a section name when it is a known heading.

    Args:
        line (str): One line of plain text.

    Returns:
        str: Section name, or None when the line is not a heading.

    """
    cleaned = line.strip().rstrip(":").strip().lower()
    return _SECTION_HEADINGS.get(cleaned)
