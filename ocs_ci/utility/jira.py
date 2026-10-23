import configparser
import os
import re
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
    "issuetype",
    "priority",
    "resolution",
    "labels",
    "components",
    "attachment",
    "issuelinks",
    "parent",
)
MAX_SOURCE_ISSUES = 3
_SOURCE_RELATIONS = {
    "clones",
    "parent",
    "is a backport of",
    "backports",
    "cloned from",
}
_SOURCE_CONTEXT = re.compile(
    r"(?:backport of|original issue|cloned from|clone of|cherry-?pick of)"
    r"\s*:?\s*(?:https?://\S*?/browse/)?"
    r"([A-Z][A-Z0-9]+-\d+)",
    re.IGNORECASE,
)
_COMMAND_LINE = re.compile(r"^(?:oc|kubectl|ceph|rbd|rados)\b")
_NOTE_PATTERNS = (
    (
        "command",
        re.compile(
            r"(?m)^(?:\$ |# )?(?:oc|kubectl|ceph|rbd|rados)\b"
            r"|\b(?:oc|kubectl) (?:get|describe|exec|logs|delete|apply|create|adm)\b"
        ),
    ),
    (
        "verification steps",
        re.compile(
            r"\b(how to verify|steps to verify|verification steps)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "environment",
        re.compile(
            r"\b(?:vmware|vsphere|baremetal|bare metal|lso|regional[- ]dr|"
            r"metro[- ]dr|external mode|provider mode|ipi|upi|"
            r"arbiter|compact cluster)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "test result",
        re.compile(
            r"\b(verified on|tested on|reproduced|cannot reproduce|still fails|"
            r"no longer fails|confirmed (?:the )?fix|check is present)\b",
            re.IGNORECASE,
        ),
    ),
    ("workaround", re.compile(r"\bworkaround\b", re.IGNORECASE)),
    (
        "pull request",
        re.compile(r"github\.com/\S+/pull/\d+", re.IGNORECASE),
    ),
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

    def __init__(self, auth=None):
        """
        Initialize JiraHelper.

        Provide credentials in config.AUTH.jira, in data/auth.yaml under jira,
        or in /etc/jira.cfg. data/auth.yaml may use email and token in place of
        username and password. Pass auth to use a credential set that was
        already resolved, such as agents_credentials.jira for the agent.

        Args:
            auth (dict): url, username, and password. When omitted, credentials
                are resolved from config, data/auth.yaml, or /etc/jira.cfg.
        """
        jira_auth = auth if auth is not None else resolve_jira_auth()
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

    def search_on_qa(self, version, project=DEFAULT_JIRA_PROJECT, release=None):
        """
        List issues in ON_QA for one target, release, or fix version.

        When release is set, only issues whose Target Release equals that
        name are returned.

        Args:
            version (str): Version name, for example odf-5.0. Ignored for the
                match when release is set.
            project (str): Jira project key. Defaults to DFBUGS.
            release (str): Target Release name. When set, this selects the bugs.

        Returns:
            list: One dict per issue, with key, summary, status, and versions.

        """
        jql = on_qa_jql(version, project, release=release)
        log.info(f"Searching ON_QA issues with JQL: {jql}")
        issues = self.jira.enhanced_jql_get_list_of_tickets(
            jql, fields=list(ON_QA_SEARCH_FIELDS)
        )
        return [issue_summary(issue) for issue in issues]

    def issue_for_verification(self, issue_key):
        """
        Return the description, headed sections, comments, and source issues.

        A backport or clone also includes the original issue it was taken
        from, so verification steps that live only on that issue are present.
        Source issues are not followed further.

        Args:
            issue_key (str): Jira issue key, for example DFBUGS-10425.

        Returns:
            dict: Issue text a reviewer uses to find verification steps.

        """
        log.info(f"Fetching verification content for {issue_key}")
        payload = self._fetch_issue_bundle(issue_key)
        payload["source_issues"] = []
        for key in source_issue_keys(payload)[:MAX_SOURCE_ISSUES]:
            log.info(f"Fetching source issue {key} for {issue_key}")
            try:
                source = self._fetch_issue_bundle(key)
            except Exception as error:
                log.warning(
                    f"Could not fetch source issue {key} for {issue_key}: {error}"
                )
                payload["source_issues"].append({"key": key, "error": str(error)})
                continue
            payload["source_issues"].append(source_issue_summary(source))
        payload["parent_issues"] = parent_issues_for_report(payload)
        return payload

    def _fetch_issue_bundle(self, issue_key):
        """
        Read one issue, its comments, and its remote links.

        Args:
            issue_key (str): Jira issue key.

        Returns:
            dict: Verification payload without followed source issues.

        """
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


def resolve_agent_jira_auth():
    """
    Find Jira credentials for the agent.

    The agent uses agents_credentials.jira in data/auth.yaml. email and token
    are accepted as username and password. The top-level jira section is left
    for the rest of ocs-ci.

    Returns:
        dict: url, username, password, and visibility when set.

    Raises:
        ValueError: agents_credentials.jira is missing or incomplete.

    """
    loaded = _load_auth_yaml()
    credentials = loaded.get("agents_credentials") or {}
    raw = credentials.get("jira") if isinstance(credentials, dict) else None
    normalized = _normalize_jira_auth(raw)
    if normalized:
        return normalized
    raise ValueError(
        "Jira credentials for the agent were not provided. Set "
        "agents_credentials.jira.url, agents_credentials.jira.email, and "
        "agents_credentials.jira.token in data/auth.yaml."
    )


def _load_auth_yaml():
    """
    Read data/auth.yaml.

    Returns:
        dict: Parsed file, or an empty dict when the file is missing or invalid.

    """
    auth_file = os.path.join(DATA_DIR, AUTHYAML)
    try:
        with open(auth_file, encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
    except (OSError, yaml.YAMLError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    return loaded


def _jira_auth_from_auth_yaml():
    """
    Read the jira section from data/auth.yaml.

    Returns:
        dict: Normalized credentials, or None when the file or section is absent.

    """
    loaded = _load_auth_yaml()
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


def on_qa_jql(version, project=DEFAULT_JIRA_PROJECT, release=None):
    """
    Build JQL for ON_QA issues of one version or one Target Release.

    Without a release, an issue matches when Target Version, Target Release,
    or Fix Version equals the version name. With a release, an issue matches
    only when Target Release equals that name.

    Args:
        version (str): Version name, for example odf-5.0.
        project (str): Jira project key.
        release (str): Target Release name. Selects bugs for that release.

    Returns:
        str: JQL query.

    Raises:
        ValueError: Neither a version nor a release was given.

    """
    release_name = str(release or "").strip()
    version_name = str(version or "").strip()
    project_key = str(project or "").strip()
    if not project_key:
        raise ValueError("project is required")
    if release_name:
        match = f"{_jql_string(TARGET_RELEASE_FIELD)} = {_jql_string(release_name)}"
    elif version_name:
        version_jql = _jql_string(version_name)
        match = (
            f"({_jql_string(TARGET_VERSION_FIELD)} = {version_jql} "
            f"OR {_jql_string(TARGET_RELEASE_FIELD)} = {version_jql} "
            f"OR fixVersion = {version_jql})"
        )
    else:
        raise ValueError("version or release is required")
    return (
        f"project = {_jql_string(project_key)} AND status = {_jql_string(ON_QA_STATUS)} "
        f"AND {match} "
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
        dict: Description, versions, environment, links, headed sections,
            comments, and the comments that change how the fix is checked.

    """
    fields = issue.get("fields") or {}
    rendered = issue.get("renderedFields") or {}
    if rendered.get("description"):
        description = html_to_text(rendered.get("description"))
    else:
        description = adf_to_text(fields.get("description")).strip()
    description = description.strip()
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
    sections = extract_headed_sections(description)
    payload = {
        "key": issue.get("key"),
        "summary": fields.get("summary") or "",
        "status": (fields.get("status") or {}).get("name"),
        "issue_type": _named(fields.get("issuetype")),
        "priority": _named(fields.get("priority")),
        "resolution": _named(fields.get("resolution")),
        "labels": [label for label in (fields.get("labels") or []) if label],
        "components": _component_names(fields.get("components")),
        "attachments": _attachment_names(fields.get("attachment")),
        "affected_version": _version_name_list(fields.get("versions")),
        "fix_version": _version_name_list(fields.get("fixVersions")),
        "versions": issue_versions(fields),
        "environment": _environment_text(fields, rendered),
        "links": issue_links(fields),
        "git_prs": github_pull_requests(remote_links),
        "description": description,
        "sections": sections,
        "comments": comment_entries,
        "verification_notes": verification_notes(comment_entries),
    }
    payload["parent_issues"] = parent_issues_for_report(payload)
    return payload


def verification_notes(comments):
    """
    Keep comments that change how a fix is checked.

    A comment is kept when it has a command, a verification heading, an
    environment, a test result, a workaround, or a pull request. Status-only
    and release-process comments are left out.

    Args:
        comments (list): Comment dicts with author, created, and body.

    Returns:
        list: Notes with author, created, reasons, body, and commands when
            the comment contains a command.

    """
    notes = []
    for comment in comments or []:
        body = (comment.get("body") or "").strip()
        if not body:
            continue
        reasons = _note_reasons(body)
        if extract_headed_sections(body) and "verification steps" not in reasons:
            reasons.append("verification steps")
        if not reasons:
            continue
        note = {
            "author": comment.get("author"),
            "created": comment.get("created"),
            "reasons": reasons,
            "body": body,
        }
        commands = _commands_in_text(body)
        if commands:
            note["commands"] = commands
        notes.append(note)
    return notes


def parent_issues_for_report(payload):
    """
    List parent and original issues with the status to show on the report.

    A Jira parent, a cloned issue, and an original bug named in the
    description are included. A child clone is not.

    Args:
        payload (dict): Verification payload, including links and source_issues.

    Returns:
        list: key, summary, status, and relation for each parent. Empty when
            the issue has none.

    """
    parents = []
    seen = set()
    sources = {}
    for source in payload.get("source_issues") or []:
        key = source.get("key")
        if key:
            sources[key] = source
    for link in payload.get("links") or []:
        relation = (link.get("relation") or "").strip()
        key = link.get("key")
        if not key or relation.lower() not in _SOURCE_RELATIONS or key in seen:
            continue
        seen.add(key)
        entry = _parent_entry(key, link.get("summary"), link.get("status"), relation)
        source = sources.get(key) or {}
        if source.get("summary"):
            entry["summary"] = source["summary"]
        if source.get("status"):
            entry["status"] = source["status"]
        parents.append(entry)
    for key, source in sources.items():
        if key in seen:
            continue
        seen.add(key)
        parents.append(
            _parent_entry(key, source.get("summary"), source.get("status"), "original")
        )
    return parents


def source_issue_keys(payload):
    """
    Return the original issues a backport or clone was taken from.

    Args:
        payload (dict): Verification payload for the issue being read.

    Returns:
        list: Issue keys, in first-seen order, excluding the payload key.

    """
    own = payload.get("key")
    keys = []
    for link in payload.get("links") or []:
        relation = (link.get("relation") or "").lower()
        if relation in _SOURCE_RELATIONS:
            _append_key(keys, link.get("key"), own)
    for match in _SOURCE_CONTEXT.finditer(payload.get("description") or ""):
        _append_key(keys, match.group(1), own)
    return keys


def source_issue_summary(payload):
    """
    Reduce a followed issue to the text needed to verify the backport.

    Args:
        payload (dict): Verification payload of the original issue.

    Returns:
        dict: Summary, environment, sections, notes, and a capped description.

    """
    description = payload.get("description") or ""
    if len(description) > 4000:
        description = description[:4000]
    return {
        "key": payload.get("key"),
        "summary": payload.get("summary") or "",
        "status": payload.get("status"),
        "environment": payload.get("environment") or "",
        "affected_version": payload.get("affected_version") or [],
        "fix_version": payload.get("fix_version") or [],
        "git_prs": payload.get("git_prs") or [],
        "sections": payload.get("sections") or {},
        "description": description,
        "verification_notes": payload.get("verification_notes") or [],
    }


def issue_links(fields):
    """
    Return parent and issue-link relations.

    Args:
        fields (dict): Jira issue fields.

    Returns:
        list: relation, key, summary, and status for each linked issue.

    """
    links = []
    parent = (fields or {}).get("parent") or {}
    if parent.get("key"):
        parent_fields = parent.get("fields") or {}
        links.append(
            {
                "relation": "parent",
                "key": parent.get("key"),
                "summary": parent_fields.get("summary") or "",
                "status": (parent_fields.get("status") or {}).get("name"),
            }
        )
    for link in (fields or {}).get("issuelinks") or []:
        link_type = link.get("type") or {}
        if link.get("outwardIssue"):
            linked = link["outwardIssue"]
            relation = link_type.get("outward") or link_type.get("name") or ""
        elif link.get("inwardIssue"):
            linked = link["inwardIssue"]
            relation = link_type.get("inward") or link_type.get("name") or ""
        else:
            continue
        linked_fields = linked.get("fields") or {}
        links.append(
            {
                "relation": relation,
                "key": linked.get("key"),
                "summary": linked_fields.get("summary") or "",
                "status": (linked_fields.get("status") or {}).get("name"),
            }
        )
    return links


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


def _note_reasons(body):
    """
    List why a comment is useful for verification.

    Args:
        body (str): Plain-text comment.

    Returns:
        list: Reason names, in pattern order.

    """
    reasons = []
    for name, pattern in _NOTE_PATTERNS:
        if pattern.search(body) and name not in reasons:
            reasons.append(name)
    return reasons


def _commands_in_text(text):
    """
    Return command lines from a comment or description.

    Args:
        text (str): Plain text.

    Returns:
        list: Commands, in first-seen order.

    """
    found = []
    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped.startswith(("$ ", "# ")):
            stripped = stripped[2:].strip()
        if _COMMAND_LINE.match(stripped) and stripped not in found:
            found.append(stripped)
    return found


def _parent_entry(key, summary, status, relation):
    """
    Build one parent-issue row for a verification report.

    Args:
        key (str): Parent issue key.
        summary (str): Parent summary.
        status (str): Parent status name.
        relation (str): How this issue relates to the parent.

    Returns:
        dict: key, summary, status, and relation.

    """
    return {
        "key": key,
        "summary": summary or "",
        "status": status or "",
        "relation": relation or "",
    }


def _append_key(keys, key, own):
    """
    Add an issue key when it is new and is not the issue being read.

    Args:
        keys (list): Keys collected so far.
        key (str): Candidate issue key.
        own (str): Key of the issue being read.

    """
    if key and key != own and key not in keys:
        keys.append(key)


def _named(value):
    """
    Return the name of a Jira object field.

    Args:
        value (dict): Field such as issuetype, priority, or resolution.

    Returns:
        str: Name, or an empty string when the field is empty.

    """
    if isinstance(value, dict):
        return value.get("name") or ""
    return ""


def _component_names(components):
    """
    Return component names.

    Args:
        components (list): Jira component objects.

    Returns:
        list: Component names.

    """
    names = []
    for component in components or []:
        name = component.get("name") if isinstance(component, dict) else component
        if name and name not in names:
            names.append(name)
    return names


def _attachment_names(attachments):
    """
    Return attachment file names.

    Args:
        attachments (list): Jira attachment objects.

    Returns:
        list: File names.

    """
    names = []
    for attachment in attachments or []:
        name = attachment.get("filename") if isinstance(attachment, dict) else ""
        if name and name not in names:
            names.append(name)
    return names


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
