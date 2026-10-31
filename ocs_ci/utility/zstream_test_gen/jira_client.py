"""
Jira client for z-stream bug collection.

Extends the existing JiraHelper to query z-stream bugs, extract structured
metadata, and post comments with generated PR links.
"""

import logging
import re
from typing import Optional

from ocs_ci.utility.zstream_test_gen.models import (
    BugInfo,
    Component,
)
from ocs_ci.utility.zstream_test_gen.config import GeneratorConfig

log = logging.getLogger(__name__)

# Patterns to identify components from bug summaries
COMPONENT_PATTERNS = [
    (r"ceph-csi-rbd|cephcsi.*rbd|rbd.*csi", Component.CEPH_CSI_RBD),
    (r"ceph-csi.*cephfs|cephfs.*csi", Component.CEPH_CSI_CEPHFS),
    (r"ceph-csi|cephcsi", Component.CEPH_CSI),
    (r"noobaa|mcg|multi.cloud|bucket", Component.NOOBAA),
    (r"ramen|disaster.recov|failover|relocat|regional.dr|metro.dr", Component.RAMEN),
    (r"ocs.operator|ocs-operator", Component.OCS_OPERATOR),
    (r"odf.operator|odf-operator", Component.ODF_OPERATOR),
]

# Pattern to extract upstream PR URLs from web links
GITHUB_PR_PATTERN = re.compile(r"https?://github\.com/([^/]+/[^/]+)/pull/(\d+)")


class ZStreamJiraClient:
    """
    Jira client specialized for z-stream bug queries.

    Args:
        cfg: Generator configuration with Jira credentials.

    """

    def __init__(self, cfg: GeneratorConfig):
        self.cfg = cfg

        try:
            from atlassian import Jira
        except ImportError:
            raise ImportError(
                "The 'atlassian-python-api' package is required. "
                "Install it with: pip install atlassian-python-api"
            )

        self.jira = Jira(
            url=cfg.jira.url,
            username=cfg.jira.username,
            password=cfg.jira.api_token,
            cloud=True,
        )
        log.info("Connected to Jira at %s", cfg.jira.url)

    def query_zstream_bugs(self, fix_version: str, max_results: int = 100) -> list:
        """
        Query all bugs for a given z-stream fix version.

        Args:
            fix_version: The fix version string, e.g. "odf-4.22.1".
            max_results: Maximum number of bugs to return.

        Returns:
            list[BugInfo]: List of bug information objects.

        """
        jql = (
            f"project = {self.cfg.jira.project_key} "
            f'AND fixVersion = "{fix_version}" '
            f"AND type = Bug "
            f"ORDER BY key ASC"
        )
        log.info("Querying Jira: %s", jql)

        issues = self._search_issues(jql, max_results=max_results)
        log.info("Found %d bugs for fix version %s", len(issues), fix_version)

        bugs = []
        for issue in issues:
            bug = self._parse_issue(issue)
            bugs.append(bug)
            log.debug(
                "Parsed bug %s: component=%s, upstream_pr=%s",
                bug.bug_id,
                bug.component.value,
                bug.upstream_pr_url,
            )

        return bugs

    def get_bug_details(self, bug_id: str) -> BugInfo:
        """
        Fetch complete details for a single bug.

        Args:
            bug_id: The Jira issue key, e.g. "DFBUGS-10065".

        Returns:
            BugInfo: Complete bug information.

        """
        issue = self.jira.issue(bug_id)
        return self._parse_issue(issue)

    def get_bug_comments(self, bug_id: str) -> list:
        """
        Fetch all comments for a bug.

        Args:
            bug_id: The Jira issue key.

        Returns:
            list[dict]: List of comment dictionaries with 'author' and 'body'.

        """
        try:
            comments_data = self.jira.issue_get_comments(bug_id)
            comments = []
            for comment in comments_data.get("comments", []):
                comments.append(
                    {
                        "author": comment.get("author", {}).get(
                            "displayName", "Unknown"
                        ),
                        "body": comment.get("body", ""),
                        "created": comment.get("created", ""),
                    }
                )
            return comments
        except Exception:
            log.warning("Failed to fetch comments for %s", bug_id, exc_info=True)
            return []

    def get_clone_chain_versions(self, bug: BugInfo) -> list:
        """
        Follow the clone chain to find all ODF versions this bug affects.

        Walks up to the original bug, then collects fix versions from all
        sibling clones (backports to other ODF versions).

        Args:
            bug: The bug to trace.

        Returns:
            list[str]: Sorted list of ODF version strings (e.g., ["4.18", "4.20", "4.22"]).

        """
        versions = set()

        # Collect from this bug's fix versions
        for fv in bug.fix_versions:
            match = re.search(r"odf-(\d+\.\d+)", fv.lower())
            if match:
                versions.add(match.group(1))

        # If we have an original bug, query its outward clones
        original_id = bug.original_bug_id
        if original_id:
            try:
                original = self.jira.issue(
                    original_id,
                    fields=["issuelinks", "fixVersions"],
                )
                orig_fields = original.get("fields", {})

                # Collect original's fix versions
                for fv in orig_fields.get("fixVersions", []):
                    match = re.search(r"odf-(\d+\.\d+)", fv.get("name", "").lower())
                    if match:
                        versions.add(match.group(1))

                # Walk all clones of the original
                for link in orig_fields.get("issuelinks", []):
                    link_type = link.get("type", {}).get("name", "")
                    if link_type.lower() != "cloners":
                        continue
                    for direction in ("inwardIssue", "outwardIssue"):
                        linked = link.get(direction)
                        if not linked:
                            continue
                        summary = linked.get("fields", {}).get("summary", "")
                        match = re.search(r"odf-(\d+\.\d+)", summary.lower())
                        if match:
                            versions.add(match.group(1))

                log.info(
                    "Clone chain for %s (original: %s): ODF versions %s",
                    bug.bug_id,
                    original_id,
                    sorted(versions),
                )
            except Exception:
                log.warning(
                    "Failed to fetch clone chain for %s", bug.bug_id, exc_info=True
                )

        return sorted(versions)

    def add_comment(self, bug_id: str, text: str):
        """
        Add a comment to a bug with generated PR links.

        Args:
            bug_id: The Jira issue key.
            text: Comment text (supports Jira markdown).

        """
        log.info("Adding comment to %s", bug_id)
        self.jira.issue_add_comment(
            bug_id,
            text,
            visibility={"type": "group", "value": "Red Hat Employee"},
        )

    def add_label(self, bug_id: str, label: str):
        """
        Add a label to a Jira issue if not already present.

        Args:
            bug_id: The Jira issue key.
            label: The label string to add.

        """
        try:
            issue = self.jira.issue(bug_id, fields=["labels"])
            current_labels = issue.get("fields", {}).get("labels", [])
            if label not in current_labels:
                self.jira.update_issue_field(
                    bug_id,
                    {"labels": current_labels + [label]},
                )
                log.info("Added label '%s' to %s", label, bug_id)
            else:
                log.debug("Label '%s' already on %s", label, bug_id)
        except Exception:
            log.warning("Failed to add label '%s' to %s", label, bug_id, exc_info=True)

    def _search_issues(self, jql: str, max_results: int = 100) -> list:
        """
        Search Jira issues with pagination.

        Args:
            jql: JQL query string.
            max_results: Maximum results to return.

        Returns:
            list[dict]: List of issue dictionaries.

        """
        all_issues = []
        start_at = 0
        page_size = 50

        while start_at < max_results:
            result = self.jira.jql(
                jql,
                start=start_at,
                limit=min(page_size, max_results - start_at),
                fields=[
                    "summary",
                    "description",
                    "labels",
                    "fixVersions",
                    "issuelinks",
                    "comment",
                    "status",
                    "customfield_12319275",  # QA Contact (may vary by instance)
                ],
            )
            issues = result.get("issues", [])
            if not issues:
                break

            all_issues.extend(issues)
            start_at += len(issues)

            if len(issues) < page_size:
                break

        return all_issues

    def _parse_issue(self, issue: dict) -> BugInfo:
        """
        Parse a Jira issue into a BugInfo object.

        Args:
            issue: Raw Jira issue dictionary.

        Returns:
            BugInfo: Parsed bug information.

        """
        fields = issue.get("fields", {})
        bug_id = issue.get("key", "")
        summary = fields.get("summary", "")

        # Extract component from summary
        component = self._detect_component(summary)

        # Extract fix versions
        fix_versions = [fv.get("name", "") for fv in fields.get("fixVersions", [])]

        # Extract linked issues and find original bug (clone source)
        original_bug_id = None
        linked_issues = []
        for link in fields.get("issuelinks", []):
            link_type = link.get("type", {}).get("name", "")
            if "inwardIssue" in link:
                linked_key = link["inwardIssue"]["key"]
                linked_summary = link["inwardIssue"]["fields"]["summary"]
                linked_issues.append(
                    {
                        "key": linked_key,
                        "type": link_type,
                        "direction": "inward",
                        "summary": linked_summary,
                    }
                )
                if link_type.lower() == "cloners" and not original_bug_id:
                    original_bug_id = linked_key
            if "outwardIssue" in link:
                linked_key = link["outwardIssue"]["key"]
                linked_summary = link["outwardIssue"]["fields"]["summary"]
                linked_issues.append(
                    {
                        "key": linked_key,
                        "type": link_type,
                        "direction": "outward",
                        "summary": linked_summary,
                    }
                )

        # Extract web links (contains upstream PR URLs)
        web_links = self._get_web_links(bug_id)
        upstream_pr_url = self._find_upstream_pr(web_links)

        # Extract labels
        labels = fields.get("labels", [])

        # Extract description
        description = self._render_description(fields.get("description", ""))

        # Extract QA contact
        qa_contact = None
        qa_field = fields.get("customfield_12319275")
        if qa_field and isinstance(qa_field, dict):
            qa_contact = qa_field.get("displayName")

        # Extract comments
        comments = []
        comment_data = fields.get("comment", {})
        if comment_data:
            for c in comment_data.get("comments", []):
                comments.append(
                    {
                        "author": c.get("author", {}).get("displayName", "Unknown"),
                        "body": self._render_description(c.get("body", "")),
                    }
                )

        return BugInfo(
            bug_id=bug_id,
            summary=summary,
            description=description,
            component=component,
            fix_versions=fix_versions,
            original_bug_id=original_bug_id,
            upstream_pr_url=upstream_pr_url,
            qa_contact=qa_contact,
            labels=labels,
            comments=comments,
            web_links=web_links,
            linked_issues=linked_issues,
        )

    def _get_web_links(self, bug_id: str) -> list:
        """
        Fetch web links (remote links) for a bug.

        Args:
            bug_id: The Jira issue key.

        Returns:
            list[dict]: List of web link dictionaries.

        """
        try:
            remote_links = self.jira.get_issue_remote_links(bug_id)
            links = []
            for link in remote_links:
                obj = link.get("object", {})
                links.append(
                    {
                        "url": obj.get("url", ""),
                        "title": obj.get("title", ""),
                    }
                )
            return links
        except Exception:
            log.warning("Failed to fetch web links for %s", bug_id, exc_info=True)
            return []

    def _find_upstream_pr(self, web_links: list) -> Optional[str]:
        """
        Find the upstream fix PR URL from web links.

        Args:
            web_links: List of web link dictionaries.

        Returns:
            Optional[str]: The GitHub PR URL if found.

        """
        for link in web_links:
            url = link.get("url", "")
            # Strip Jira tracking parameters
            clean_url = url.split("?")[0]
            match = GITHUB_PR_PATTERN.match(clean_url)
            if match:
                return clean_url
        return None

    def _detect_component(self, summary: str) -> Component:
        """
        Detect the ODF component from the bug summary text.

        Args:
            summary: The bug summary string.

        Returns:
            Component: The detected component.

        """
        lower_summary = summary.lower()
        for pattern, component in COMPONENT_PATTERNS:
            if re.search(pattern, lower_summary):
                return component
        return Component.UNKNOWN

    def _render_description(self, description) -> str:
        """
        Convert Jira description (ADF or plain text) to readable text.

        Jira Cloud uses Atlassian Document Format (ADF) which is a JSON structure.
        This method extracts the text content from it.

        Args:
            description: The description field (can be str, dict, or None).

        Returns:
            str: Plain text description.

        """
        if description is None:
            return ""
        if isinstance(description, str):
            return description

        # ADF (Atlassian Document Format) - extract text recursively
        if isinstance(description, dict):
            return self._extract_adf_text(description)

        return str(description)

    def _extract_adf_text(self, node: dict) -> str:
        """
        Recursively extract text from an ADF node.

        Args:
            node: An ADF node dictionary.

        Returns:
            str: Extracted text content.

        """
        if not isinstance(node, dict):
            return str(node) if node else ""

        text_parts = []

        if node.get("type") == "text":
            text_parts.append(node.get("text", ""))

        if node.get("type") == "hardBreak":
            text_parts.append("\n")

        for child in node.get("content", []):
            text_parts.append(self._extract_adf_text(child))

        # Add newlines after block elements
        block_types = {
            "paragraph",
            "heading",
            "bulletList",
            "orderedList",
            "listItem",
            "codeBlock",
            "blockquote",
            "rule",
        }
        if node.get("type") in block_types:
            text_parts.append("\n")

        return "".join(text_parts)
