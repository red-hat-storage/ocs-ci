"""
Jira integration for log analysis.

Searches Jira for existing bugs matching failure signatures,
enriches failure analyses with linked issues, and generates
bug creation suggestions for unmatched failures.
"""

import logging
import re
from typing import Optional

from ocs_ci.utility.log_analysis.models import FailureAnalysis, FailureCategory

logger = logging.getLogger(__name__)

# Jira projects to search in, ordered by relevance
DEFAULT_PROJECTS = ["DFBUGS"]

# Max JQL results per query
DEFAULT_MAX_RESULTS = 5

# Categories that warrant Jira search
SEARCHABLE_CATEGORIES = {
    FailureCategory.PRODUCT_BUG,
    FailureCategory.UNKNOWN,
    FailureCategory.INFRA_ISSUE,
    FailureCategory.FLAKY_TEST,
}


class JiraSearchIntegration:
    """
    Search Jira for bugs matching failure analyses and enrich results.

    Uses the existing JiraHelper for API access. Gracefully degrades
    if Jira credentials are unavailable.
    """

    def __init__(
        self,
        projects: Optional[list] = None,
        max_results: int = DEFAULT_MAX_RESULTS,
    ):
        """
        Args:
            projects: Jira project keys to search (default: DFBUGS, RHSTOR, OCSQE)
            max_results: Max results per JQL query
        """
        self.projects = projects or list(DEFAULT_PROJECTS)
        self.max_results = max_results
        self._jira = None
        self._available = None

    @property
    def jira(self):
        """Lazy-initialize JiraHelper to avoid credential errors at import time."""
        if self._jira is None:
            try:
                from ocs_ci.utility.jira import JiraHelper

                self._jira = JiraHelper()
                self._available = True
            except Exception as e:
                logger.warning(f"Jira integration unavailable: {e}")
                self._available = False
        return self._jira

    def is_available(self) -> bool:
        """Check if Jira credentials are configured."""
        if self._available is None:
            # Trigger lazy init
            _ = self.jira
        return self._available

    def enrich_analyses(self, failure_analyses: list) -> list:
        """
        Enrich failure analyses with Jira issue links.

        For each failure that matches searchable categories, search Jira
        for related bugs and add them to suggested_jira_issues.

        Args:
            failure_analyses: List of FailureAnalysis objects

        Returns:
            The same list, with suggested_jira_issues populated
        """
        if not self.is_available():
            logger.debug("Jira integration not available, skipping enrichment")
            return failure_analyses

        # Group by unique search terms to avoid duplicate queries
        search_cache = {}
        enriched_count = 0
        search_count = 0

        for fa in failure_analyses:
            if fa.category not in SEARCHABLE_CATEGORIES:
                continue

            # Skip if already has linked Jira issues from known-issue matching
            if fa.matched_known_issues:
                self._enrich_known_issues(fa)
                continue

            search_key = self._build_search_key(fa)
            if not search_key:
                continue

            if search_key in search_cache:
                fa.suggested_jira_issues = list(search_cache[search_key])
                if fa.suggested_jira_issues:
                    enriched_count += 1
                continue

            try:
                jql = self._build_jql(fa)
                if not jql:
                    continue

                search_count += 1
                results = self.jira.search_issues(jql, max_results=self.max_results)
                search_cache[search_key] = results
                fa.suggested_jira_issues = list(results)
                if results:
                    enriched_count += 1
            except Exception as e:
                logger.debug(f"Jira search failed for {fa.test_result.name}: {e}")

        if search_count > 0:
            logger.info(
                f"Jira: {search_count} searches, "
                f"{enriched_count} failures linked to existing bugs"
            )
        return failure_analyses

    def _enrich_known_issues(self, fa: FailureAnalysis):
        """Fetch details for known issue references (e.g., DFBUGS-2781)."""
        enriched = []
        for issue_key in fa.matched_known_issues:
            if not re.match(r"^[A-Z]+-\d+$", issue_key):
                # Not a real Jira key (e.g., "INFRA")
                continue
            try:
                summary = self.jira.get_issue_summary(issue_key)
                enriched.append(summary)
            except Exception as e:
                logger.debug(f"Could not fetch details for {issue_key}: {e}")
                enriched.append({"key": issue_key, "url": ""})

        if enriched:
            fa.suggested_jira_issues = enriched

    def _build_search_key(self, fa: FailureAnalysis) -> str:
        """Build a cache key for deduplicating Jira searches."""
        rc_keywords = (
            self._extract_root_cause_keywords(fa.root_cause_summary)
            if fa.root_cause_summary
            else []
        )
        evidence_error = self._extract_primary_evidence_error(fa.evidence)
        if rc_keywords and evidence_error:
            return f"{':'.join(rc_keywords[:3])}:{evidence_error[:80]}"
        if evidence_error:
            return evidence_error[:80]
        if rc_keywords:
            return ":".join(rc_keywords[:3])
        return ""

    def _build_jql(self, fa: FailureAnalysis) -> str:
        """
        Build a JQL query from failure analysis.

        Strategy: Search for the primary error message found in must-gather
        evidence (exact phrase) combined with product/component keywords
        from the root cause summary. This matches actual ODF bugs much
        better than test framework exception types or test names.
        """
        project_clause = self._project_clause()

        # Extract the primary error from must-gather evidence
        evidence_error = self._extract_primary_evidence_error(fa.evidence)

        # Root cause keywords for broad context
        rc_keywords = []
        if fa.root_cause_summary:
            rc_keywords = self._extract_root_cause_keywords(fa.root_cause_summary)

        if not evidence_error and not rc_keywords:
            return ""

        # Only search open or recently resolved issues
        status_clause = (
            'status in ("Open", "In Progress", "To Do", "New", "Closed", "Done")'
        )

        # Build text search — prefer evidence error as exact phrase
        if evidence_error:
            # Escape quotes in the error message
            safe_error = evidence_error.replace('"', '\\"')
            # Truncate to avoid overly specific queries
            if len(safe_error) > 120:
                safe_error = safe_error[:120]
            text_clause = f'text ~ "{safe_error}"'
        else:
            # Fall back to root cause keywords only
            text_query = " ".join(rc_keywords[:4])
            text_query = text_query.replace('"', '\\"')
            text_clause = f'text ~ "{text_query}"'

        jql = (
            f"{project_clause} AND {status_clause} "
            f"AND {text_clause} "
            f"ORDER BY updated DESC"
        )

        return jql

    def _project_clause(self) -> str:
        """Build JQL project clause."""
        if len(self.projects) == 1:
            return f'project = "{self.projects[0]}"'
        projects_str = ", ".join(f'"{p}"' for p in self.projects)
        return f"project in ({projects_str})"

    @staticmethod
    def _extract_primary_evidence_error(evidence: list) -> str:
        """
        Extract the primary error message from must-gather evidence entries.

        Evidence entries follow the format:
          '<explanation> — <source_file>: '<quoted log line>''

        We extract the quoted log portions and pick the most specific
        error message for Jira search.
        """
        if not evidence:
            return ""

        candidates = []
        for entry in evidence:
            if not isinstance(entry, str):
                continue
            # Extract text within single quotes (the actual log messages)
            quoted = re.findall(r"'([^']{15,})'", entry)
            for q in quoted:
                # Skip generic/noisy strings
                if any(
                    skip in q.lower()
                    for skip in [
                        "health_ok",
                        "running normally",
                        "successfully",
                        "search by name",
                    ]
                ):
                    continue
                candidates.append(q)

        if not candidates:
            return ""

        # Score candidates — prefer longer messages with error indicators
        error_indicators = [
            "error",
            "fail",
            "crash",
            "warn",
            "down",
            "degraded",
            "timeout",
            "refused",
            "denied",
            "not found",
            "invalid",
            "EINVAL",
            "exit status",
            "not exist",
            "exception",
        ]

        def score(text):
            lower = text.lower()
            s = 0
            for ind in error_indicators:
                if ind in lower:
                    s += 10
            # Bonus for longer, more specific messages
            s += min(len(text), 100) // 10
            return s

        candidates.sort(key=score, reverse=True)
        return candidates[0]

    @staticmethod
    def _extract_root_cause_keywords(summary: str) -> list:
        """Extract meaningful keywords from root cause summary."""
        # Look for specific technical terms
        technical_terms = re.findall(
            r"\b(?:ceph|osd|mon|mds|rgw|rook|noobaa|mcg|s3|pvc|pv|"
            r"storagecluster|storageclass|operator|pod|node|timeout|"
            r"crash|oom|permission|denied|quota|capacity)\b",
            summary,
            re.IGNORECASE,
        )
        # Deduplicate while preserving order
        seen = set()
        result = []
        for term in technical_terms:
            lower = term.lower()
            if lower not in seen:
                seen.add(lower)
                result.append(lower)
        return result

    def generate_bug_suggestion(self, fa: FailureAnalysis, run_url: str = "") -> dict:
        """
        Generate a suggested bug report for a failure without matching Jira issues.

        Args:
            fa: FailureAnalysis to generate bug for
            run_url: URL of the test run for reference

        Returns:
            dict with keys: project, summary, description, labels, priority
        """
        # Extract exception type from traceback for bug summary
        clean_exception = "Failure"
        if fa.test_result.traceback:
            tb_lines = fa.test_result.traceback.strip().splitlines()
            if tb_lines:
                match = re.match(
                    r"^([\w.]+(?:Error|Exception|Failure))", tb_lines[-1].strip()
                )
                if match:
                    clean_exception = match.group(1).split(".")[-1]

        # Build summary
        test_short = fa.test_result.name
        if len(test_short) > 60:
            test_short = test_short[:57] + "..."
        summary = f"{test_short}: {clean_exception}"
        if len(summary) > 120:
            summary = summary[:117] + "..."

        # Build description
        description_parts = [
            "h2. Failure Details",
            f"*Test*: {fa.test_result.full_name}",
            f"*Squad*: {fa.test_result.squad or 'Unknown'}",
            f"*Category*: {fa.category.value}",
            f"*Confidence*: {fa.confidence:.0%}",
        ]

        if fa.test_result.polarion_id:
            description_parts.append(f"*Polarion ID*: {fa.test_result.polarion_id}")

        if run_url:
            description_parts.append(f"*Run URL*: {run_url}")

        description_parts.append("")

        if fa.root_cause_summary:
            description_parts.append("h2. Root Cause Analysis")
            description_parts.append(fa.root_cause_summary)
            description_parts.append("")

        if fa.evidence:
            description_parts.append("h2. Evidence")
            for e in fa.evidence:
                description_parts.append(f"* {e}")
            description_parts.append("")

        if fa.test_result.traceback:
            description_parts.append("h2. Traceback")
            tb = fa.test_result.traceback[:3000]
            description_parts.append(f"{{noformat}}\n{tb}\n{{noformat}}")

        description_parts.append("")
        description_parts.append("_Generated by OCS-CI Log Analysis Tool_")

        # Determine project based on category
        project = "DFBUGS"
        if fa.category == FailureCategory.TEST_BUG:
            project = "OCSQE"

        # Labels
        labels = ["auto-detected"]
        if fa.test_result.squad:
            labels.append(f"squad-{fa.test_result.squad.lower()}")

        # Priority mapping
        priority_map = {
            FailureCategory.PRODUCT_BUG: "Major",
            FailureCategory.INFRA_ISSUE: "Normal",
            FailureCategory.FLAKY_TEST: "Normal",
            FailureCategory.TEST_BUG: "Normal",
            FailureCategory.UNKNOWN: "Minor",
        }

        return {
            "project": project,
            "summary": summary,
            "description": "\n".join(description_parts),
            "labels": labels,
            "priority": priority_map.get(fa.category, "Normal"),
        }
