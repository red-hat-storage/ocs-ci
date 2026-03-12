"""
Failure classifier orchestrator.

Coordinates the full analysis pipeline for each test failure:
1. Known issue regex matching (instant, no cost)
2. Failure signature computation and cache lookup
3. Log preprocessing (extract relevant context)
4. AI classification (via pluggable backend)
5. Cache storage of results
"""

import logging
from typing import Optional

from ocs_ci.utility.log_analysis.ai.base import AIBackend
from ocs_ci.utility.log_analysis.analysis.known_issues import KnownIssuesMatcher
from ocs_ci.utility.log_analysis.cache import AnalysisCache
from ocs_ci.utility.log_analysis.exceptions import AIBackendError
from ocs_ci.utility.log_analysis.models import (
    FailureAnalysis,
    FailureCategory,
    FailureSignature,
    TestResult,
)
from ocs_ci.utility.log_analysis.parsers.test_log_parser import TestLogParser
from ocs_ci.utility.log_analysis.parsers.must_gather_parser import MustGatherParser

logger = logging.getLogger(__name__)


class FailureClassifier:
    """
    Orchestrates the full failure analysis pipeline.

    Combines regex matching, caching, log parsing, and AI classification
    to produce a FailureAnalysis for each failed test.
    """

    def __init__(
        self,
        ai_backend: AIBackend,
        known_issues_matcher: Optional[KnownIssuesMatcher] = None,
        cache: Optional[AnalysisCache] = None,
        skip_ai_for_known: bool = True,
        max_failures: int = 30,
        failed_logs_dir: Optional[str] = None,
    ):
        """
        Args:
            ai_backend: AI backend for classification
            known_issues_matcher: Regex matcher (uses default if None)
            cache: Analysis cache (disabled if None)
            skip_ai_for_known: Skip AI for regex-matched known issues
            max_failures: Max unique failures to analyze with AI
            failed_logs_dir: URL to failed_testcase_ocs_logs dir for must-gather access
        """
        self.ai_backend = ai_backend
        self.known_issues = known_issues_matcher or KnownIssuesMatcher()
        self.cache = cache
        self.skip_ai_for_known = skip_ai_for_known
        self.max_failures = max_failures
        self.failed_logs_dir = failed_logs_dir
        self.log_parser = TestLogParser()
        self.mg_parser = MustGatherParser()

    def classify_failures(
        self,
        failures: list,
        fetcher=None,
    ) -> list:
        """
        Classify a list of test failures.

        Args:
            failures: List of TestResult objects (status=FAILED or ERROR)
            fetcher: ArtifactFetcher for downloading logs (optional)

        Returns:
            List of FailureAnalysis objects
        """
        results = []
        ai_call_count = 0
        cache_hit_count = 0
        known_issue_count = 0

        # Group failures by signature to avoid duplicate AI calls
        signature_groups = {}
        for failure in failures:
            sig = FailureSignature.from_test_result(failure)
            if sig.cache_key not in signature_groups:
                signature_groups[sig.cache_key] = {
                    "signature": sig,
                    "failures": [],
                }
            signature_groups[sig.cache_key]["failures"].append(failure)

        logger.info(
            f"Classifying {len(failures)} failures "
            f"({len(signature_groups)} unique signatures)"
        )

        for cache_key, group in signature_groups.items():
            sig = group["signature"]
            group_failures = group["failures"]
            representative = group_failures[0]

            # Step 1: Known issue matching
            known_matches = self.known_issues.match_test_result(representative)
            if known_matches and self.skip_ai_for_known:
                analysis_dict = {
                    "category": "known_issue",
                    "confidence": 1.0,
                    "root_cause_summary": (
                        f"Matched known issue(s): "
                        f"{', '.join(m['issue'] for m in known_matches)}"
                    ),
                    "evidence": [
                        f"Pattern match: {m.get('description', m['pattern'])}"
                        for m in known_matches
                    ],
                    "matched_known_issues": [m["issue"] for m in known_matches],
                    "recommended_action": "See linked Jira issue(s)",
                }
                known_issue_count += len(group_failures)
                for f in group_failures:
                    results.append(self._build_analysis(f, analysis_dict))
                continue

            # Step 2: Cache lookup
            if self.cache:
                cached = self.cache.get(sig)
                if cached:
                    cache_hit_count += len(group_failures)
                    for f in group_failures:
                        results.append(self._build_analysis(f, cached))
                    continue

            # Step 3: AI classification (respecting budget)
            if (
                self.ai_backend.requires_budget_limit
                and ai_call_count >= self.max_failures
            ):
                logger.warning(
                    f"AI call limit ({self.max_failures}) reached. "
                    f"Remaining failures will be unclassified."
                )
                for f in group_failures:
                    results.append(self._build_unclassified(f))
                continue

            # Fetch and parse logs if available
            log_excerpt = ""
            infra_context = ""

            if fetcher and representative.log_path:
                log_excerpt = self._fetch_and_parse_log(
                    fetcher, representative.log_path
                )

            # Build must-gather URL for this test if available
            must_gather_url = self._build_must_gather_url(representative.name)

            # Step 4: Call AI backend
            try:
                analysis_dict = self.ai_backend.classify_failure(
                    test_name=representative.name,
                    test_class=representative.classname,
                    duration=representative.duration,
                    squad=representative.squad or "Unknown",
                    traceback=representative.traceback or "",
                    log_excerpt=log_excerpt,
                    infra_context=infra_context,
                    must_gather_url=must_gather_url,
                )
                ai_call_count += 1

                # Merge known issue matches if any (partial matches)
                if known_matches:
                    analysis_dict.setdefault("matched_known_issues", [])
                    analysis_dict["matched_known_issues"].extend(
                        m["issue"] for m in known_matches
                    )

                # Cache the result
                if self.cache:
                    self.cache.put(sig, analysis_dict)

            except Exception as e:
                ai_call_count += 1  # Count failed calls toward the limit
                logger.warning(
                    f"AI classification failed for {representative.name}: {e}"
                )
                analysis_dict = {
                    "category": "unknown",
                    "confidence": 0.0,
                    "root_cause_summary": self._extract_error_summary(representative),
                    "evidence": [],
                    "recommended_action": f"AI classification failed: {e}",
                }

            for f in group_failures:
                results.append(self._build_analysis(f, analysis_dict))

        logger.info(
            f"Classification complete: {ai_call_count} AI calls, "
            f"{cache_hit_count} cache hits, {known_issue_count} known issues"
        )

        return results

    def _fetch_and_parse_log(self, fetcher, log_path: str) -> str:
        """Fetch and parse a per-test log file."""
        try:
            log_content = fetcher.fetch_text(log_path)
            parsed = self.log_parser.parse(log_content)
            return self.log_parser.build_excerpt(parsed)
        except Exception as e:
            logger.debug(f"Could not fetch/parse log at {log_path}: {e}")
            return ""

    def _build_must_gather_url(self, test_name: str) -> str:
        """Build the must-gather base URL for a specific test.

        The must-gather directory structure is:
            {failed_logs_dir}/{test_name}_ocs_logs/{cluster_id}/ocs_must_gather/
                                                               /ocp_must_gather/

        We return the {test_name}_ocs_logs/ URL and let the agentic
        Claude navigate into cluster_id/ocs_must_gather/ etc.
        """
        if not self.failed_logs_dir:
            return ""

        # URL-encode brackets from parameterized test names
        # e.g., test_raw_block_pv[Retain] -> test_raw_block_pv%5bRetain%5d
        safe_name = test_name.replace("[", "%5b").replace("]", "%5d")
        base = self.failed_logs_dir.rstrip("/")
        url = f"{base}/{safe_name}_ocs_logs"
        logger.debug(f"Must-gather URL for {test_name}: {url}")
        return url

    @staticmethod
    def _build_analysis(
        test_result: TestResult, analysis_dict: dict
    ) -> FailureAnalysis:
        """Build a FailureAnalysis from a test result and analysis dict."""
        return FailureAnalysis(
            test_result=test_result,
            category=FailureCategory(analysis_dict.get("category", "unknown")),
            confidence=float(analysis_dict.get("confidence", 0.0)),
            root_cause_summary=analysis_dict.get("root_cause_summary", ""),
            evidence=analysis_dict.get("evidence", []),
            matched_known_issues=analysis_dict.get("matched_known_issues", []),
            suggested_jira_issues=analysis_dict.get("suggested_jira_issues", []),
            recommended_action=analysis_dict.get("recommended_action", ""),
        )

    @staticmethod
    def _build_unclassified(test_result: TestResult) -> FailureAnalysis:
        """Build an unclassified FailureAnalysis."""
        summary = ""
        if test_result.traceback:
            lines = test_result.traceback.strip().splitlines()
            if lines:
                summary = lines[-1].strip()[:200]

        return FailureAnalysis(
            test_result=test_result,
            category=FailureCategory.UNKNOWN,
            confidence=0.0,
            root_cause_summary=summary,
            recommended_action="AI call limit reached. Re-run with higher --max-failures.",
        )

    @staticmethod
    def _extract_error_summary(test_result: TestResult) -> str:
        """Extract a one-line error summary from a traceback."""
        if not test_result.traceback:
            return "No traceback available"
        lines = test_result.traceback.strip().splitlines()
        if lines:
            return lines[-1].strip()[:200]
        return "No traceback available"
