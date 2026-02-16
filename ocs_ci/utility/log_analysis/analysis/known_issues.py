"""
Regex-based known issue matching.

Extends the pattern from ocs_ci/utility/utils.py (ceph_health_fixes)
into a general-purpose failure matcher. Checks tracebacks and log text
against known patterns and maps them to Jira issue keys.
"""

import logging
import re
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


# Known issue patterns for test failures.
# Each entry has:
#   - issue: Jira issue key
#   - pattern: regex pattern to match against traceback/log text
#   - description: (optional) human-readable description
DEFAULT_KNOWN_ISSUES = [
    # Ceph health issues (from ocs_ci/utility/utils.py:2982)
    {
        "issue": "DFBUGS-2781",
        "pattern": r"mgr module prometheus crashed in",
        "description": "Prometheus mgr module crash",
    },
    {
        "issue": "DFBUGS-4521",
        "pattern": r"Netsplit detected between mon",
        "description": "Mon netsplit detection",
    },
    # Common test infrastructure issues
    {
        "issue": "INFRA",
        "pattern": r"TimeoutExpiredError.*wait_for_resource",
        "description": "Resource wait timeout - possible infra slowness",
    },
    {
        "issue": "INFRA",
        "pattern": r"ConnectionRefusedError|ConnectionResetError|ConnectionAbortedError",
        "description": "Network connection error - possible infra issue",
    },
    {
        "issue": "INFRA",
        "pattern": r"Unable to connect to the server.*connection refused",
        "description": "Kubernetes API server unreachable",
    },
    {
        "issue": "INFRA",
        "pattern": r"error: the server doesn't have a resource type",
        "description": "Missing CRD - possible deployment issue",
    },
]


class KnownIssuesMatcher:
    """Match failure text against known issue patterns."""

    def __init__(self, extra_patterns_file: Optional[str] = None):
        """
        Args:
            extra_patterns_file: Optional path to YAML file with additional patterns.
                Expected format:
                    known_issues:
                      - issue: "DFBUGS-1234"
                        pattern: "some regex"
                        description: "optional description"
        """
        self.patterns = list(DEFAULT_KNOWN_ISSUES)
        if extra_patterns_file:
            self._load_extra_patterns(extra_patterns_file)

        # Pre-compile regex patterns
        self._compiled = []
        for p in self.patterns:
            try:
                compiled = re.compile(p["pattern"], re.IGNORECASE | re.DOTALL)
                self._compiled.append((compiled, p))
            except re.error as e:
                logger.warning(f"Invalid regex pattern '{p['pattern']}': {e}")

        logger.debug(f"Loaded {len(self._compiled)} known issue patterns")

    def match(self, text: str) -> list:
        """
        Find all known issues matching the given text.

        Args:
            text: Traceback, log excerpt, or error message to match against

        Returns:
            List of matching pattern dicts (issue, pattern, description)
        """
        matches = []
        for compiled_re, pattern_dict in self._compiled:
            if compiled_re.search(text):
                matches.append(pattern_dict)
                logger.debug(
                    f"Known issue match: {pattern_dict['issue']} "
                    f"({pattern_dict.get('description', 'no description')})"
                )
        return matches

    def match_test_result(self, test_result) -> list:
        """
        Match a TestResult against known issues.

        Checks both traceback and test name.

        Args:
            test_result: TestResult object

        Returns:
            List of matching pattern dicts
        """
        text_parts = []
        if test_result.traceback:
            text_parts.append(test_result.traceback)
        if test_result.name:
            text_parts.append(test_result.name)
        if test_result.classname:
            text_parts.append(test_result.classname)

        combined_text = "\n".join(text_parts)
        return self.match(combined_text)

    def _load_extra_patterns(self, path: str):
        """Load additional patterns from a YAML file."""
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
            extra = data.get("known_issues", [])
            if not isinstance(extra, list):
                logger.warning(f"Expected 'known_issues' list in {path}")
                return
            for entry in extra:
                if "issue" in entry and "pattern" in entry:
                    self.patterns.append(entry)
                else:
                    logger.warning(f"Skipping invalid pattern entry: {entry}")
            logger.info(f"Loaded {len(extra)} extra patterns from {path}")
        except (IOError, yaml.YAMLError) as e:
            logger.warning(f"Failed to load extra patterns from {path}: {e}")
