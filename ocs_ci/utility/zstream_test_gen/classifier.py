"""
Bug classifier for z-stream test generation.

Classifies bugs into categories: automatable, needs-dr-setup, manual-only,
or already-covered based on bug metadata and existing test coverage.
"""

import logging
import re
from pathlib import Path
from typing import Optional

from ocs_ci.utility.zstream_test_gen.models import (
    BugClassification,
    BugInfo,
    Component,
    ConfidenceScore,
    COMPONENT_CONFIG,
)
from ocs_ci.utility.zstream_test_gen.config import GeneratorConfig

log = logging.getLogger(__name__)

# Keywords that indicate a bug needs manual test design.
# These are checked against the bug SUMMARY only (not full description),
# to avoid false positives from passing mentions of UI/performance in comments.
MANUAL_ONLY_SUMMARY_KEYWORDS = {
    "cve": [
        r"\bcve-\d{4}-\d+",
        r"\bcve\b",
    ],
    "ui": [
        r"\bui\s+(test|page|panel|tab|view|element|screen|button|click|display)",
        r"\bconsole\s+(ui|page|panel|tab|view|screen)",
        r"\bdashboard\b",
        r"\bodf\s+console\b",
        r"\bweb\s+console\b",
    ],
    "performance": [
        r"\bperformance\s+(regress|degrad|issue|test|bench)",
        r"\bslow(ness|er)?\s+(io|read|write|response|query)",
        r"\blatency\s+(increas|spike|high|regress)",
    ],
    "timing": [
        r"\brace\s*condition\b",
        r"\bflaky\s+test\b",
    ],
    "lifecycle": [
        r"\bupgrade\s+(fail|break|regress|issue|block)",
        r"\binstallation\s+(fail|break|issue|block)",
        r"\bdeploy(ment)?\s+(fail|break|issue|block)",
    ],
}

# Keywords that indicate DR setup is needed
DR_KEYWORDS = [
    r"\bdr\b",
    r"\bdisaster.recovery\b",
    r"\bfailover\b",
    r"\brelocat",
    r"\bregional.dr\b",
    r"\bmetro.dr\b",
    r"\brdr\b",
    r"\bmdr\b",
    r"\bramen\b",
    r"\bacm\b",
]


class BugClassifier:
    """
    Classifies z-stream bugs for test generation.

    Args:
        cfg: Generator configuration.

    """

    def __init__(self, cfg: GeneratorConfig):
        self.cfg = cfg
        self.ocsci_root = Path(cfg.ocsci_root)

    def classify(self, bug: BugInfo) -> BugInfo:
        """
        Classify a bug and update its classification fields.

        Classification priority:
        1. already-covered -- existing test covers this scenario
        2. manual-only -- UI, performance, vague, upgrade, or deployment bugs
        3. needs-dr-setup -- DR/Ramen bugs that are automatable but need DR infra
        4. automatable -- clear functional bugs with enough context

        Args:
            bug: The bug to classify.

        Returns:
            BugInfo: The same bug with classification and reason updated.

        """
        # Check if already covered by existing tests
        existing_test = self._find_existing_test(bug)
        if existing_test:
            bug.classification = BugClassification.ALREADY_COVERED
            bug.classification_reason = f"Existing test: {existing_test}"
            bug.existing_test_path = existing_test
            log.info("%s classified as already-covered: %s", bug.bug_id, existing_test)
            return bug

        # Check if manual-only
        manual_reason = self._check_manual_only(bug)
        if manual_reason:
            bug.classification = BugClassification.MANUAL_ONLY
            bug.classification_reason = manual_reason
            log.info("%s classified as manual-only: %s", bug.bug_id, manual_reason)
            return bug

        # Check if DR setup is needed
        if self._needs_dr_setup(bug):
            bug.classification = BugClassification.NEEDS_DR_SETUP
            bug.classification_reason = "Requires DR (disaster recovery) infrastructure"
            log.info("%s classified as needs-dr-setup", bug.bug_id)
            return bug

        # Default: automatable
        bug.classification = BugClassification.AUTOMATABLE
        bug.classification_reason = (
            "Functional bug with sufficient context for test generation"
        )
        log.info("%s classified as automatable", bug.bug_id)
        return bug

    def score_confidence(self, bug: BugInfo) -> ConfidenceScore:
        """
        Score how confident we are in the generated test quality.

        Scores based on signal availability:
        - Has upstream PR URL (+2)
        - Has upstream PR diff (+2)
        - Has description with substance (+1)
        - Has reproduction steps in comments (+1)
        - Has Rovo/AI verification steps (+2)
        - Has similar existing tests to learn from (+1)
        - Known component (not UNKNOWN) (+1)

        Args:
            bug: The enriched bug to score.

        Returns:
            ConfidenceScore: The computed confidence score.

        """
        conf = ConfidenceScore(max_score=10)

        if bug.upstream_pr_url:
            conf.score += 2
            conf.signals.append("+ upstream PR linked")
        else:
            conf.signals.append("- no upstream PR")

        if bug.upstream_pr_diff:
            conf.score += 2
            conf.signals.append("+ PR diff available")
        else:
            conf.signals.append("- no PR diff")

        if bug.description and len(bug.description) > 50:
            conf.score += 1
            conf.signals.append("+ has description")
        else:
            conf.signals.append("- no/short description")

        has_repro = False
        if bug.comments:
            for comment in bug.comments:
                body = comment.get("body", "").lower()
                if any(
                    kw in body
                    for kw in (
                        "steps to reproduce",
                        "repro",
                        "how to reproduce",
                        "reproduce:",
                        "reproduction",
                    )
                ):
                    has_repro = True
                    break
        if has_repro:
            conf.score += 1
            conf.signals.append("+ reproduction steps in comments")
        else:
            conf.signals.append("- no reproduction steps")

        if bug.rovo_spec and bug.rovo_spec.verification_steps:
            conf.score += 2
            conf.signals.append("+ AI verification steps")
        else:
            conf.signals.append("- no verification steps")

        similar = self.find_similar_tests(bug, max_results=1)
        if similar:
            conf.score += 1
            conf.signals.append("+ similar tests found")
        else:
            conf.signals.append("- no similar tests")

        if bug.component != Component.UNKNOWN:
            conf.score += 1
            conf.signals.append("+ known component")
        else:
            conf.signals.append("- unknown component")

        bug.confidence = conf
        log.info(
            "Confidence for %s: %s (%s)",
            bug.bug_id,
            conf.to_text(),
            ", ".join(conf.signals),
        )
        return conf

    def _find_existing_test(self, bug: BugInfo) -> Optional[str]:
        """
        Search for existing tests that already cover this bug.

        Checks for:
        1. Tests referencing the same bug ID
        2. Tests referencing the original bug ID (if this is a backport)

        Args:
            bug: The bug to search for.

        Returns:
            Optional[str]: Relative path to the existing test, or None.

        """
        tests_dir = self.ocsci_root / "tests"
        if not tests_dir.exists():
            return None

        # Search for bug ID references in test files
        bug_ids_to_search = [bug.bug_id]
        if bug.original_bug_id:
            bug_ids_to_search.append(bug.original_bug_id)

        for bug_id in bug_ids_to_search:
            # Extract the numeric part for flexible matching
            numeric_id = bug_id.split("-")[-1] if "-" in bug_id else bug_id

            for test_file in tests_dir.rglob("test_*.py"):
                try:
                    content = test_file.read_text(errors="ignore")
                    # Check for bug ID in various formats
                    if (
                        bug_id in content
                        or f'"{numeric_id}"' in content
                        or f"'{numeric_id}'" in content
                    ):
                        rel_path = str(test_file.relative_to(self.ocsci_root))
                        log.debug("Found existing test for %s: %s", bug_id, rel_path)
                        return rel_path
                except OSError:
                    continue

        return None

    def _check_manual_only(self, bug: BugInfo) -> Optional[str]:
        """
        Check if a bug should be classified as manual-only.

        Uses the bug summary (not full description) for keyword matching
        to avoid false positives from passing mentions in comments.

        Args:
            bug: The bug to check.

        Returns:
            Optional[str]: Reason for manual-only classification, or None.

        """
        # Use summary only for keyword matching to avoid false positives
        summary_lower = bug.summary.lower()

        reasons = {
            "cve": "CVE/security fix — verified by package version, not functional test",
            "ui": "UI-related bug requires manual test design",
            "performance": "Performance-related bug requires specialized testing",
            "timing": "Intermittent/timing issue requires careful manual test design",
            "lifecycle": "Upgrade/deployment bug requires specialized test infrastructure",
        }

        for category, patterns in MANUAL_ONLY_SUMMARY_KEYWORDS.items():
            for pattern in patterns:
                match = re.search(pattern, summary_lower)
                if match:
                    log.debug(
                        "%s matched manual-only pattern '%s' in summary: ...%s...",
                        bug.bug_id,
                        pattern,
                        match.group(0),
                    )
                    return reasons[category]

        # Check if there's enough context to generate a test
        if not bug.upstream_pr_url and not bug.description:
            return "Insufficient context: no upstream PR link and no description"

        # If Rovo spec exists but has no verification steps
        if bug.rovo_spec and not bug.rovo_spec.verification_steps:
            if not bug.upstream_pr_url:
                return "Rovo could not determine verification steps and no upstream PR available"

        return None

    def _needs_dr_setup(self, bug: BugInfo) -> bool:
        """
        Check if a bug requires DR infrastructure.

        Only checks the summary to avoid false positives from passing
        mentions of DR terms in the description or comments.

        Args:
            bug: The bug to check.

        Returns:
            bool: True if DR setup is needed.

        """
        if bug.component == Component.RAMEN:
            return True

        summary_lower = bug.summary.lower()
        for pattern in DR_KEYWORDS:
            match = re.search(pattern, summary_lower)
            if match:
                log.debug(
                    "%s matched DR pattern '%s' in summary: ...%s...",
                    bug.bug_id,
                    pattern,
                    match.group(0),
                )
                return True

        return False

    def find_similar_tests(self, bug: BugInfo, max_results: int = 5) -> list:
        """
        Find existing tests similar to what we need to generate.

        Used as few-shot examples for the test generator.

        Args:
            bug: The bug to find similar tests for.
            max_results: Maximum number of similar tests to return.

        Returns:
            list[dict]: List of dicts with 'path' and 'content' keys.

        """
        component_config = COMPONENT_CONFIG.get(
            bug.component, COMPONENT_CONFIG[Component.UNKNOWN]
        )
        test_dirs = component_config["test_dirs"]

        # Collect candidate test files from the component's directories
        candidates = []
        for test_dir in test_dirs:
            full_dir = self.ocsci_root / test_dir
            if not full_dir.exists():
                continue
            for test_file in full_dir.rglob("test_*.py"):
                try:
                    content = test_file.read_text(errors="ignore")
                    rel_path = str(test_file.relative_to(self.ocsci_root))
                    candidates.append(
                        {
                            "path": rel_path,
                            "content": content,
                            "score": self._similarity_score(bug, content),
                        }
                    )
                except OSError:
                    continue

        # Sort by similarity score and return top results
        candidates.sort(key=lambda x: x["score"], reverse=True)

        results = []
        for candidate in candidates[:max_results]:
            # Truncate very long test files
            content = candidate["content"]
            if len(content) > 5000:
                content = content[:5000] + "\n# ... [truncated] ..."

            results.append(
                {
                    "path": candidate["path"],
                    "content": content,
                }
            )

        log.info(
            "Found %d similar tests for %s (component: %s)",
            len(results),
            bug.bug_id,
            bug.component.value,
        )
        return results

    def _similarity_score(self, bug: BugInfo, test_content: str) -> float:
        """
        Score how similar a test file is to the bug's domain.

        Simple keyword-overlap scoring. Not ML-based -- uses shared
        technical terms between the bug summary/description and the test.

        Args:
            bug: The bug to compare against.
            test_content: Content of the test file.

        Returns:
            float: Similarity score (higher is more similar).

        """
        bug_text = f"{bug.summary} {bug.description}".lower()
        test_lower = test_content.lower()

        # Extract meaningful keywords from bug text
        keywords = set(re.findall(r"[a-z_]{4,}", bug_text))
        # Remove very common words
        stopwords = {
            "this",
            "that",
            "with",
            "from",
            "when",
            "have",
            "will",
            "they",
            "been",
            "does",
            "should",
            "would",
            "could",
            "into",
            "also",
            "which",
            "other",
            "each",
            "some",
            "test",
            "tests",
        }
        keywords -= stopwords

        score = 0.0
        for keyword in keywords:
            if keyword in test_lower:
                score += 1.0

        # Bonus for fixture overlap
        fixture_patterns = [
            "pvc_factory",
            "pod_factory",
            "storageclass_factory",
            "snapshot_factory",
            "multi_pvc_factory",
            "bucket_factory",
        ]
        for fixture in fixture_patterns:
            if fixture in test_lower and any(
                term in bug_text for term in fixture.replace("_", " ").split()
            ):
                score += 2.0

        # Bonus for tests that reference Jira bugs (they verify fixes)
        if "jira" in test_lower or "dfbugs" in test_lower:
            score += 1.0

        return score
