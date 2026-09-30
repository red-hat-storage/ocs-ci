"""
Main pipeline orchestrator for z-stream test generation.

Ties together all stages: collect, classify, generate, validate, publish.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from ocs_ci.utility.zstream_test_gen.models import (
    BugClassification,
    BugInfo,
    ExecutionSpec,
    GeneratedTest,
    ZStreamReport,
)
from ocs_ci.utility.zstream_test_gen.config import GeneratorConfig, load_config
from ocs_ci.utility.zstream_test_gen.jira_client import ZStreamJiraClient
from ocs_ci.utility.zstream_test_gen.github_client import GitHubClient
from ocs_ci.utility.zstream_test_gen.classifier import BugClassifier
from ocs_ci.utility.zstream_test_gen.generator import TestGenerator
from ocs_ci.utility.zstream_test_gen.validator import TestValidator
from ocs_ci.utility.zstream_test_gen.publisher import TestPublisher

log = logging.getLogger(__name__)


class ZStreamTestPipeline:
    """
    End-to-end pipeline for generating z-stream verification tests.

    Args:
        cfg: Generator configuration. If None, loads from defaults.

    """

    def __init__(self, cfg: Optional[GeneratorConfig] = None):
        self.cfg = cfg or load_config()
        self._jira = None
        self._github = None
        self._classifier = None
        self._generator = None
        self._validator = None
        self._publisher = None

    @property
    def jira(self):
        if self._jira is None:
            self._jira = ZStreamJiraClient(self.cfg)
        return self._jira

    @property
    def github(self):
        if self._github is None:
            self._github = GitHubClient(self.cfg)
        return self._github

    @property
    def classifier(self):
        if self._classifier is None:
            self._classifier = BugClassifier(self.cfg)
        return self._classifier

    @property
    def generator(self):
        if self._generator is None:
            self._generator = TestGenerator(self.cfg)
        return self._generator

    @property
    def validator(self):
        if self._validator is None:
            self._validator = TestValidator(self.cfg)
        return self._validator

    @property
    def publisher(self):
        if self._publisher is None:
            self._publisher = TestPublisher(
                self.cfg,
                self.github,
                self.jira if self.cfg.jira.api_token else None,
            )
        return self._publisher

    def run(self, fix_version: str) -> ZStreamReport:
        """
        Run the full pipeline for a z-stream fix version.

        Stages:
        1. Collect bugs from Jira
        2. Enrich with upstream PR diffs
        3. Classify each bug
        4. Generate tests for automatable bugs
        5. Validate generated tests
        6. Publish as draft PRs

        Args:
            fix_version: The fix version string, e.g. "odf-4.22.1".

        Returns:
            ZStreamReport: Summary report of the pipeline run.

        """
        log.info("=" * 60)
        log.info("Starting z-stream test generation for %s", fix_version)
        log.info("=" * 60)

        report = ZStreamReport(fix_version=fix_version)

        # Stage 1: Collect bugs from Jira
        log.info("Stage 1: Collecting bugs from Jira")
        bugs = self.jira.query_zstream_bugs(fix_version)
        report.total_bugs = len(bugs)
        log.info("Collected %d bugs", len(bugs))

        # Stage 2: Enrich with upstream PR diffs
        log.info("Stage 2: Enriching bugs with upstream PR context")
        for bug in bugs:
            self._enrich_bug(bug)

        # Stage 3: Classify
        log.info("Stage 3: Classifying bugs")
        for bug in bugs:
            self.classifier.classify(bug)

        # Stage 3.5: Deduplicate — group bugs by unique fix
        automatable = [
            b
            for b in bugs
            if b.classification
            in (
                BugClassification.AUTOMATABLE,
                BugClassification.NEEDS_DR_SETUP,
            )
        ]
        unique_bugs, skipped = self._deduplicate_bugs(automatable)
        for bug in skipped:
            bug.classification = BugClassification.ALREADY_COVERED
            report.already_covered.append(bug)

        # Stage 3.6: Score confidence
        log.info("Stage 3.6: Scoring confidence for %d unique bugs", len(unique_bugs))
        for bug in unique_bugs:
            self.classifier.score_confidence(bug)

        # Stage 4 + 5 + 6: Generate, validate, publish
        log.info(
            "Stage 4-6: Generating tests for %d unique bugs (%d duplicates skipped)",
            len(unique_bugs),
            len(skipped),
        )

        for bug in unique_bugs:
            result, prs = self._process_bug(bug, fix_version)
            if result:
                # Flag low-confidence tests for human review
                if bug.confidence and bug.confidence.level == "low":
                    result.needs_review = True
                if bug.classification == BugClassification.NEEDS_DR_SETUP:
                    report.needs_dr_setup.append(result)
                else:
                    report.generated.append(result)
                report.published_prs.extend(prs)

        # Collect non-automatable bugs into report
        for bug in bugs:
            if bug.classification == BugClassification.ALREADY_COVERED:
                report.already_covered.append(bug)
            elif bug.classification == BugClassification.MANUAL_ONLY:
                report.manual_only.append(bug)
            elif bug.classification == BugClassification.GENERATION_FAILED:
                report.generation_failed.append(bug)

        # Save report
        self._save_report(report)

        log.info("=" * 60)
        log.info("Pipeline complete for %s", fix_version)
        log.info(report.to_text())

        return report

    def process_single_bug(self, bug_id: str) -> Optional[GeneratedTest]:
        """
        Process a single bug by ID.

        Useful for testing the pipeline on individual bugs.

        Args:
            bug_id: The Jira issue key, e.g. "DFBUGS-10065".

        Returns:
            Optional[GeneratedTest]: The generated test, or None if not automatable.

        """
        log.info("Processing single bug: %s", bug_id)

        # Fetch bug from Jira
        bug = self.jira.get_bug_details(bug_id)

        # Enrich
        self._enrich_bug(bug)

        # Classify
        self.classifier.classify(bug)

        if bug.classification not in (
            BugClassification.AUTOMATABLE,
            BugClassification.NEEDS_DR_SETUP,
        ):
            log.info(
                "Bug %s classified as %s: %s",
                bug_id,
                bug.classification.value,
                bug.classification_reason,
            )
            return None

        # Score confidence
        self.classifier.score_confidence(bug)

        # Generate, validate
        result, _prs = self._process_bug(bug, fix_version=None)
        if result and bug.confidence and bug.confidence.level == "low":
            result.needs_review = True
        return result

    def _deduplicate_bugs(self, bugs: list) -> tuple:
        """
        Group bugs by unique fix and keep only one representative per group.

        Bugs that share the same original bug ID (clone chain root) or the
        same upstream PR URL are treating the same underlying fix. We keep
        the bug with the richest context (most signals) and merge ODF
        versions from siblings into its execution spec.

        Args:
            bugs: List of classified, enriched BugInfo objects.

        Returns:
            tuple: (unique_bugs, skipped_bugs) — the representative bugs
                and the duplicates that were skipped.

        """
        groups = {}
        for bug in bugs:
            # Determine the dedup key: original bug ID > upstream PR > self
            key = bug.original_bug_id or bug.upstream_pr_url or bug.bug_id
            if key not in groups:
                groups[key] = []
            groups[key].append(bug)

        unique = []
        skipped = []

        for key, group in groups.items():
            if len(group) == 1:
                unique.append(group[0])
                continue

            # Pick the best representative: prefer the one with most context
            def _richness(b):
                score = 0
                if b.upstream_pr_diff:
                    score += 3
                if b.description and len(b.description) > 50:
                    score += 1
                if b.rovo_spec and b.rovo_spec.verification_steps:
                    score += 2
                if b.comments:
                    score += 1
                return score

            group.sort(key=_richness, reverse=True)
            representative = group[0]
            siblings = group[1:]

            # Merge ODF versions from siblings into the representative
            all_versions = set()
            if representative.execution_spec:
                all_versions.update(representative.execution_spec.odf_versions)
            all_bug_ids = [representative.bug_id]

            for sibling in siblings:
                if sibling.execution_spec:
                    all_versions.update(sibling.execution_spec.odf_versions)
                # Also grab versions from fix_versions
                for fv in sibling.fix_versions:
                    match = re.search(r"odf-(\d+\.\d+)", fv.lower())
                    if match:
                        all_versions.add(match.group(1))
                all_bug_ids.append(sibling.bug_id)
                sibling.classification_reason = (
                    f"Duplicate of {representative.bug_id} " f"(same fix: {key})"
                )

            if representative.execution_spec is None:
                representative.execution_spec = ExecutionSpec()
            representative.execution_spec.odf_versions = sorted(all_versions)

            log.info(
                "Dedup group %s: kept %s, skipped %s, merged ODF versions: %s",
                key,
                representative.bug_id,
                [s.bug_id for s in siblings],
                representative.execution_spec.odf_versions,
            )

            unique.append(representative)
            skipped.extend(siblings)

        log.info(
            "Deduplication: %d bugs -> %d unique + %d duplicates",
            len(bugs),
            len(unique),
            len(skipped),
        )
        return unique, skipped

    def _enrich_bug(self, bug: BugInfo):
        """
        Enrich a bug with upstream PR diff, AI analysis, and clone chain versions.

        Args:
            bug: The bug to enrich.

        """
        # Fetch upstream PR diff if available
        if bug.upstream_pr_url:
            log.debug("Fetching PR diff for %s: %s", bug.bug_id, bug.upstream_pr_url)
            bug.upstream_pr_diff = self.github.get_pr_diff(bug.upstream_pr_url)

        # Use Claude to analyze the bug and generate a test spec
        # This also builds the initial execution_spec from AI analysis
        try:
            bug.rovo_spec = self.generator.analyze_bug(bug)
        except Exception:
            log.warning(
                "Failed to analyze bug %s with Claude", bug.bug_id, exc_info=True
            )

        # Enrich execution spec with clone chain versions from Jira
        try:
            chain_versions = self.jira.get_clone_chain_versions(bug)
            if chain_versions:
                if bug.execution_spec is None:
                    bug.execution_spec = ExecutionSpec()
                existing = set(bug.execution_spec.odf_versions)
                existing.update(chain_versions)
                bug.execution_spec.odf_versions = sorted(existing)
                log.info(
                    "Merged clone chain versions for %s: %s",
                    bug.bug_id,
                    bug.execution_spec.odf_versions,
                )
        except Exception:
            log.warning("Failed to fetch clone chain for %s", bug.bug_id, exc_info=True)

    def _process_bug(
        self,
        bug: BugInfo,
        fix_version: Optional[str],
    ) -> tuple:
        """
        Generate, validate, and optionally publish a test for a bug.

        Args:
            bug: The classified, enriched bug.
            fix_version: The fix version (for PR creation).

        Returns:
            tuple: (GeneratedTest or None, list[PublishedPR])

        """
        try:
            # Find similar tests
            similar_tests = self.classifier.find_similar_tests(bug)

            # Generate
            test = self.generator.generate_test(bug, similar_tests)

            # Format with black
            test.code = self.validator.format_code(test.code)

            # Validate (with retry loop)
            for attempt in range(1 + self.cfg.max_retries):
                test = self.validator.validate(test)

                if test.validation_passed:
                    break

                if attempt < self.cfg.max_retries:
                    log.info(
                        "Retrying generation for %s (attempt %d/%d). Errors: %s",
                        bug.bug_id,
                        attempt + 1,
                        self.cfg.max_retries,
                        "; ".join(test.validation_errors),
                    )
                    fixed_code = self.generator.fix_validation_errors(test)
                    test.code = self.validator.format_code(fixed_code)
                    test.validation_errors = []

            if not test.validation_passed:
                log.warning(
                    "Test generation failed for %s after %d attempts. "
                    "Final errors: %s",
                    bug.bug_id,
                    1 + self.cfg.max_retries,
                    "; ".join(test.validation_errors),
                )
                bug.classification = BugClassification.GENERATION_FAILED
                bug.classification_reason = (
                    f"Validation failed: {'; '.join(test.validation_errors[:3])}"
                )
                return None, []

            # Second pass: extract helpers from inline patterns
            try:
                test = self.generator.extract_helpers(test)
                if test.helper_specs:
                    # Re-format and re-validate the rewritten test
                    test.code = self.validator.format_code(test.code)
                    test = self.validator.validate(test)
                    if not test.validation_passed:
                        log.warning(
                            "Test for %s failed validation after helper extraction, "
                            "falling back to pre-extraction code",
                            bug.bug_id,
                        )
                        # Revert: re-generate without helpers rather than fail
                        test = self.generator.generate_test(bug, similar_tests)
                        test.code = self.validator.format_code(test.code)
                        test = self.validator.validate(test)
                        test.helper_specs = []
            except Exception:
                log.warning(
                    "Helper extraction failed for %s, continuing without helpers",
                    bug.bug_id,
                    exc_info=True,
                )

            # Save generated test and helpers to output directory
            self._save_generated_test(test)

            # Publish if enabled
            published_prs = []
            if self.cfg.create_prs and fix_version:
                published_prs = self.publisher.publish_test(test, fix_version)
                for pr in published_prs:
                    log.info("Published PR: %s", pr.pr_url)

            return test, published_prs

        except Exception:
            log.error("Failed to process bug %s", bug.bug_id, exc_info=True)
            bug.classification = BugClassification.GENERATION_FAILED
            bug.classification_reason = "Unexpected error during generation"
            return None, []

    def _save_generated_test(self, test: GeneratedTest):
        """
        Save a generated test file and any extracted helpers to the output directory.

        Args:
            test: The generated test to save.

        """
        output_dir = Path(self.cfg.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save the test file
        output_path = output_dir / Path(test.file_path).name
        output_path.write_text(test.code)
        log.info("Saved generated test to %s", output_path)

        # Save helper files
        helpers_dir = output_dir / "helpers"
        helpers_meta = []
        for helper in test.helper_specs:
            helpers_dir.mkdir(parents=True, exist_ok=True)
            safe_name = helper.function_name.replace("/", "_")
            helper_path = helpers_dir / f"{safe_name}.py"
            helper_path.write_text(helper.function_code)
            log.info(
                "Saved helper %s -> %s (target: %s)",
                helper.function_name,
                helper_path,
                helper.target_file,
            )
            helpers_meta.append(helper.to_dict())

        # Save metadata
        meta_path = output_path.with_suffix(".meta.json")
        meta = {
            "bug_id": test.bug.bug_id,
            "summary": test.bug.summary,
            "component": test.bug.component.value,
            "classification": test.bug.classification.value,
            "file_path": test.file_path,
            "upstream_pr_url": test.bug.upstream_pr_url,
            "similar_tests_used": test.similar_tests_used,
            "validation_passed": test.validation_passed,
            "needs_review": test.needs_review,
            "helpers": helpers_meta,
            "generated_at": datetime.utcnow().isoformat(),
        }
        if test.bug.execution_spec:
            meta["execution_spec"] = test.bug.execution_spec.to_dict()
        if test.bug.confidence:
            meta["confidence"] = test.bug.confidence.to_dict()
        meta_path.write_text(json.dumps(meta, indent=2))

    def _save_report(self, report: ZStreamReport):
        """
        Save the pipeline report to the output directory.

        Args:
            report: The report to save.

        """
        output_dir = Path(self.cfg.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save text report
        report_path = output_dir / f"report_{report.fix_version}.txt"
        report_path.write_text(report.to_text())
        log.info("Saved report to %s", report_path)

        # Save JSON report
        json_path = output_dir / f"report_{report.fix_version}.json"
        json_data = {
            "fix_version": report.fix_version,
            "total_bugs": report.total_bugs,
            "generated_count": len(report.generated),
            "already_covered_count": len(report.already_covered),
            "manual_only_count": len(report.manual_only),
            "needs_dr_setup_count": len(report.needs_dr_setup),
            "generation_failed_count": len(report.generation_failed),
            "generated_bugs": [
                {
                    "bug_id": t.bug.bug_id,
                    "file_path": t.file_path,
                    "needs_review": t.needs_review,
                    "confidence": (
                        t.bug.confidence.to_dict() if t.bug.confidence else None
                    ),
                    "execution_spec": (
                        t.bug.execution_spec.to_dict() if t.bug.execution_spec else None
                    ),
                    "helpers": (
                        [h.to_dict() for h in t.helper_specs] if t.helper_specs else []
                    ),
                }
                for t in report.generated
            ],
            "already_covered_bugs": [
                {"bug_id": b.bug_id, "existing_test": b.existing_test_path}
                for b in report.already_covered
            ],
            "manual_only_bugs": [
                {"bug_id": b.bug_id, "reason": b.classification_reason}
                for b in report.manual_only
            ],
            "generation_failed_bugs": [
                {"bug_id": b.bug_id, "reason": b.classification_reason}
                for b in report.generation_failed
            ],
            "published_prs": [
                {
                    "bug_id": pr.bug_id,
                    "target_branch": pr.target_branch,
                    "pr_url": pr.pr_url,
                    "pr_number": pr.pr_number,
                }
                for pr in report.published_prs
            ],
        }
        json_path.write_text(json.dumps(json_data, indent=2))
