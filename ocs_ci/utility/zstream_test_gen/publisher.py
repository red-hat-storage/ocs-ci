"""
Publisher for generated tests.

Handles creating GitHub PRs, backporting to release branches,
and posting comments on Jira bugs.
"""

import logging
import re
from typing import Optional

from ocs_ci.utility.zstream_test_gen.models import (
    BugInfo,
    GeneratedTest,
    PublishedPR,
)
from ocs_ci.utility.zstream_test_gen.config import GeneratorConfig
from ocs_ci.utility.zstream_test_gen.github_client import GitHubClient
from ocs_ci.utility.zstream_test_gen.jira_client import ZStreamJiraClient

log = logging.getLogger(__name__)


class TestPublisher:
    """
    Publishes generated tests as GitHub PRs and updates Jira.

    Args:
        cfg: Generator configuration.
        github: GitHub client instance.
        jira: Jira client instance (optional, for posting comments).

    """

    def __init__(
        self,
        cfg: GeneratorConfig,
        github: GitHubClient,
        jira: Optional[ZStreamJiraClient] = None,
    ):
        self.cfg = cfg
        self.github = github
        self.jira = jira

    def publish_test(
        self,
        test: GeneratedTest,
        fix_version: str,
    ) -> list:
        """
        Publish a generated test as draft PR(s).

        Creates a PR against master with the test file and any extracted
        helpers in a single commit, then optionally creates backport PRs
        against release branches derived from execution_spec.odf_versions.

        Args:
            test: The validated, generated test.
            fix_version: The z-stream fix version string.

        Returns:
            list[PublishedPR]: List of published PRs (master + backports).

        """
        bug = test.bug
        published = []

        # Skip bugs that already have a PR submitted
        if "zstream-test-pr-submitted" in bug.labels:
            log.info(
                "Skipping PR for %s: already has zstream-test-pr-submitted label",
                bug.bug_id,
            )
            return published

        # Check confidence gate
        if self.cfg.min_confidence and bug.confidence:
            level_order = {"low": 0, "medium": 1, "high": 2}
            required = level_order.get(self.cfg.min_confidence, 0)
            actual = level_order.get(bug.confidence.level, 0)
            if actual < required:
                log.info(
                    "Skipping PR for %s: confidence %s < required %s",
                    bug.bug_id,
                    bug.confidence.level,
                    self.cfg.min_confidence,
                )
                return published

        if self.cfg.dry_run:
            log.info(
                "[DRY RUN] Would publish test for %s to %s", bug.bug_id, test.file_path
            )
            if test.helper_specs:
                for h in test.helper_specs:
                    log.info(
                        "[DRY RUN]   + helper %s -> %s", h.function_name, h.target_file
                    )
            return published

        # Build the list of files for the commit (test + helpers)
        commit_files = self._build_commit_files(test)

        # Build PR metadata
        branch_name = f"zstream-test/{bug.bug_id.lower()}-{bug.short_slug}"
        commit_message = self._build_commit_message(bug, test)
        pr_title = f"Add verification test for {bug.bug_id}"
        pr_body = self._build_pr_body(test, fix_version)

        # Final secrets gate — scan all content before pushing to GitHub
        secrets_found = self._scan_for_secrets(commit_files, pr_body, commit_message)
        if secrets_found:
            for finding in secrets_found:
                log.error("SECRETS GATE: %s", finding)
            log.error(
                "Blocking PR for %s: sensitive data detected in generated content",
                bug.bug_id,
            )
            return published

        # Build labels: base labels + bug ID + target versions
        labels = list(self.cfg.pr_labels)
        labels.append(bug.bug_id)
        if bug.execution_spec and bug.execution_spec.odf_versions:
            for ver in bug.execution_spec.odf_versions:
                labels.append(f"odf-{ver}")

        # Sync fork with upstream before pushing
        self.github.ensure_fork_synced("master")

        master_pr = self.github.create_branch_and_pr(
            branch_name=branch_name,
            target_branch="master",
            commit_message=commit_message,
            pr_title=pr_title,
            pr_body=pr_body,
            draft=self.cfg.draft_prs,
            labels=labels,
            files=commit_files,
        )

        if master_pr:
            published.append(
                PublishedPR(
                    bug_id=bug.bug_id,
                    branch=branch_name,
                    target_branch="master",
                    pr_url=master_pr["html_url"],
                    pr_number=master_pr["number"],
                )
            )
            log.info("Created master PR for %s: %s", bug.bug_id, master_pr["html_url"])

            # Create backport PRs if enabled
            if self.cfg.backport:
                backport_prs = self._create_backport_prs(
                    test=test,
                    source_branch=branch_name,
                    fix_version=fix_version,
                    commit_message=commit_message,
                    pr_body=pr_body,
                    commit_files=commit_files,
                )
                published.extend(backport_prs)

        # Post comment and label on Jira bug
        if self.jira and published:
            self._post_jira_comment(bug, published)
            self.jira.add_label(bug.bug_id, "zstream-test-pr-submitted")

        return published

    def _build_commit_files(self, test: GeneratedTest) -> list:
        """
        Build the list of files to include in the PR commit.

        Includes the test file and any extracted helper functions. Helpers
        are appended to their target files — since we can't read the existing
        file content via the API without extra calls, helpers are committed
        as standalone new files that the reviewer will manually merge into
        the target module.

        Args:
            test: The generated test with optional helper_specs.

        Returns:
            list[dict]: List of dicts with 'path' and 'content' keys.

        """
        files = [{"path": test.file_path, "content": test.code}]

        for helper in test.helper_specs:
            safe_name = helper.function_name
            helper_path = f"ocs_ci/helpers/zstream_helpers/" f"{safe_name}.py"
            header = (
                f'"""\nHelper function extracted by z-stream test generator.\n\n'
                f"Target: {helper.target_file}\n"
                f"Insertion point: {helper.insertion_point}\n"
                f"Description: {helper.description}\n\n"
                f"Review and merge this into {helper.target_file} before running the test.\n"
                f'"""\n\n'
            )
            files.append(
                {
                    "path": helper_path,
                    "content": header + helper.function_code,
                }
            )

        return files

    def _create_backport_prs(
        self,
        test: GeneratedTest,
        source_branch: str,
        fix_version: str,
        commit_message: str,
        pr_body: str,
        commit_files: list = None,
    ) -> list:
        """
        Create backport PRs for each affected release branch.

        Uses execution_spec.odf_versions to determine which release branches
        need the test, falling back to fix_version parsing.

        Args:
            test: The generated test.
            source_branch: The original feature branch.
            fix_version: The fix version string.
            commit_message: Commit message.
            pr_body: PR body text.
            commit_files: List of file dicts to include in the backport.

        Returns:
            list[PublishedPR]: List of published backport PRs.

        """
        published = []
        release_branches = self._determine_release_branches(test.bug, fix_version)

        if commit_files is None:
            commit_files = [{"path": test.file_path, "content": test.code}]

        for release_branch in release_branches:
            log.info(
                "Creating backport PR for %s -> %s",
                test.bug.bug_id,
                release_branch,
            )

            pr_title = f"Add verification test for {test.bug.bug_id}"
            backport_pr = self.github.create_backport_pr(
                source_branch=source_branch,
                release_branch=release_branch,
                commit_message=commit_message,
                pr_title=pr_title,
                pr_body=pr_body + f"\n\nBackport to `{release_branch}`.",
                draft=self.cfg.draft_prs,
                files=commit_files,
            )

            if backport_pr:
                published.append(
                    PublishedPR(
                        bug_id=test.bug.bug_id,
                        branch=f"{source_branch}-backport-{release_branch}",
                        target_branch=release_branch,
                        pr_url=backport_pr["html_url"],
                        pr_number=backport_pr["number"],
                    )
                )

        return published

    def _determine_release_branches(self, bug: BugInfo, fix_version: str) -> list:
        """
        Determine which release branches need backport PRs.

        Priority: execution_spec.odf_versions (most complete, includes clone
        chain data) > linked_issues > fix_version string.

        Args:
            bug: The bug being fixed.
            fix_version: The fix version string.

        Returns:
            list[str]: Release branch names (e.g., ["release-4.22", "release-4.21"]).

        """
        versions = set()

        # Primary source: execution_spec (includes clone chain versions)
        if bug.execution_spec and bug.execution_spec.odf_versions:
            for ver in bug.execution_spec.odf_versions:
                versions.add(ver)

        # Fallback: extract from fix_version string
        if not versions:
            match = re.search(r"(\d+\.\d+)", fix_version)
            if match:
                versions.add(match.group(1))

        # Also check linked backport issues
        for link in bug.linked_issues:
            summary = link.get("summary", "").lower()
            backport_match = re.search(r"backport to odf-(\d+\.\d+)", summary)
            if backport_match:
                versions.add(backport_match.group(1))

        branches = [f"release-{v}" for v in sorted(versions)]
        log.info(
            "Release branches for %s: %s",
            bug.bug_id,
            branches,
        )
        return branches

    def _build_commit_message(self, bug: BugInfo, test: GeneratedTest = None) -> str:
        """
        Build a commit message for the generated test.

        Args:
            bug: The bug the test verifies.
            test: The generated test (optional, for helper info).

        Returns:
            str: Commit message.

        """
        # Clean up summary for commit message
        summary = bug.summary
        summary = re.sub(r"\[Backport to odf-[\d.]+z?\]\s*-?\s*", "", summary)
        summary = re.sub(r"\[[^\]]*\]\s*-?\s*", "", summary)
        summary = summary.strip()

        if len(summary) > 72:
            summary = summary[:69] + "..."

        lines = [
            f"Add verification test for {bug.bug_id}: {summary}",
            "",
            f"Adds a test to verify the fix for {bug.bug_id}.",
        ]

        if test and test.helper_specs:
            lines.append("")
            lines.append("Includes helper functions:")
            for h in test.helper_specs:
                lines.append(f"  - {h.function_name} (target: {h.target_file})")

        if bug.upstream_pr_url:
            lines.append(f"Upstream fix: {bug.upstream_pr_url}")

        lines.append(f"Jira: https://redhat.atlassian.net/browse/{bug.bug_id}")

        if bug.execution_spec and bug.execution_spec.odf_versions:
            lines.append(f"Affects: ODF {', '.join(bug.execution_spec.odf_versions)}")

        return "\n".join(lines)

    def _build_pr_body(self, test: GeneratedTest, fix_version: str) -> str:
        """
        Build the PR body/description.

        Args:
            test: The generated test.
            fix_version: The z-stream fix version.

        Returns:
            str: PR body in markdown format.

        """
        bug = test.bug
        spec = bug.rovo_spec

        body_parts = [
            "## Summary",
            "",
            f"AI-generated verification test for [{bug.bug_id}]"
            f"(https://redhat.atlassian.net/browse/{bug.bug_id}).",
            "",
            f"**Bug**: {bug.summary}",
            f"**Fix version**: {fix_version}",
            f"**Component**: {bug.component.value}",
        ]

        if bug.upstream_pr_url:
            body_parts.append(f"**Upstream fix**: {bug.upstream_pr_url}")

        # Execution context
        if bug.execution_spec:
            es = bug.execution_spec
            body_parts.extend(["", "## Execution Context", ""])
            if es.odf_versions:
                body_parts.append(f"- **ODF versions**: {', '.join(es.odf_versions)}")
            if es.ocp_versions:
                body_parts.append(f"- **OCP versions**: {es.ocp_versions}")
            if es.platforms:
                body_parts.append(f"- **Platforms**: {', '.join(es.platforms)}")
            if es.deploy_modes:
                body_parts.append(f"- **Deploy modes**: {', '.join(es.deploy_modes)}")
            if es.acm_version:
                body_parts.append(f"- **ACM version**: {es.acm_version}")

        # Confidence
        if bug.confidence:
            body_parts.append(f"- **Confidence**: {bug.confidence.to_text()}")

        if spec and spec.root_cause:
            body_parts.extend(
                [
                    "",
                    "## Root Cause",
                    "",
                    spec.root_cause,
                ]
            )

        if spec and spec.verification_steps:
            body_parts.extend(
                [
                    "",
                    "## What This Test Verifies",
                    "",
                    spec.verification_steps,
                ]
            )

        # Helper functions section
        if test.helper_specs:
            body_parts.extend(
                [
                    "",
                    "## Helper Functions",
                    "",
                    "This PR includes helper functions that need to be merged into "
                    "their target modules. They are placed under "
                    "`ocs_ci/helpers/zstream_helpers/` as staging files.",
                    "",
                ]
            )
            for h in test.helper_specs:
                body_parts.append(f"- **`{h.function_name}`** → `{h.target_file}`")
                if h.description:
                    body_parts.append(f"  - {h.description}")
                if h.insertion_point:
                    body_parts.append(f"  - Suggested placement: {h.insertion_point}")

        # Review checklist
        checklist = [
            "",
            "## Review Checklist",
            "",
            "- [ ] Test logic correctly verifies the fix",
            "- [ ] Fixtures and cleanup are appropriate",
            "- [ ] Markers and version gating are correct",
            "- [ ] Test can run in the target environment",
        ]
        if test.helper_specs:
            checklist.append(
                "- [ ] Helper functions reviewed and merged into target modules"
            )
        checklist.extend(
            [
                "",
                "---",
                "",
                "*This test was auto-generated by the z-stream test generator. "
                "Human review is required before merging.*",
            ]
        )
        body_parts.extend(checklist)

        return "\n".join(body_parts)

    def _scan_for_secrets(
        self, commit_files: list, pr_body: str, commit_message: str
    ) -> list:
        """
        Scan all PR content for secrets before pushing to GitHub.

        Args:
            commit_files: List of file dicts with 'path' and 'content'.
            pr_body: The PR description text.
            commit_message: The commit message.

        Returns:
            list[str]: Descriptions of detected secrets, empty if clean.

        """
        from ocs_ci.utility.zstream_test_gen.validator import TestValidator

        findings = []
        for file_entry in commit_files:
            file_findings = TestValidator.scan_content_for_secrets(
                file_entry["content"]
            )
            for f in file_findings:
                findings.append(f"File {file_entry['path']}: {f}")

        body_findings = TestValidator.scan_content_for_secrets(pr_body)
        for f in body_findings:
            findings.append(f"PR body: {f}")

        msg_findings = TestValidator.scan_content_for_secrets(commit_message)
        for f in msg_findings:
            findings.append(f"Commit message: {f}")

        return findings

    def _post_jira_comment(self, bug: BugInfo, published: list):
        """
        Post a comment on the Jira bug with links to generated PRs.

        Args:
            bug: The bug to comment on.
            published: List of published PRs.

        """
        lines = ["Automated verification test generated:"]
        for pr in published:
            lines.append(f"- [{pr.target_branch} PR #{pr.pr_number}|{pr.pr_url}]")
        lines.append("")
        lines.append("Status: Draft PR -- awaiting human review.")

        try:
            self.jira.add_comment(bug.bug_id, "\n".join(lines))
            log.info("Posted Jira comment on %s", bug.bug_id)
        except Exception:
            log.warning("Failed to post Jira comment on %s", bug.bug_id, exc_info=True)
