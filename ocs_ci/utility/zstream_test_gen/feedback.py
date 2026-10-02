"""
Automated feedback loop for the z-stream test generator.

After each run, checks previously submitted PRs for reviewer corrections.
Extracts patterns from the diffs and stores them as correction rules that
are automatically loaded into future generation prompts.

Labels used:
- ``ai-generated``: applied when a PR is created by the tool
- ``zstream-feedback-collected``: applied after feedback is extracted
"""

import logging
import subprocess
from pathlib import Path
from typing import Optional

import yaml

from ocs_ci.utility.zstream_test_gen.config import GeneratorConfig

log = logging.getLogger(__name__)

LABEL_GENERATED = "ai-generated"
LABEL_GENERATED_ALT = "automation-for-bug-verification"
LABEL_FEEDBACK_DONE = "zstream-feedback-collected"
LABEL_REVIEW_FIXED = "zstream-review-addressed"
CORRECTIONS_FILE = Path.home() / ".zstream_test_gen_corrections.yaml"
PROCESSED_PRS_FILE = Path.home() / ".zstream_test_gen_processed_prs.yaml"

FEEDBACK_ANALYSIS_PROMPT = """\
You are reviewing corrections that human reviewers made to AI-generated \
ocs-ci pytest tests. Your job is to extract reusable rules that will \
improve future test generation.

For each PR below you will see:
- The original AI-generated code
- The final merged code (after reviewer edits)
- Reviewer comments (if any)

Analyze the differences and extract ONLY rules that are:
1. General enough to apply to future tests (not bug-specific)
2. About coding patterns, conventions, or best practices
3. Actionable — something the AI can concretely change

For each rule, output exactly this format (one per rule):

### RULE
CATEGORY: <one of: convention, pattern, import, fixture, assertion, cleanup, naming>
RULE: <one-sentence rule the AI must follow>
EXAMPLE: <short before/after code snippet showing the correction>
### END_RULE

If a PR was merged without changes, output:
### NO_CORRECTIONS
PR #{pr_number}: merged without changes — generation was correct.
### END_NO_CORRECTIONS

If a PR was closed without merging, output:
### REJECTED
PR #{pr_number}: <brief reason why it was rejected, if discernible>
### END_REJECTED

PRs to analyze:

{pr_analyses}
"""


class FeedbackCollector:
    """
    Collects feedback from previously submitted PRs and extracts
    correction rules for future generations.

    Args:
        cfg: Generator configuration.
        generator: TestGenerator instance for Claude API access.

    """

    def __init__(self, cfg: GeneratorConfig, generator=None):
        self.cfg = cfg
        self._generator = generator

    @property
    def generator(self):
        if self._generator is None:
            from ocs_ci.utility.zstream_test_gen.generator import TestGenerator

            self._generator = TestGenerator(self.cfg)
        return self._generator

    def collect_feedback(self) -> dict:
        """
        Main entry point. Finds unprocessed PRs, analyzes corrections,
        saves new rules, and marks PRs as processed.

        Returns:
            dict: Summary with counts of processed, corrected, and
                  rejected PRs, plus any new rules added.

        """
        summary = {
            "processed": 0,
            "corrected": 0,
            "merged_clean": 0,
            "rejected": 0,
            "new_rules": 0,
        }

        prs = self._find_unprocessed_prs()
        if not prs:
            log.info("No unprocessed AI-generated PRs found")
            return summary

        log.info("Found %d unprocessed AI-generated PRs", len(prs))

        pr_analyses = []
        for pr in prs:
            analysis = self._analyze_pr(pr)
            if analysis:
                pr_analyses.append(analysis)

        if not pr_analyses:
            log.info("No analyzable PR diffs found")
            return summary

        new_rules = self._extract_rules(pr_analyses)
        summary["new_rules"] = len(new_rules)

        if new_rules:
            self._save_corrections(new_rules)
            log.info("Saved %d new correction rules", len(new_rules))

        for pr in prs:
            status = pr.get("status", "")
            if status == "merged_clean":
                summary["merged_clean"] += 1
            elif status == "corrected":
                summary["corrected"] += 1
            elif status == "rejected":
                summary["rejected"] += 1
            summary["processed"] += 1
            self._mark_pr_processed(pr["number"])

        return summary

    def _find_unprocessed_prs(self) -> list:
        """
        Find merged or closed PRs that have the ai-generated label
        but not the zstream-feedback-collected label.

        Returns:
            list[dict]: PR metadata dicts with number, state, title.

        """
        repo = self.cfg.github.ocsci_repo
        try:
            result = subprocess.run(
                [
                    "gh",
                    "pr",
                    "list",
                    "--repo",
                    repo,
                    "--label",
                    LABEL_GENERATED,
                    "--state",
                    "all",
                    "--json",
                    "number,title,state,labels,mergedAt,closedAt",
                    "--limit",
                    "50",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                log.warning("gh pr list failed: %s", result.stderr.strip())
                return []

            import json

            prs = json.loads(result.stdout)

            unprocessed = []
            for pr in prs:
                label_names = [l.get("name", "") for l in pr.get("labels", [])]
                if LABEL_FEEDBACK_DONE in label_names:
                    continue
                if pr["state"] not in ("MERGED", "CLOSED"):
                    continue
                unprocessed.append(
                    {
                        "number": pr["number"],
                        "title": pr["title"],
                        "state": pr["state"],
                    }
                )

            return unprocessed

        except (subprocess.SubprocessError, Exception):
            log.warning("Failed to query PRs for feedback", exc_info=True)
            return []

    def _analyze_pr(self, pr: dict) -> Optional[dict]:
        """
        Analyze a single PR to extract the diff between original
        generation and final merged state, plus reviewer comments.

        Args:
            pr: PR metadata dict.

        Returns:
            Optional[dict]: Analysis dict with diff and comments, or None.

        """
        repo = self.cfg.github.ocsci_repo
        pr_number = pr["number"]

        if pr["state"] == "CLOSED":
            comments = self._get_pr_comments(pr_number)
            pr["status"] = "rejected"
            return {
                "number": pr_number,
                "title": pr["title"],
                "state": "closed",
                "diff": None,
                "comments": comments,
            }

        try:
            result = subprocess.run(
                [
                    "gh",
                    "pr",
                    "diff",
                    str(pr_number),
                    "--repo",
                    repo,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                log.warning(
                    "Failed to get diff for PR #%d: %s",
                    pr_number,
                    result.stderr.strip(),
                )
                return None

            commits = self._get_pr_commits(pr_number)
            if len(commits) <= 1:
                pr["status"] = "merged_clean"
                return {
                    "number": pr_number,
                    "title": pr["title"],
                    "state": "merged_clean",
                    "diff": None,
                    "comments": [],
                }

            first_sha = commits[0]
            last_sha = commits[-1]
            correction_diff = self._get_commit_range_diff(
                pr_number, first_sha, last_sha
            )

            if not correction_diff or correction_diff.strip() == "":
                pr["status"] = "merged_clean"
                return {
                    "number": pr_number,
                    "title": pr["title"],
                    "state": "merged_clean",
                    "diff": None,
                    "comments": [],
                }

            comments = self._get_pr_comments(pr_number)
            pr["status"] = "corrected"

            return {
                "number": pr_number,
                "title": pr["title"],
                "state": "corrected",
                "diff": correction_diff[:10000],
                "comments": comments,
            }

        except (subprocess.SubprocessError, Exception):
            log.warning("Failed to analyze PR #%d", pr_number, exc_info=True)
            return None

    def _get_pr_commits(self, pr_number: int) -> list:
        """Get the list of commit SHAs for a PR."""
        repo = self.cfg.github.ocsci_repo
        try:
            result = subprocess.run(
                [
                    "gh",
                    "api",
                    f"repos/{repo}/pulls/{pr_number}/commits",
                    "--jq",
                    ".[].sha",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                return [
                    s.strip() for s in result.stdout.strip().split("\n") if s.strip()
                ]
        except subprocess.SubprocessError:
            pass
        return []

    def _get_commit_range_diff(
        self, pr_number: int, first_sha: str, last_sha: str
    ) -> Optional[str]:
        """Get the diff between the first and last commits of a PR."""
        repo = self.cfg.github.ocsci_repo
        try:
            result = subprocess.run(
                [
                    "gh",
                    "api",
                    f"repos/{repo}/compare/{first_sha}...{last_sha}",
                    "--jq",
                    ".files[] | .filename, .patch",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                return result.stdout
        except subprocess.SubprocessError:
            pass
        return None

    def _get_pr_comments(self, pr_number: int) -> list:
        """Get review comments for a PR."""
        repo = self.cfg.github.ocsci_repo
        try:
            result = subprocess.run(
                [
                    "gh",
                    "api",
                    f"repos/{repo}/pulls/{pr_number}/comments",
                    "--jq",
                    '.[] | "\\(.user.login): \\(.body)"',
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip().split("\n")
        except subprocess.SubprocessError:
            pass
        return []

    def _extract_rules(self, pr_analyses: list) -> list:
        """
        Send PR analyses to Claude to extract reusable correction rules.

        Args:
            pr_analyses: List of analysis dicts from _analyze_pr.

        Returns:
            list[dict]: Extracted rules with category, rule, and example.

        """
        parts = []
        for analysis in pr_analyses:
            section = f"## PR #{analysis['number']}: {analysis['title']}\n"
            section += f"State: {analysis['state']}\n\n"

            if analysis.get("diff"):
                section += "### Reviewer corrections (diff):\n"
                section += f"```diff\n{analysis['diff']}\n```\n\n"

            if analysis.get("comments"):
                section += "### Reviewer comments:\n"
                for comment in analysis["comments"]:
                    section += f"- {comment}\n"
                section += "\n"

            parts.append(section)

        if not parts:
            return []

        prompt = FEEDBACK_ANALYSIS_PROMPT.format(
            pr_number="(see below)", pr_analyses="\n".join(parts)
        )

        try:
            response = self.generator.call_claude(
                prompt, system_prompt="You are an expert code review analyst."
            )
            return self._parse_rules(response)
        except Exception:
            log.warning("Failed to extract rules via Claude", exc_info=True)
            return []

    def _parse_rules(self, response: str) -> list:
        """Parse rule blocks from Claude's response."""
        rules = []
        blocks = response.split("### RULE")

        for block in blocks[1:]:
            if "### END_RULE" not in block:
                continue
            content = block.split("### END_RULE")[0].strip()

            rule = {}
            for line in content.split("\n"):
                line = line.strip()
                if line.startswith("CATEGORY:"):
                    rule["category"] = line.split(":", 1)[1].strip()
                elif line.startswith("RULE:"):
                    rule["rule"] = line.split(":", 1)[1].strip()
                elif line.startswith("EXAMPLE:"):
                    rule["example"] = line.split(":", 1)[1].strip()

            if "rule" in rule:
                rules.append(rule)

        return rules

    def _save_corrections(self, new_rules: list):
        """
        Append new correction rules to the persistent corrections file.

        Deduplicates by rule text to avoid storing the same correction
        twice across multiple feedback runs.

        Args:
            new_rules: List of rule dicts to save.

        """
        existing = self.load_corrections()
        existing_texts = {r.get("rule", "") for r in existing}

        added = 0
        for rule in new_rules:
            if rule.get("rule") and rule["rule"] not in existing_texts:
                existing.append(rule)
                existing_texts.add(rule["rule"])
                added += 1
                log.info(
                    "New correction rule [%s]: %s",
                    rule.get("category", "general"),
                    rule["rule"],
                )

        if added > 0:
            with open(CORRECTIONS_FILE, "w") as f:
                yaml.dump(
                    {"corrections": existing},
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                )
            log.info(
                "Corrections file updated: %d total rules (%d new)",
                len(existing),
                added,
            )

    def _mark_pr_processed(self, pr_number: int):
        """Add the feedback-collected label to a PR."""
        repo = self.cfg.github.ocsci_repo
        try:
            subprocess.run(
                [
                    "gh",
                    "pr",
                    "edit",
                    str(pr_number),
                    "--repo",
                    repo,
                    "--add-label",
                    LABEL_FEEDBACK_DONE,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            log.info("Marked PR #%d with label %s", pr_number, LABEL_FEEDBACK_DONE)
        except subprocess.SubprocessError:
            log.warning("Failed to label PR #%d", pr_number, exc_info=True)

    @staticmethod
    def load_corrections() -> list:
        """
        Load correction rules from the persistent file.

        Returns:
            list[dict]: Stored correction rules.

        """
        if not CORRECTIONS_FILE.exists():
            return []
        try:
            with open(CORRECTIONS_FILE) as f:
                data = yaml.safe_load(f) or {}
            return data.get("corrections", [])
        except Exception:
            log.warning("Failed to load corrections file", exc_info=True)
            return []

    @staticmethod
    def format_corrections_for_prompt() -> str:
        """
        Format stored corrections as a prompt section.

        Returns:
            str: Formatted corrections block, or empty string if none.

        """
        corrections = FeedbackCollector.load_corrections()
        if not corrections:
            return ""

        lines = [
            "\n## Learned Corrections (from reviewer feedback)\n",
            "The following rules were extracted from reviewer corrections "
            "on previously generated tests. Follow them strictly:\n",
        ]

        by_category = {}
        for rule in corrections:
            cat = rule.get("category", "general")
            by_category.setdefault(cat, []).append(rule)

        for category, rules in sorted(by_category.items()):
            lines.append(f"\n### {category.title()}")
            for rule in rules:
                lines.append(f"- {rule['rule']}")
                if rule.get("example"):
                    lines.append(f"  Example: {rule['example']}")

        return "\n".join(lines)


REVIEW_FIX_PROMPT = """\
You are an expert ocs-ci test engineer fixing code review comments on an \
AI-generated pytest test for OpenShift Data Foundation.

Below are the current file contents and the reviewer comments (from CodeRabbit). \
Fix ALL the issues raised. Return the complete, corrected file content for \
each file that needs changes.

## Important ocs-ci conventions
- Never use `time.sleep()` for waiting on async operations. Use \
`TimeoutSampler` from `ocs_ci.utility.utils` for polling.
- Teardown/finalizer errors must be re-raised, not swallowed with broad \
`except Exception`.
- All helpers must be self-contained with their own imports (logging, etc.).
- Constants must go into `ocs_ci/ocs/constants.py`, not standalone files.
- Helper functions intended for an existing module (e.g., `dr_helpers`) must \
be placed there directly, not in `zstream_helpers/`.
- Import paths in the test must match where the helpers actually exist.
- Use `request.addfinalizer()` for cleanup, never `yield`.

For each file that needs changes, output exactly:

### FILE: <file_path>
```python
<complete corrected file content>
```
### END_FILE

If a file should be deleted (e.g., misnamed constants file), output:
### DELETE: <file_path>
### END_DELETE

If a file should be moved/renamed, output the DELETE for the old path and a \
FILE block for the new path.

## Current files in this PR:

{file_contents}

## CodeRabbit review comments:

{review_comments}
"""


class ReviewFixer:
    """
    Processes CodeRabbit review comments on open PRs, generates fixes
    using Claude, pushes fix commits, and extracts learning rules.

    Args:
        cfg: Generator configuration.
        github: GitHubClient instance.
        generator: TestGenerator instance for Claude API access.

    """

    def __init__(self, cfg, github, generator=None):
        self.cfg = cfg
        self.github = github
        self._generator = generator

    @property
    def generator(self):
        if self._generator is None:
            from ocs_ci.utility.zstream_test_gen.generator import TestGenerator

            self._generator = TestGenerator(self.cfg)
        return self._generator

    def fix_open_prs(self) -> dict:
        """
        Find open PRs with CodeRabbit comments and fix them.

        Returns:
            dict: Summary with counts of fixed PRs and new rules.

        """
        summary = {
            "prs_checked": 0,
            "prs_fixed": 0,
            "commits_pushed": 0,
            "new_rules": 0,
        }

        prs = self._find_reviewable_prs()
        if not prs:
            log.info("No open PRs with unaddressed CodeRabbit comments")
            return summary

        log.info("Found %d open PRs with review comments to address", len(prs))

        all_analyses = []
        for pr in prs:
            summary["prs_checked"] += 1
            comments = pr.get("comments", [])

            log.info(
                "PR #%d: %d CodeRabbit comments to address",
                pr["number"],
                len(comments),
            )

            fixed = self._fix_pr(pr, comments)
            if fixed:
                summary["prs_fixed"] += 1
                summary["commits_pushed"] += 1
                all_analyses.append({"number": pr["number"], "comments": comments})

        if all_analyses:
            rules = self._extract_rules_from_comments(all_analyses)
            if rules:
                collector = FeedbackCollector(self.cfg)
                collector._save_corrections(rules)
                summary["new_rules"] = len(rules)
                log.info("Learned %d new rules from review comments", len(rules))

        return summary

    def _find_reviewable_prs(self) -> list:
        """
        Find open PRs with AI-generated labels that have unaddressed comments.

        Tracks how many CodeRabbit comments were addressed per PR. If new
        comments appear after a fix commit, the PR is reprocessed.

        """
        import json

        repo = self.cfg.github.ocsci_repo
        processed = self._load_processed_prs()
        seen = set()
        reviewable = []

        for label in (LABEL_GENERATED, LABEL_GENERATED_ALT):
            try:
                result = subprocess.run(
                    [
                        "gh",
                        "pr",
                        "list",
                        "--repo",
                        repo,
                        "--label",
                        label,
                        "--state",
                        "open",
                        "--json",
                        "number,title,labels",
                        "--limit",
                        "50",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.returncode != 0:
                    continue

                prs = json.loads(result.stdout)
                for pr in prs:
                    pr_num = pr["number"]
                    if pr_num in seen:
                        continue
                    seen.add(pr_num)

                    current_comments = self.github.get_coderabbit_comments(pr_num)
                    current_count = len(current_comments)

                    prev_count = processed.get(pr_num, -1)
                    if current_count == 0:
                        continue
                    if current_count <= prev_count:
                        log.debug(
                            "PR #%d: %d comments, already addressed %d",
                            pr_num,
                            current_count,
                            prev_count,
                        )
                        continue

                    if prev_count >= 0:
                        log.info(
                            "PR #%d: %d new comments since last fix",
                            pr_num,
                            current_count - prev_count,
                        )

                    reviewable.append(
                        {
                            "number": pr_num,
                            "title": pr["title"],
                            "comments": current_comments,
                        }
                    )

            except (subprocess.SubprocessError, Exception):
                log.warning(
                    "Failed to query open PRs with label %s",
                    label,
                    exc_info=True,
                )

        return reviewable

    def _fix_pr(self, pr: dict, comments: list) -> bool:
        """
        Generate and push fixes for a single PR's review comments.

        Args:
            pr: PR metadata dict with 'number' and 'title'.
            comments: List of CodeRabbit comment dicts.

        Returns:
            bool: True if a fix commit was pushed.

        """
        pr_number = pr["number"]

        head = self.github.get_pr_head_ref(pr_number)
        if not head:
            log.warning("Could not get head ref for PR #%d", pr_number)
            return False

        head_repo = head["repo"]
        head_branch = head["branch"]
        head_sha = head["sha"]

        pr_files = self._get_pr_file_paths(pr_number)
        if not pr_files:
            log.warning("No files found in PR #%d", pr_number)
            return False

        file_contents = {}
        for file_path in pr_files:
            content = self.github.get_file_content_from_ref(
                head_repo, file_path, head_sha
            )
            if content is not None:
                file_contents[file_path] = content

        if not file_contents:
            log.warning("Could not read any files from PR #%d", pr_number)
            return False

        fixed_files, deleted_files = self._generate_fixes(file_contents, comments)

        if not fixed_files and not deleted_files:
            log.info("PR #%d: no fixes generated", pr_number)
            return False

        commit_files = []
        for path, content in fixed_files.items():
            commit_files.append({"path": path, "content": content})

        if commit_files:
            new_sha = self.github.push_commit_to_branch(
                repo=head_repo,
                branch=head_branch,
                files=commit_files,
                commit_message=(
                    "Address CodeRabbit review comments\n\n"
                    "Auto-generated fixes for review feedback."
                ),
            )

            if not new_sha:
                log.error("Failed to push fix commit for PR #%d", pr_number)
                return False

        self._label_pr_fixed(pr_number, len(comments))
        log.info(
            "PR #%d: fix commit pushed (%d comments addressed)",
            pr_number,
            len(comments),
        )
        return True

    def _get_pr_file_paths(self, pr_number: int) -> list:
        """Get the list of file paths changed in a PR."""
        repo = self.cfg.github.ocsci_repo
        try:
            result = subprocess.run(
                [
                    "gh",
                    "api",
                    f"repos/{repo}/pulls/{pr_number}/files",
                    "--jq",
                    ".[].filename",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                return [
                    f.strip() for f in result.stdout.strip().split("\n") if f.strip()
                ]
        except subprocess.SubprocessError:
            pass
        return []

    def _generate_fixes(self, file_contents: dict, comments: list) -> tuple:
        """
        Use Claude to generate fixes for review comments.

        Args:
            file_contents: Dict mapping file path -> current content.
            comments: List of CodeRabbit comment dicts.

        Returns:
            tuple: (fixed_files dict, deleted_files list)

        """
        import re

        files_section = []
        for path, content in file_contents.items():
            files_section.append(f"### {path}\n```python\n{content}\n```\n")

        comments_section = []
        for comment in comments:
            body = self._clean_coderabbit_comment(comment.get("body", ""))
            comments_section.append(
                f"**File: {comment.get('path', 'unknown')}"
                f" (line {comment.get('line', '?')})**\n{body}\n"
            )

        prompt = REVIEW_FIX_PROMPT.format(
            file_contents="\n".join(files_section),
            review_comments="\n".join(comments_section),
        )

        try:
            response = self.generator.call_claude(prompt)
        except Exception:
            log.warning("Claude API call failed for review fixes", exc_info=True)
            return {}, []

        fixed_files = {}
        deleted_files = []

        file_blocks = re.split(r"### FILE:\s*", response)
        for block in file_blocks[1:]:
            if "### END_FILE" not in block:
                continue
            content_part = block.split("### END_FILE")[0]
            lines = content_part.strip().split("\n", 1)
            if len(lines) < 2:
                continue
            path = lines[0].strip()
            code = lines[1].strip()
            if code.startswith("```python"):
                code = code[len("```python") :].strip()
            if code.endswith("```"):
                code = code[: -len("```")].strip()
            fixed_files[path] = code

        delete_blocks = re.split(r"### DELETE:\s*", response)
        for block in delete_blocks[1:]:
            if "### END_DELETE" not in block:
                continue
            path = block.split("### END_DELETE")[0].strip()
            deleted_files.append(path)

        return fixed_files, deleted_files

    def _clean_coderabbit_comment(self, body: str) -> str:
        """Strip HTML details blocks and metadata from CodeRabbit comments."""
        import re

        body = re.sub(r"<details>.*?</details>", "", body, flags=re.DOTALL)
        body = re.sub(r"<!--.*?-->", "", body, flags=re.DOTALL)
        body = re.sub(r"_Source:.*$", "", body, flags=re.MULTILINE)
        body = body.strip()
        return body

    def _label_pr_fixed(self, pr_number: int, comment_count: int = 0):
        """Record the PR and comment count in the local tracking file."""
        processed = self._load_processed_prs()
        processed[pr_number] = comment_count
        self._save_processed_prs(processed)
        log.info("Marked PR #%d as addressed (%d comments)", pr_number, comment_count)

    @staticmethod
    def _load_processed_prs() -> dict:
        """Load the dict of PR number -> comment count that have been processed."""
        if not PROCESSED_PRS_FILE.exists():
            return {}
        try:
            with open(PROCESSED_PRS_FILE) as f:
                data = yaml.safe_load(f) or {}
            raw = data.get("review_fixed_prs", {})
            if isinstance(raw, list):
                return {pr: 0 for pr in raw}
            return {int(k): v for k, v in raw.items()}
        except Exception:
            return {}

    @staticmethod
    def _save_processed_prs(pr_data: dict):
        """Save the dict of PR number -> comment count."""
        with open(PROCESSED_PRS_FILE, "w") as f:
            yaml.dump(
                {"review_fixed_prs": pr_data},
                f,
                default_flow_style=False,
            )

    def _extract_rules_from_comments(self, analyses: list) -> list:
        """
        Extract reusable rules from CodeRabbit comments.

        Args:
            analyses: List of dicts with 'number' and 'comments'.

        Returns:
            list[dict]: Extracted correction rules.

        """
        parts = []
        for analysis in analyses:
            section = f"## PR #{analysis['number']}\n\n"
            section += "### CodeRabbit review comments:\n"
            for comment in analysis["comments"]:
                body = self._clean_coderabbit_comment(comment.get("body", ""))
                section += (
                    f"\n**{comment.get('path', '')}:{comment.get('line', '')}**\n"
                    f"{body}\n"
                )
            parts.append(section)

        prompt = FEEDBACK_ANALYSIS_PROMPT.format(
            pr_number="(see below)", pr_analyses="\n".join(parts)
        )

        try:
            response = self.generator.call_claude(
                prompt,
                system_prompt="You are an expert code review analyst.",
            )
            collector = FeedbackCollector(self.cfg)
            return collector._parse_rules(response)
        except Exception:
            log.warning("Failed to extract rules from comments", exc_info=True)
            return []
