"""
GitHub integration for posting PR review comments.

Handles posting Claude-generated review comments to GitHub Pull Requests.

Signed-off-by: Claude Sonnet 4.5 <noreply@anthropic.com>
"""

import os
import subprocess
import json
from typing import List, Optional, Dict
from dataclasses import dataclass

from tools.code_review.provider_client.analyzers.claude_reviewer import ReviewComment


@dataclass
class PRInfo:
    """Information about a GitHub Pull Request"""

    number: int
    repo: str  # Format: "owner/repo"
    base_ref: str
    head_ref: str
    head_sha: str


class GitHubReviewer:
    """Posts review comments to GitHub PRs"""

    def __init__(self, token: Optional[str] = None):
        """
        Initialize GitHub reviewer.

        Args:
            token: GitHub token (defaults to GITHUB_TOKEN env var)
        """
        self.token = token or os.getenv("GITHUB_TOKEN")
        if not self.token:
            raise ValueError(
                "GITHUB_TOKEN environment variable or token parameter required"
            )

        # Check if gh CLI is available
        try:
            subprocess.run(
                ["gh", "--version"],
                capture_output=True,
                check=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError(
                "GitHub CLI (gh) not found. Install from: https://cli.github.com/"
            )

    def get_current_pr(self) -> Optional[PRInfo]:
        """
        Get PR information for current branch.

        Returns:
            PRInfo if current branch has an open PR, None otherwise
        """
        try:
            # Get PR info using gh CLI
            result = subprocess.run(
                ["gh", "pr", "view", "--json", "number,baseRefName,headRefName,headRefOid,repository"],
                capture_output=True,
                text=True,
                check=True,
            )

            data = json.loads(result.stdout)

            return PRInfo(
                number=data["number"],
                repo=f"{data['repository']['owner']['login']}/{data['repository']['name']}",
                base_ref=data["baseRefName"],
                head_ref=data["headRefName"],
                head_sha=data["headRefOid"],
            )

        except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError):
            return None

    def get_pr_by_number(self, pr_number: int) -> Optional[PRInfo]:
        """
        Get PR information by PR number.

        Args:
            pr_number: PR number

        Returns:
            PRInfo or None if not found
        """
        try:
            result = subprocess.run(
                [
                    "gh",
                    "pr",
                    "view",
                    str(pr_number),
                    "--json",
                    "number,baseRefName,headRefName,headRefOid,repository",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            data = json.loads(result.stdout)

            return PRInfo(
                number=data["number"],
                repo=f"{data['repository']['owner']['login']}/{data['repository']['name']}",
                base_ref=data["baseRefName"],
                head_ref=data["headRefName"],
                head_sha=data["headRefOid"],
            )

        except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError):
            return None

    def post_review_comments(
        self, pr_info: PRInfo, comments: List[ReviewComment], approve: bool = False
    ) -> bool:
        """
        Post review comments to a PR.

        Args:
            pr_info: PR information
            comments: List of review comments to post
            approve: If True and no errors, approve the PR

        Returns:
            True if successful, False otherwise
        """
        if not comments:
            return True

        try:
            # Determine review event based on severity
            has_errors = any(c.severity == "error" for c in comments)

            if has_errors:
                event = "REQUEST_CHANGES"
            elif approve:
                event = "APPROVE"
            else:
                event = "COMMENT"

            # Build review body
            body_parts = [
                "## 🤖 AI Code Review - Provider/Client Patterns",
                "",
                f"Found {len(comments)} issue(s) in this PR.",
                "",
            ]

            if has_errors:
                body_parts.append(
                    "⚠️ **Errors detected:** Please fix the issues below before merging."
                )
            else:
                body_parts.append(
                    "💡 **Suggestions:** Consider these improvements to follow Provider/Client patterns."
                )

            body_parts.extend(
                [
                    "",
                    "---",
                    "",
                    "Powered by [Claude Code](https://claude.ai/code) | ",
                    f"Reviewed {len(comments)} location(s)",
                ]
            )

            review_body = "\n".join(body_parts)

            # Post review using gh CLI
            # Note: gh pr review doesn't support inline comments directly
            # We'll post a review comment and then individual comments

            # First, post the main review
            subprocess.run(
                [
                    "gh",
                    "pr",
                    "review",
                    str(pr_info.number),
                    "--body",
                    review_body,
                    f"--{event.lower()}",
                ],
                check=True,
                env={**os.environ, "GITHUB_TOKEN": self.token},
            )

            # Then post individual inline comments using gh api
            for comment in comments:
                self._post_inline_comment(pr_info, comment)

            return True

        except subprocess.CalledProcessError as e:
            print(f"Error posting review: {e}")
            return False

    def _post_inline_comment(self, pr_info: PRInfo, comment: ReviewComment):
        """Post a single inline comment on a PR"""
        try:
            # Use gh api to post inline review comment
            comment_data = {
                "body": f"🤖 **Claude Review**\n\n{comment.body}",
                "path": comment.file_path,
                "line": comment.line_number,
                "side": "RIGHT",  # Comment on new version of file
            }

            subprocess.run(
                [
                    "gh",
                    "api",
                    f"repos/{pr_info.repo}/pulls/{pr_info.number}/comments",
                    "-f",
                    f"body={comment_data['body']}",
                    "-f",
                    f"path={comment_data['path']}",
                    "-F",
                    f"line={comment_data['line']}",
                    "-f",
                    f"side={comment_data['side']}",
                    "-f",
                    f"commit_id={pr_info.head_sha}",
                ],
                check=True,
                capture_output=True,
                env={**os.environ, "GITHUB_TOKEN": self.token},
            )

        except subprocess.CalledProcessError as e:
            # Non-fatal: log but continue
            print(
                f"Warning: Could not post inline comment at {comment.file_path}:{comment.line_number}"
            )
            print(f"Error: {e.stderr.decode() if e.stderr else str(e)}")

    def post_summary_comment(self, pr_info: PRInfo, summary: str) -> bool:
        """
        Post a summary comment (not a review).

        Args:
            pr_info: PR information
            summary: Markdown-formatted summary

        Returns:
            True if successful
        """
        try:
            subprocess.run(
                [
                    "gh",
                    "pr",
                    "comment",
                    str(pr_info.number),
                    "--body",
                    summary,
                ],
                check=True,
                env={**os.environ, "GITHUB_TOKEN": self.token},
            )
            return True

        except subprocess.CalledProcessError:
            return False


def create_summary_comment(
    comments: List[ReviewComment], analyzer_findings_count: int
) -> str:
    """Create a summary comment for PR"""

    if not comments:
        return """## ✅ Provider/Client Pattern Check Passed

No issues detected! Your code correctly follows Provider/Client patterns.

🤖 *Powered by [Claude Code](https://claude.ai/code)*
"""

    errors = [c for c in comments if c.severity == "error"]
    warnings = [c for c in comments if c.severity == "warning"]

    summary_parts = [
        "## 🤖 AI Code Review - Provider/Client Patterns",
        "",
        f"**Status:** {'❌ Issues Found' if errors else '⚠️ Suggestions Available'}",
        "",
        f"- **Errors:** {len(errors)}",
        f"- **Warnings:** {len(warnings)}",
        f"- **Locations checked:** {analyzer_findings_count}",
        "",
        "### Summary",
        "",
    ]

    if errors:
        summary_parts.append(
            "The following errors must be addressed before merging:"
        )
        summary_parts.append("")
        for comment in errors[:3]:  # Show first 3
            summary_parts.append(
                f"- `{comment.file_path}:{comment.line_number}` - Missing provider context"
            )
        if len(errors) > 3:
            summary_parts.append(f"- *...and {len(errors) - 3} more*")
        summary_parts.append("")

    summary_parts.extend(
        [
            "### What to do",
            "",
            "1. Check inline comments on affected lines",
            "2. Apply suggested fixes (wrap in `RunWithProviderConfigContextIfAvailable()`)",
            "3. Or add `@runs_on_provider` marker to test functions",
            "4. Re-run the review after pushing fixes",
            "",
            "---",
            "",
            "🤖 *Powered by [Claude Code](https://claude.ai/code)*",
        ]
    )

    return "\n".join(summary_parts)
