#!/usr/bin/env python3
"""
AI-Powered Provider/Client Pattern Review

Claude-powered code review for Provider/Client pattern violations in ocs-ci.
Detects missing context managers and markers, provides intelligent suggestions.

Usage:
    # Basic pattern analysis (no AI)
    python tools/code-review/provider-client/review.py

    # With Claude AI review
    python tools/code-review/provider-client/review.py --use-claude

    # Analyze and post to GitHub PR
    python tools/code-review/provider-client/review.py --pr 1234 --use-claude --post-to-github

    # Analyze specific file with AI
    python tools/code-review/provider-client/review.py path/to/file.py --use-claude

    # Analyze committed changes
    python tools/code-review/provider-client/review.py --diff master..HEAD --use-claude

Environment Variables:
    ANTHROPIC_API_KEY - Required for --use-claude
    GITHUB_TOKEN - Required for --post-to-github

Signed-off-by: Claude Sonnet 4.5 <noreply@anthropic.com>
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from tools.code_review.provider_client.analyzers.provider_client_analyzer import (
    ProviderClientAnalyzer,
    format_findings,
)

# Optional imports for Claude and GitHub features
try:
    from tools.code_review.provider_client.analyzers.claude_reviewer import (
        ClaudeReviewer,
        format_review_comments,
    )
    CLAUDE_AVAILABLE = True
except ImportError:
    CLAUDE_AVAILABLE = False

try:
    from tools.code_review.provider_client.github_integration import (
        GitHubReviewer,
        create_summary_comment,
    )
    GITHUB_AVAILABLE = True
except ImportError:
    GITHUB_AVAILABLE = False


def get_git_diff(diff_spec: str = None) -> str:
    """
    Get git diff output.

    Args:
        diff_spec: Git diff specification (e.g., 'master..HEAD', 'HEAD~1')
                   If None, gets diff of staged + unstaged changes

    Returns:
        Git diff output
    """
    if diff_spec:
        # Specific diff range
        cmd = ["git", "diff", diff_spec]
    else:
        # Current changes (staged + unstaged)
        cmd = ["git", "diff", "HEAD"]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout


def get_pr_diff(pr_number: str) -> str:
    """
    Get diff for a specific PR using gh CLI.

    Args:
        pr_number: PR number

    Returns:
        Diff output
    """
    cmd = ["gh", "pr", "diff", pr_number]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error getting PR diff: {result.stderr}")
        sys.exit(1)
    return result.stdout


def main():
    parser = argparse.ArgumentParser(
        description="AI-Powered Provider/Client Pattern Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic analysis (pattern detection only)
  python ci/ai_review_poc.py

  # With Claude AI for intelligent suggestions
  python ci/ai_review_poc.py --use-claude

  # Analyze PR and post review comments
  python ci/ai_review_poc.py --pr 1234 --use-claude --post-to-github

  # Approve PR if no errors found
  python ci/ai_review_poc.py --pr 1234 --use-claude --post-to-github --approve
        """
    )
    parser.add_argument(
        "file",
        nargs="?",
        help="Specific file to analyze (if not analyzing diff)",
    )
    parser.add_argument(
        "--diff",
        help="Git diff specification (e.g., master..HEAD)",
    )
    parser.add_argument(
        "--pr",
        help="GitHub PR number to analyze",
    )
    parser.add_argument(
        "--patterns",
        help="Path to patterns.yaml file",
    )
    parser.add_argument(
        "--use-claude",
        action="store_true",
        help="Use Claude API for intelligent review (requires ANTHROPIC_API_KEY)",
    )
    parser.add_argument(
        "--post-to-github",
        action="store_true",
        help="Post review comments to GitHub PR (requires GITHUB_TOKEN and --pr)",
    )
    parser.add_argument(
        "--approve",
        action="store_true",
        help="Approve PR if no errors found (use with --post-to-github)",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.post_to_github and not args.pr:
        parser.error("--post-to-github requires --pr to specify the PR number")

    if args.use_claude and not CLAUDE_AVAILABLE:
        print("❌ Error: Claude integration not available.")
        print("Install required packages: pip install anthropic")
        return 1

    if args.post_to_github and not GITHUB_AVAILABLE:
        print("❌ Error: GitHub integration not available.")
        print("Ensure github_integration.py is present and gh CLI is installed.")
        return 1

    if args.use_claude and not os.getenv("ANTHROPIC_API_KEY"):
        print("❌ Error: ANTHROPIC_API_KEY environment variable not set")
        print("Get your API key from: https://console.anthropic.com/")
        return 1

    if args.post_to_github and not os.getenv("GITHUB_TOKEN"):
        print("❌ Error: GITHUB_TOKEN environment variable not set")
        print("Generate a token at: https://github.com/settings/tokens")
        return 1

    # Initialize analyzer
    analyzer = ProviderClientAnalyzer(patterns_file=args.patterns)

    # Initialize optional components
    claude_reviewer = None
    github_reviewer = None
    pr_info = None

    if args.use_claude:
        print("🤖 Initializing Claude AI reviewer...")
        try:
            claude_reviewer = ClaudeReviewer()
        except Exception as e:
            print(f"❌ Error initializing Claude: {e}")
            return 1

    if args.post_to_github:
        print("🐙 Initializing GitHub integration...")
        try:
            github_reviewer = GitHubReviewer()
            if args.pr:
                pr_info = github_reviewer.get_pr_by_number(int(args.pr))
                if not pr_info:
                    print(f"❌ Error: Could not find PR #{args.pr}")
                    return 1
                print(f"✓ Found PR #{pr_info.number}: {pr_info.head_ref} -> {pr_info.base_ref}")
        except Exception as e:
            print(f"❌ Error initializing GitHub: {e}")
            return 1

    # Determine what to analyze
    print("\n📋 Running pattern analysis...")
    if args.pr:
        print(f"   Analyzing PR #{args.pr}...\n")
        diff = get_pr_diff(args.pr)
        findings = analyzer.analyze_diff(diff)

    elif args.diff:
        print(f"   Analyzing git diff: {args.diff}...\n")
        diff = get_git_diff(args.diff)
        findings = analyzer.analyze_diff(diff)

    elif args.file:
        print(f"   Analyzing file: {args.file}...\n")
        findings = analyzer.analyze_file(args.file)

    else:
        # Default: analyze current changes
        print("   Analyzing current git changes (staged + unstaged)...\n")
        diff = get_git_diff()

        if not diff.strip():
            print("No changes detected. Try:")
            print("  - Make some changes and run again")
            print("  - Use --diff master..HEAD to analyze committed changes")
            print("  - Use --pr NUMBER to analyze a PR")
            print("  - Specify a file path to analyze a specific file")
            return 0

        findings = analyzer.analyze_diff(diff)

    # Display basic findings
    output = format_findings(findings)
    print(output)

    # Get Claude review if requested
    claude_comments = []
    if args.use_claude and findings:
        print("\n🤖 Getting Claude AI review...\n")
        try:
            context = f"PR #{args.pr}" if args.pr else None
            claude_comments = claude_reviewer.review_findings(findings, context)
            print(format_review_comments(claude_comments))
        except Exception as e:
            print(f"⚠️  Warning: Claude review failed: {e}")
            print("Continuing with basic findings only...")

    # Post to GitHub if requested
    if args.post_to_github and pr_info:
        print("\n🐙 Posting review to GitHub...\n")
        try:
            comments_to_post = claude_comments if claude_comments else []

            if findings and not comments_to_post:
                # No Claude comments, create summary instead
                summary = create_summary_comment([], len(findings))
                success = github_reviewer.post_summary_comment(pr_info, summary)
            elif comments_to_post:
                # Post Claude review comments
                success = github_reviewer.post_review_comments(
                    pr_info, comments_to_post, approve=args.approve
                )
            else:
                # No issues found
                summary = create_summary_comment([], 0)
                success = github_reviewer.post_summary_comment(pr_info, summary)

            if success:
                print(f"✓ Review posted to PR #{pr_info.number}")
                print(f"  View at: https://github.com/{pr_info.repo}/pull/{pr_info.number}")
            else:
                print("⚠️  Warning: Failed to post some comments")

        except Exception as e:
            print(f"❌ Error posting to GitHub: {e}")
            return 1

    # Summary
    print("\n" + "=" * 60)
    if findings:
        errors = sum(1 for f in findings if f.severity == "error")
        warnings = sum(1 for f in findings if f.severity == "warning")
        print(f"Summary: {errors} error(s), {warnings} warning(s)")

        if claude_comments:
            print(f"Claude generated {len(claude_comments)} intelligent review comment(s)")

        if args.post_to_github:
            print(f"Review posted to GitHub PR #{pr_info.number}")
    else:
        print("✨ All checks passed! Code follows Provider/Client patterns correctly.")

    print("=" * 60)

    # Exit with error code if there are errors
    return 1 if findings and any(f.severity == "error" for f in findings) else 0


if __name__ == "__main__":
    sys.exit(main())
