"""
CLI entry point for the AI-powered log analysis tool.

Usage:
    analyze-logs <source> [options]

Examples:
    analyze-logs http://magna002.ceph.redhat.com/ocsci-jenkins/.../logs/
    analyze-logs /tmp/ocs-ci-logs-1234567/ -f json -o analysis.json
    analyze-logs http://magna002.../logs/ --known-issues-only
"""

import argparse
import logging
import sys
import urllib3

from ocs_ci.utility.log_analysis import analyze_run
from ocs_ci.utility.log_analysis.reporting.report_builder import ReportBuilder

logger = logging.getLogger(__name__)


def main(argv=None):
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="analyze-logs",
        description="AI-powered analysis of OCS-CI test run logs",
    )

    parser.add_argument(
        "source",
        help="URL or local path to log directory",
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=["json", "markdown", "html"],
        default="markdown",
        dest="output_format",
        help="Output format (default: markdown)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--ai-backend",
        choices=["claude-code", "anthropic", "none"],
        default="claude-code",
        help="AI backend to use (default: claude-code)",
    )
    parser.add_argument(
        "--model",
        default="sonnet",
        help="AI model to use (default: sonnet)",
    )
    parser.add_argument(
        "--known-issues-only",
        action="store_true",
        help="Only run regex-based known issue matching (no AI, no cost)",
    )
    parser.add_argument(
        "--no-jira",
        action="store_true",
        help="Skip Jira integration",
    )
    parser.add_argument(
        "--jira-config",
        default=None,
        help="Path to Jira INI config file (with url and token in [DEFAULT] section)",
    )
    parser.add_argument(
        "--known-issues-file",
        default=None,
        help="Path to YAML file with additional known issue patterns",
    )
    parser.add_argument(
        "--cache-dir",
        default="~/.ocs-ci/analysis_cache",
        help="Directory for caching analysis results",
    )
    parser.add_argument(
        "--max-budget-usd",
        type=float,
        default=0.50,
        help="Max spend per AI analysis call in USD (default: 0.50)",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=30,
        help="Max unique failure signatures to analyze with AI (default: 30)",
    )
    parser.add_argument(
        "--test",
        nargs="+",
        default=None,
        help=(
            "Only analyze failures matching these test name substrings "
            "(e.g., --test noobaa pvc_clone)"
        ),
    )
    parser.add_argument(
        "--squad",
        default=None,
        help=(
            "Only analyze failures from a specific squad "
            "(e.g., black_squad, green_squad, brown_squad)"
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit total number of failures to process (for debugging)",
    )
    parser.add_argument(
        "--save-prompts",
        action="store_true",
        help="Save AI prompts to ~/.ocs-ci/prompts/<run_id>/ for debugging",
    )
    parser.add_argument(
        "--record-history",
        action="store_true",
        help="Save this run's results to the history store for cross-run analysis",
    )
    parser.add_argument(
        "--history-dir",
        default="~/.ocs-ci/analysis_history",
        help="Directory for run history files (default: ~/.ocs-ci/analysis_history)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    args = parser.parse_args(argv)

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Suppress urllib3 InsecureRequestWarning for self-signed certs
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if args.known_issues_only:
        args.ai_backend = "none"

    if args.jira_config:
        import configparser

        from ocs_ci.framework import config as ocsci_config

        cp = configparser.ConfigParser()
        cp.read(args.jira_config)
        ocsci_config.AUTH["jira"] = {
            "url": cp["DEFAULT"]["url"],
            "token": cp["DEFAULT"]["token"],
        }

    try:
        # Run analysis
        run_analysis = analyze_run(
            source=args.source,
            ai_backend=args.ai_backend,
            known_issues_only=args.known_issues_only,
            model=args.model,
            max_budget_usd=args.max_budget_usd,
            max_failures=args.max_failures,
            cache_dir=args.cache_dir,
            known_issues_file=args.known_issues_file,
            no_jira=args.no_jira,
            record_history=args.record_history,
            history_dir=args.history_dir,
            limit=args.limit,
            squad=args.squad,
            test_filter=args.test,
            save_prompts=args.save_prompts,
        )

        # Generate report
        builder = ReportBuilder()
        report = builder.build(run_analysis, fmt=args.output_format)

        # Output
        if args.output:
            with open(args.output, "w") as f:
                f.write(report)
            logger.info(f"Report written to {args.output}")
        else:
            print(report)

        # Exit with non-zero if there are unclassified failures
        unclassified = sum(
            1 for fa in run_analysis.failure_analyses if fa.category.value == "unknown"
        )
        if unclassified > 0:
            logger.info(
                f"{unclassified} unclassified failures. "
                "Run with AI backend for classification."
            )

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=args.verbose)
        sys.exit(1)


def trends_main(argv=None):
    """Entry point for the analyze-trends command."""
    parser = argparse.ArgumentParser(
        prog="analyze-trends",
        description="Cross-run trend analysis for OCS-CI test history",
    )

    parser.add_argument(
        "--history-dir",
        default="~/.ocs-ci/analysis_history",
        help="Directory for run history files (default: ~/.ocs-ci/analysis_history)",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=100,
        help="Maximum number of runs to analyze (default: 100)",
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="Filter by platform (e.g., baremetal, aws)",
    )
    parser.add_argument(
        "--ocs-version",
        default=None,
        help="Filter by OCS version (e.g., 4.21)",
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=["json", "markdown", "html"],
        default="markdown",
        dest="output_format",
        help="Output format (default: markdown)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    args = parser.parse_args(argv)

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        from ocs_ci.utility.log_analysis.analysis.history_store import RunHistoryStore
        from ocs_ci.utility.log_analysis.analysis.pattern_detector import (
            PatternDetector,
        )

        store = RunHistoryStore(history_dir=args.history_dir, max_runs=args.max_runs)
        history = store.get_history(
            max_runs=args.max_runs,
            platform=args.platform,
            ocs_version=args.ocs_version,
        )

        if not history:
            logger.error(
                "No run history found. Use 'analyze-logs --record-history' "
                "to record runs first."
            )
            sys.exit(1)

        logger.info(f"Analyzing {len(history)} historical runs...")

        detector = PatternDetector(history)
        trend_report = detector.build_trend_report()

        builder = ReportBuilder()
        report = builder.build_trends_report(trend_report, fmt=args.output_format)

        if args.output:
            with open(args.output, "w") as f:
                f.write(report)
            logger.info(f"Trend report written to {args.output}")
        else:
            print(report)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Trend analysis failed: {e}", exc_info=args.verbose)
        sys.exit(1)


if __name__ == "__main__":
    main()
