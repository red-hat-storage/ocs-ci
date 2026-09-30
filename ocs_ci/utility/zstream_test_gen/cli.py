"""
CLI entry point for the z-stream test generator.

Usage::

    # Generate tests for all bugs in a z-stream release
    python -m ocs_ci.utility.zstream_test_gen --fix-version odf-4.22.1

    # Process a single bug
    python -m ocs_ci.utility.zstream_test_gen --bug DFBUGS-10065

    # Dry run (no PRs created)
    python -m ocs_ci.utility.zstream_test_gen --fix-version odf-4.22.1 --dry-run

    # Generate sample config file
    python -m ocs_ci.utility.zstream_test_gen --init-config

"""

import argparse
import logging
import sys


def setup_logging(verbose: bool = False):
    """
    Configure logging for the CLI.

    Args:
        verbose: If True, set log level to DEBUG.

    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="AI-driven test generation for ODF z-stream bug fix verification.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --fix-version odf-4.22.1              Process all bugs in the z-stream
  %(prog)s --bug DFBUGS-10065                     Process a single bug
  %(prog)s --fix-version odf-4.22.1 --dry-run    Dry run (generate but don't create PRs)
  %(prog)s --fix-version odf-4.22.1 --confidence high   Only submit PRs for high-confidence tests
  %(prog)s --init-config                          Create sample config file
        """,
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--fix-version",
        help="Z-stream fix version to process (e.g., odf-4.22.1)",
    )
    group.add_argument(
        "--bug",
        help="Single Jira bug ID to process (e.g., DFBUGS-10065)",
    )
    group.add_argument(
        "--init-config",
        action="store_true",
        help="Create a sample configuration file at ~/.zstream_test_gen.yaml",
    )
    group.add_argument(
        "--learn-only",
        action="store_true",
        help="Only collect feedback from previous PRs (no test generation)",
    )
    group.add_argument(
        "--fix-reviews",
        action="store_true",
        help="Fix CodeRabbit review comments on open PRs and learn from them",
    )
    group.add_argument(
        "--maintain",
        action="store_true",
        help="Run maintenance only: fix reviews + collect feedback, no test generation",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate tests but don't create PRs",
    )
    parser.add_argument(
        "--no-backport",
        action="store_true",
        help="Skip creating backport PRs to release branches",
    )
    parser.add_argument(
        "--no-pr",
        action="store_true",
        help="Don't create any PRs (just generate and save locally)",
    )
    parser.add_argument(
        "--confidence",
        choices=["high", "medium", "low"],
        help="Only publish PRs for tests at or above this confidence level",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to save generated tests (default: .zstream_gen_output/)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (debug) logging",
    )

    args = parser.parse_args()

    if not any(
        [
            args.fix_version,
            args.bug,
            args.init_config,
            args.learn_only,
            args.fix_reviews,
            args.maintain,
        ]
    ):
        parser.print_help()
        sys.exit(1)

    setup_logging(args.verbose)
    log = logging.getLogger(__name__)

    # Handle --init-config (no heavy imports needed)
    if args.init_config:
        from ocs_ci.utility.zstream_test_gen.config import create_sample_config

        config_path = create_sample_config()
        print(f"Sample configuration created at: {config_path}")
        print("Edit this file with your Jira, GitHub, and Claude credentials.")
        sys.exit(0)

    # Heavy imports only when actually running the pipeline
    from ocs_ci.utility.zstream_test_gen.config import load_config
    from ocs_ci.utility.zstream_test_gen.pipeline import ZStreamTestPipeline

    # Load configuration
    cfg = load_config()

    # Handle --learn-only
    if args.learn_only:
        pipeline = ZStreamTestPipeline(cfg)
        summary = pipeline.run_feedback_only()
        print("\nFeedback collection complete:")
        print(f"  PRs processed:      {summary['processed']}")
        print(f"  With corrections:   {summary['corrected']}")
        print(f"  Merged clean:       {summary['merged_clean']}")
        print(f"  Rejected:           {summary['rejected']}")
        print(f"  New rules learned:  {summary['new_rules']}")
        sys.exit(0)

    # Handle --fix-reviews
    if args.fix_reviews:
        pipeline = ZStreamTestPipeline(cfg)
        summary = pipeline.run_fix_reviews()
        print("\nReview fix complete:")
        print(f"  PRs checked:        {summary['prs_checked']}")
        print(f"  PRs fixed:          {summary['prs_fixed']}")
        print(f"  Commits pushed:     {summary['commits_pushed']}")
        print(f"  New rules learned:  {summary['new_rules']}")
        sys.exit(0)

    # Handle --maintain (fix reviews + collect feedback, no generation)
    if args.maintain:
        pipeline = ZStreamTestPipeline(cfg)
        print("\n--- Fix reviews ---")
        review_summary = pipeline.run_fix_reviews()
        print(f"  PRs checked:        {review_summary['prs_checked']}")
        print(f"  PRs fixed:          {review_summary['prs_fixed']}")
        print(f"  Commits pushed:     {review_summary['commits_pushed']}")
        print(f"  New rules learned:  {review_summary['new_rules']}")

        print("\n--- Collect feedback ---")
        feedback_summary = pipeline.run_feedback_only()
        print(f"  PRs processed:      {feedback_summary['processed']}")
        print(f"  With corrections:   {feedback_summary['corrected']}")
        print(f"  Merged clean:       {feedback_summary['merged_clean']}")
        print(f"  Rejected:           {feedback_summary['rejected']}")
        print(f"  New rules learned:  {feedback_summary['new_rules']}")
        sys.exit(0)

    # Apply CLI overrides
    if args.dry_run:
        cfg.dry_run = True
        cfg.create_prs = False
    if args.no_pr:
        cfg.create_prs = False
    if args.no_backport:
        cfg.backport = False
    if args.output_dir:
        cfg.output_dir = args.output_dir
    if args.confidence:
        cfg.min_confidence = args.confidence

    # Validate required credentials
    if not cfg.jira.api_token:
        log.error(
            "Jira API token not configured. Set JIRA_API_TOKEN env var "
            "or add it to ~/.zstream_test_gen.yaml"
        )
        sys.exit(1)

    if not cfg.claude.api_key and not cfg.claude.project_id:
        log.error(
            "Claude credentials not configured. Set ANTHROPIC_API_KEY or "
            "VERTEX_PROJECT_ID env var, or add to ~/.zstream_test_gen.yaml"
        )
        sys.exit(1)

    # Run the pipeline
    pipeline = ZStreamTestPipeline(cfg)

    if args.fix_version:
        report = pipeline.run(args.fix_version)
        print("\n" + report.to_text())
    elif args.bug:
        test = pipeline.process_single_bug(args.bug)
        if test:
            print(f"\nGenerated test for {args.bug}:")
            print(f"  File: {test.file_path}")
            print(f"  Validation: {'PASSED' if test.validation_passed else 'FAILED'}")
            if test.validation_errors:
                for error in test.validation_errors:
                    print(f"    - {error}")
            print(f"\nGenerated code saved to: {cfg.output_dir}")
        else:
            print(f"\nBug {args.bug} was not automatable or generation failed.")
            sys.exit(1)


if __name__ == "__main__":
    main()
