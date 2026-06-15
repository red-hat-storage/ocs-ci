"""
CLI for z-stream ON_QA JIRA bug intake and reproduction step generation.

Usage:
    # Stage 1: JIRA intake
    python .claude/agents/zstream/zstream_issue_verification.py --odf-version 4.22 --list-jira

    # Stage 2: Generate reproduction steps (requires --run-id from stage 1)
    python .claude/agents/zstream/zstream_issue_verification.py \
      --odf-version 4.22 --run-id 20260614_232133 --generate-repro-steps

    # Stage 3: Find matching ocs-ci tests (requires --run-id from stages 1-2)
    python .claude/agents/zstream/zstream_issue_verification.py \
      --odf-version 4.22 --run-id 20260614_232133 --find-matching-tests

    # Stage 3 with Claude Agent SDK (semantic search via Read/Glob/Grep)
    python .claude/agents/zstream/zstream_issue_verification.py \
      --odf-version 4.22 --run-id 20260614_232133 \
      --find-matching-tests --use-claude-agent
"""

import argparse
import json
import logging
import sys
from pathlib import Path

_ZSTREAM_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _ZSTREAM_DIR.parents[2]
for _path in (_ZSTREAM_DIR, _REPO_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from repro_steps_generator import STAGE_REPRO_STEPS, run_repro_steps_stage
from run_record import STAGE_JIRA_INTAKE, STAGE_TEST_MATCHING, RunRecord
from test_matcher import run_test_matching_stage

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Z-stream Lane C agent: JIRA intake, repro steps, and test matching"
    )
    parser.add_argument(
        "--odf-version",
        required=True,
        help="ODF z-stream version (e.g. 4.21.7 or odf-4.21.7)",
    )
    parser.add_argument(
        "--output",
        choices=["keys", "raw", "details", "repro-steps", "matching-tests"],
        default="details",
        help="Output format",
    )
    parser.add_argument(
        "--jira-config",
        default=None,
        help="Path to jira.cfg with url, username, and token/password",
    )
    parser.add_argument(
        "--list-jira",
        action="store_true",
        help="Print JIRA issues as JSON after intake",
    )
    parser.add_argument(
        "--print-jql",
        action="store_true",
        help="Print the JQL query and exit",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Existing run id (required for stages 2 and 3)",
    )
    parser.add_argument(
        "--generate-repro-steps",
        action="store_true",
        help="Stage 2: generate reproduction and verification steps (requires --run-id)",
    )
    parser.add_argument(
        "--find-matching-tests",
        action="store_true",
        help="Stage 3: find ocs-ci tests matching reproduction steps (requires --run-id)",
    )
    parser.add_argument(
        "--use-claude-agent",
        action="store_true",
        help=(
            "Use claude-agent-sdk to match tests (with Read/Glob/Grep). "
            "Requires: pip install claude-agent-sdk and ANTHROPIC_API_KEY"
        ),
    )
    parser.add_argument(
        "--claude-model",
        default=None,
        help="Claude model for --use-claude-agent (e.g. claude-sonnet-4-20250514)",
    )
    parser.add_argument(
        "--top-tests",
        type=int,
        default=10,
        help="Max matching tests per issue for --find-matching-tests (default: 10)",
    )
    parser.add_argument(
        "--no-jira-refresh",
        action="store_true",
        help="Skip JIRA API refresh during repro step generation (use run record data only)",
    )
    parser.add_argument(
        "--stage",
        default=None,
        help="Explicit pipeline stage name (overrides stage flags)",
    )
    return parser.parse_args()


def _setup_logging(run_record: RunRecord | None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )
    if run_record is not None:
        run_record.setup_file_logging()


def _resolve_stage(args: argparse.Namespace) -> str:
    if args.stage:
        return args.stage
    if args.find_matching_tests:
        return STAGE_TEST_MATCHING
    if args.generate_repro_steps:
        return STAGE_REPRO_STEPS
    return STAGE_JIRA_INTAKE


def _run_jira_intake(args: argparse.Namespace, run_record: RunRecord) -> list[dict]:
    """Stage 1: fetch JIRA issues and initialize the shared issues file."""
    from agent_helper import build_on_qa_jql, fetch_on_qa_zstream_bug_details

    jql = build_on_qa_jql(args.odf_version)
    details = fetch_on_qa_zstream_bug_details(
        args.odf_version, jira_config=args.jira_config
    )
    run_record.init_jira_intake(details, jql=jql, odf_version=args.odf_version)
    return details


def _run_repro_steps_stage(
    args: argparse.Namespace, run_record: RunRecord
) -> list[dict]:
    """Stage 2: generate reproduction steps and update the run record."""
    issues = run_record.get_issues()
    if not issues:
        log.error("Run %s has no issues. Run JIRA intake first.", run_record.run_id)
        return []

    target_odf = run_record._data.get("odf_version") or args.odf_version
    per_issue = run_repro_steps_stage(
        issues,
        target_odf,
        jira_config=args.jira_config,
        refresh_jira=not args.no_jira_refresh,
    )
    run_record.append_stage_bulk(STAGE_REPRO_STEPS, per_issue)
    return [per_issue[key] for key in sorted(per_issue)]


def _run_test_matching_stage(
    args: argparse.Namespace,
    run_record: RunRecord,
) -> list[dict]:
    """Stage 3: find matching ocs-ci tests and update the run record."""
    issues = run_record.get_issues()
    if not issues:
        log.error("Run %s has no issues. Run JIRA intake first.", run_record.run_id)
        return []

    missing_repro = [
        i["key"]
        for i in issues
        if i.get("stages", {}).get("repro_steps", {}).get("status") != "completed"
    ]
    if missing_repro:
        log.warning(
            "Issues missing repro_steps stage (matching uses intake data only): %s",
            missing_repro,
        )

    if args.use_claude_agent:
        from claude_test_matcher import run_test_matching_claude_stage

        per_issue = run_test_matching_claude_stage(
            issues,
            top_n=args.top_tests,
            model=args.claude_model,
        )
    else:
        per_issue = run_test_matching_stage(issues, top_n=args.top_tests)

    run_record.append_stage_bulk(STAGE_TEST_MATCHING, per_issue)
    return [per_issue[key] for key in sorted(per_issue)]


def main() -> int:
    args = parse_args()

    if args.print_jql:
        from agent_helper import build_on_qa_jql

        print(build_on_qa_jql(args.odf_version))
        return 0

    stage = _resolve_stage(args)

    if stage in (STAGE_REPRO_STEPS, STAGE_TEST_MATCHING) and not args.run_id:
        log.error("Stage '%s' requires --run-id from stage 1", stage)
        return 1

    if stage == STAGE_JIRA_INTAKE and not args.run_id:
        run_record = RunRecord.create(args.odf_version)
    else:
        run_record = RunRecord.load(args.run_id)

    _setup_logging(run_record)

    summary = run_record.to_summary()
    log.info("Run id: %s", summary["run_id"])
    log.info("Issues file: %s", summary["issues_file"])
    log.info("Stage: %s", stage)

    details: list[dict] = []
    if stage == STAGE_JIRA_INTAKE:
        if args.run_id and run_record.get_issues():
            log.info(
                "Run %s already has %d issues; re-fetching JIRA",
                run_record.run_id,
                len(run_record.get_issues()),
            )
        details = _run_jira_intake(args, run_record)
    elif stage == STAGE_REPRO_STEPS:
        details = _run_repro_steps_stage(args, run_record)
        if not details:
            return 1
    elif stage == STAGE_TEST_MATCHING:
        details = _run_test_matching_stage(args, run_record)
        if not details:
            return 1
    else:
        log.error("Unknown stage: %s", stage)
        return 1

    if args.generate_repro_steps or args.output == "repro-steps":
        print(json.dumps(details, indent=2))
    elif args.find_matching_tests or args.output == "matching-tests":
        print(json.dumps(details, indent=2))
    elif args.list_jira or args.output == "details":
        print(json.dumps(run_record.get_issues(), indent=2))

    if args.output == "keys":
        for issue in run_record.get_issues():
            key = issue.get("key") if isinstance(issue, dict) else None
            if key:
                print(key)

    if args.output == "raw" and stage == STAGE_JIRA_INTAKE:
        from agent_helper import fetch_on_qa_zstream_issues

        raw_issues = fetch_on_qa_zstream_issues(
            args.odf_version, jira_config=args.jira_config
        )
        print(json.dumps(raw_issues, indent=2))

    log.info("Run record summary: %s", json.dumps(run_record.to_summary()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
