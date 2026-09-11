#!/usr/bin/env python3
"""
OCS-CI Test Match Agent CLI — find pytest tests for JIRA issues.

Usage (from ocs-ci repo root):

  # From z-stream run record
  python .claude/agents/ocs_ci_test_match/test_match_cli.py match \\
    --run-id 20260620_091223 --issue DFBUGS-784

  # Direct JIRA key
  python .claude/agents/ocs_ci_test_match/test_match_cli.py match \\
    --jira-key DFBUGS-784

  # Issue JSON file
  python .claude/agents/ocs_ci_test_match/test_match_cli.py match \\
    --issue-file issue.json

  # Claude Agent SDK (semantic search)
  python .claude/agents/ocs_ci_test_match/test_match_cli.py match \\
    --jira-key DFBUGS-784 --use-claude-agent
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_AGENT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _AGENT_DIR.parents[2]
_ISSUE_VERIFICATION_DIR = (
    _AGENT_DIR.parents[1] / "workflow" / "issue_verification_workflow"
)

for _path in (_AGENT_DIR, _REPO_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from models import STAGE_TEST_MATCHING
from operations import (
    load_issue_from_file,
    load_issue_from_jira,
    load_issues_from_run_record,
    match_issue,
    match_issues,
)

if str(_ISSUE_VERIFICATION_DIR) not in sys.path:
    sys.path.insert(0, str(_ISSUE_VERIFICATION_DIR))

from workflow_config import apply_config_to_namespace

log = logging.getLogger("ocs_ci_test_match")


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )


def _resolve_issues(args: argparse.Namespace) -> list[dict]:
    sources = sum(
        bool(flag)
        for flag in (
            args.run_id,
            args.jira_key,
            args.issue_file,
        )
    )
    if sources != 1:
        raise SystemExit(
            "Specify exactly one input: --run-id, --jira-key, or --issue-file"
        )

    if args.run_id:
        return load_issues_from_run_record(args.run_id, issue_key=args.issue)
    if args.jira_key:
        return [load_issue_from_jira(args.jira_key, jira_config=args.jira_config)]
    return [load_issue_from_file(args.issue_file)]


def _maybe_update_run_record(
    args: argparse.Namespace,
    per_issue: dict[str, dict],
) -> None:
    if not args.update_run_record:
        return
    if not args.run_id:
        raise SystemExit("--update-run-record requires --run-id")

    if str(_ISSUE_VERIFICATION_DIR) not in sys.path:
        sys.path.insert(0, str(_ISSUE_VERIFICATION_DIR))

    from run_record import RunRecord

    run_record = RunRecord.load(args.run_id)
    run_record.append_stage_bulk(STAGE_TEST_MATCHING, per_issue)
    log.info("Updated run record %s with test_matching stage", args.run_id)


def cmd_match(args: argparse.Namespace) -> int:
    apply_config_to_namespace(
        args,
        agent="test_match",
        config_path=args.workflow_config,
        mappings={
            "top_tests": "agents.top_n",
            "claude_model": "agents.model",
            "jira_config": "auth.jira_config",
        },
    )
    issues = _resolve_issues(args)

    missing_repro = [
        issue["key"]
        for issue in issues
        if issue.get("key")
        and issue.get("stages", {}).get("repro_steps", {}).get("status") != "completed"
    ]
    if missing_repro:
        log.warning(
            "Issues missing repro_steps stage (matching uses intake data only): %s",
            missing_repro,
        )

    if len(issues) == 1 and not args.run_id:
        result = match_issue(
            issues[0],
            top_n=args.top_tests,
            use_claude=args.use_claude_agent,
            model=args.claude_model,
        )
        print(json.dumps(result, indent=2))
        if args.update_run_record and issues[0].get("key"):
            _maybe_update_run_record(args, {issues[0]["key"]: result})
        return 0

    per_issue = match_issues(
        issues,
        top_n=args.top_tests,
        use_claude=args.use_claude_agent,
        model=args.claude_model,
    )
    ordered = [per_issue[key] for key in sorted(per_issue)]
    print(json.dumps(ordered, indent=2))
    _maybe_update_run_record(args, per_issue)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OCS-CI test match agent — find pytest tests for JIRA issues"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    match = sub.add_parser("match", help="Find matching ocs-ci tests")
    match.add_argument(
        "--workflow-config",
        default=None,
        metavar="PATH",
        help="Shared workflow config (default: config/workflow.yaml)",
    )
    match.add_argument(
        "--run-id",
        default=None,
        help="Z-stream run id from stage 1 (loads issues from run record)",
    )
    match.add_argument(
        "--issue",
        default=None,
        help="Single issue key when using --run-id",
    )
    match.add_argument(
        "--jira-key",
        default=None,
        help="Fetch issue directly from JIRA",
    )
    match.add_argument(
        "--issue-file",
        default=None,
        help="Path to issue JSON (object or single-item list)",
    )
    match.add_argument(
        "--jira-config",
        default=None,
        help="Path to jira.cfg (for --jira-key)",
    )
    match.add_argument(
        "--use-claude-agent",
        action="store_true",
        help="Use claude-agent-sdk semantic search (Read/Glob/Grep)",
    )
    match.add_argument(
        "--claude-model",
        default=None,
        help="Claude model for --use-claude-agent",
    )
    match.add_argument(
        "--top-tests",
        type=int,
        default=10,
        help="Max matching tests per issue (default: 10)",
    )
    match.add_argument(
        "--update-run-record",
        action="store_true",
        help="Write test_matching results to z-stream run record (requires --run-id)",
    )
    match.set_defaults(func=cmd_match)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
