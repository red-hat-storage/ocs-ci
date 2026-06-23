#!/usr/bin/env python3
"""
OCS-CI Live Repro Agent CLI — plan or run live issue reproduction on cluster.

Usage (from ocs-ci repo root):

  python .claude/agents/ocs_ci_live_repro/verify_cli.py plan \\
    --run-id 20260622_194551 \\
    --deploy-job-url https://jenkins.../job/qe-deploy-ocs-cluster/69391/

  python .claude/agents/ocs_ci_live_repro/verify_cli.py live \\
    --run-id 20260622_194551 \\
    --deploy-job-url https://jenkins.../69391/ \\
    --issue DFBUGS-784
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_AGENT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _AGENT_DIR.parents[2]
_ZSTREAM_DIR = _AGENT_DIR.parents[1] / "workflow" / "zstream_workflow"

for _path in (_AGENT_DIR, _REPO_ROOT, _ZSTREAM_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from operations import load_issues_from_run_record, verify_issues

log = logging.getLogger("ocs_ci_live_repro")


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )


def cmd_plan(args: argparse.Namespace) -> int:
    issues = load_issues_from_run_record(args.run_id, issue_key=args.issue)
    results = verify_issues(
        issues,
        deploy_job_url=args.deploy_job_url,
        target_zstream=args.odf_version,
        dry_run=not args.live,
        skip_on_env_mismatch=not args.force,
        force=args.force,
        oc_command_path=args.oc_command,
        model=args.model,
        max_turns=args.max_turns,
        backend=args.backend,
    )

    if args.write_run_record:
        from run_record import RunRecord

        from models import STAGE_LIVE_CLUSTER_VERIFICATION

        run_record = RunRecord.load(args.run_id)
        for key, data in results.items():
            status = data.pop("stage_status", "completed")
            run_record.append_stage(
                STAGE_LIVE_CLUSTER_VERIFICATION,
                key,
                data,
                status=status,
            )
        run_record.mark_stage_completed(STAGE_LIVE_CLUSTER_VERIFICATION)
        log.info("Updated run record %s", args.run_id)

    print(json.dumps(results, indent=2))
    return 0


def _add_verify_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--deploy-job-url", required=True)
    parser.add_argument("--issue", default=None, help="Single JIRA key")
    parser.add_argument("--odf-version", default=None, help="Target z-stream version")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Verify even when cluster env mismatches issue",
    )
    parser.add_argument(
        "--write-run-record",
        action="store_true",
        help="Persist results to z-stream run record",
    )
    parser.add_argument(
        "--oc-command",
        default="oc",
        help="Path to oc binary (live mode only)",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Claude model override (live mode only)",
    )
    parser.add_argument(
        "--max-turns",
        type=int,
        default=40,
        help="Max agent turns for live verification (sdk backend only)",
    )
    parser.add_argument(
        "--backend",
        choices=("auto", "claude-cli", "sdk"),
        default="auto",
        help="Live backend: auto prefers Claude Code CLI (claude login), then SDK",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OCS-CI live issue reproduction agent")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="Build verification plan (dry-run)")
    _add_verify_args(plan)
    plan.set_defaults(func=cmd_plan, live=False)

    live = sub.add_parser("live", help="Run live cluster verification (Phase B)")
    _add_verify_args(live)
    live.set_defaults(func=cmd_plan, live=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
