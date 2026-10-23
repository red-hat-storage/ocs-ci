#!/usr/bin/env python3
"""
OCS-CI Run Agent CLI — Jenkins cluster lifecycle for ocs-ci QE.

Usage (from ocs-ci repo root):

  python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py resolve --job-url <build-url>
  python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py trigger-tests \\
    --source-job-url <build-url> --test-path tests/.../test_foo.py --dry-run
  python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py wait --job-url <build-url>
  python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py abort --job-url <build-url>
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_OCS_CI_RUN_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _OCS_CI_RUN_DIR.parents[2]
if str(_OCS_CI_RUN_DIR) not in sys.path:
    sys.path.insert(0, str(_OCS_CI_RUN_DIR))

from config import WAIT_POLL_SEC_DEFAULT, WAIT_TIMEOUT_SEC_DEFAULT
from job_controller import abort_job, wait_for_job
from job_resolver import resolve_job
from job_trigger import trigger_test_run

log = logging.getLogger("ocs_ci_run")


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )


def cmd_resolve(args: argparse.Namespace) -> int:
    profile = resolve_job(
        args.job_url,
        download_kubeconfig=not args.no_kubeconfig,
        work_dir=Path(args.work_dir) if args.work_dir else None,
        prefer_mcp=args.prefer_mcp,
    )
    print(json.dumps(profile.to_dict(), indent=2))
    return 0


def cmd_trigger_tests(args: argparse.Namespace) -> int:
    dry_run = args.dry_run and not args.no_dry_run
    result = trigger_test_run(
        args.source_job_url,
        args.test_path,
        test_name_expression=args.test_name_expression or "",
        run_teardown=args.run_teardown,
        additional_pytest_params=args.additional_pytest_params or "",
        dry_run=dry_run,
    )
    print(json.dumps(result.to_dict(), indent=2))
    return 0


def cmd_wait(args: argparse.Namespace) -> int:
    status = wait_for_job(
        args.job_url,
        timeout_sec=args.timeout,
        poll_sec=args.poll_interval,
        resolve_on_complete=args.resolve,
        prefer_mcp=args.prefer_mcp,
    )
    print(json.dumps(status.to_dict(), indent=2))
    return 0 if not status.building else 1


def cmd_abort(args: argparse.Namespace) -> int:
    result = abort_job(args.job_url, dry_run=args.dry_run and not args.no_dry_run)
    print(json.dumps(result, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OCS-CI Run Agent — Jenkins cluster lifecycle"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--prefer-mcp",
        action="store_true",
        help="Use Jenkins MCP for reads when caller is configured",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_resolve = sub.add_parser("resolve", help="Resolve cluster from Jenkins build")
    p_resolve.add_argument("--job-url", required=True)
    p_resolve.add_argument("--work-dir", default=None)
    p_resolve.add_argument("--no-kubeconfig", action="store_true")
    p_resolve.set_defaults(func=cmd_resolve)

    p_trigger = sub.add_parser("trigger-tests", help="Trigger parameterized test run")
    p_trigger.add_argument("--source-job-url", required=True)
    p_trigger.add_argument(
        "--test-path",
        action="append",
        required=True,
        help="Test file path (repeatable)",
    )
    p_trigger.add_argument("--test-name-expression", default="")
    p_trigger.add_argument("--additional-pytest-params", default="")
    p_trigger.add_argument(
        "--run-teardown",
        action="store_true",
        help="Set RUN_TEARDOWN=True (default False)",
    )
    p_trigger.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Prepare parameters only (default)",
    )
    p_trigger.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually trigger Jenkins",
    )
    p_trigger.set_defaults(func=cmd_trigger_tests)

    p_wait = sub.add_parser("wait", help="Wait for Jenkins build to finish")
    p_wait.add_argument("--job-url", required=True)
    p_wait.add_argument("--timeout", type=int, default=WAIT_TIMEOUT_SEC_DEFAULT)
    p_wait.add_argument("--poll-interval", type=int, default=WAIT_POLL_SEC_DEFAULT)
    p_wait.add_argument("--resolve", action="store_true")
    p_wait.set_defaults(func=cmd_wait)

    p_abort = sub.add_parser("abort", help="Abort a running Jenkins build")
    p_abort.add_argument("--job-url", required=True)
    p_abort.add_argument("--dry-run", action="store_true", default=True)
    p_abort.add_argument("--no-dry-run", action="store_true")
    p_abort.set_defaults(func=cmd_abort)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
