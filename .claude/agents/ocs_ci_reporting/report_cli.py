#!/usr/bin/env python3
"""
OCS-CI Reporting Agent CLI — render and deliver workflow reports.

Usage (from ocs-ci repo root):

  # Issue verification run record
  python .claude/agents/ocs_ci_reporting/report_cli.py send \\
    --run-id 20260620_091223 \\
    --workflow issue_verification \\
    --dry-run

  # Custom context JSON + template
  python .claude/agents/ocs_ci_reporting/report_cli.py send \\
    --context-file report_context.json \\
    --template plain.md.j2 \\
    --channel file --channel slack
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

for _path in (_AGENT_DIR, _REPO_ROOT, _ISSUE_VERIFICATION_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from operations import build_and_deliver

log = logging.getLogger("ocs_ci_reporting")


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )


def _load_context_from_run(
    run_id: str,
    workflow: str,
    *,
    odf_version: str | None = None,
) -> dict:
    if workflow != "issue_verification":
        raise SystemExit(
            f"Built-in run context only supports issue_verification (got {workflow})"
        )
    from run_record import RunRecord
    from report_context import build_issue_verification_report_context

    run_record = RunRecord.load(run_id)
    return build_issue_verification_report_context(
        run_record._data,
        parameters={"odf_version": run_record._data.get("odf_version")},
    )


def _parse_channels(args: argparse.Namespace) -> list[dict]:
    channels: list[dict] = []
    types = args.channel or ["file"]
    for channel_type in types:
        entry: dict = {"type": channel_type}
        if channel_type == "email" and args.email_to:
            entry["to"] = [a.strip() for a in args.email_to.split(",") if a.strip()]
        channels.append(entry)
    return channels


def cmd_send(args: argparse.Namespace) -> int:
    if args.context_file:
        context = json.loads(Path(args.context_file).read_text(encoding="utf-8"))
    elif args.run_id:
        context = _load_context_from_run(
            args.run_id,
            args.workflow,
            odf_version=args.odf_version,
        )
    else:
        raise SystemExit("Provide --run-id or --context-file")

    output_dir = args.output_dir
    if args.run_id and not output_dir:
        from run_record import RunRecord

        run_record = RunRecord.load(args.run_id)
        output_dir = str(run_record.run_dir)

    result = build_and_deliver(
        context,
        template=args.template,
        channels=_parse_channels(args),
        report_format=args.format,
        subject=args.subject,
        output_dir=output_dir,
        dry_run=not args.no_dry_run,
        auth_path=args.auth_file,
    )

    payload = {
        "subject": result.report.subject,
        "template": result.report.template,
        "channels": [
            {
                "type": c.channel_type,
                "status": c.status,
                "detail": c.detail,
                "artifact_path": c.artifact_path,
            }
            for c in result.channels
        ],
        "succeeded": result.succeeded,
    }
    print(json.dumps(payload, indent=2))
    return 0 if result.succeeded else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OCS-CI reporting agent")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    send = sub.add_parser("send", help="Build and deliver a report")
    send.add_argument("--run-id", default=None, help="Load context from run record")
    send.add_argument(
        "--workflow",
        default="issue_verification",
        help="Workflow name for built-in context builders",
    )
    send.add_argument(
        "--odf-version", default=None, help="ODF version suffix for run dir"
    )
    send.add_argument(
        "--context-file", default=None, help="JSON file with report context"
    )
    send.add_argument(
        "--template",
        default="issue_verification.md.j2",
        help="Template name or path",
    )
    send.add_argument(
        "--format",
        default="markdown",
        choices=["markdown", "html", "text"],
    )
    send.add_argument("--subject", default=None)
    send.add_argument(
        "--channel",
        action="append",
        choices=["file", "slack", "email"],
        help="Delivery channel (repeatable)",
    )
    send.add_argument("--email-to", default=None, help="Comma-separated recipients")
    send.add_argument("--output-dir", default=None)
    send.add_argument("--auth-file", default=None, help="Path to data/auth.yaml")
    send.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually send to Slack/email (file always written)",
    )
    send.set_defaults(func=cmd_send)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
