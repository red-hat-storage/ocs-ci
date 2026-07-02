#!/usr/bin/env python3
"""
OCS-CI JIRA Agent CLI.

Usage (from ocs-ci repo root):

  python .claude/agents/ocs_ci_jira/jira_cli.py get --issue DFBUGS-784
  python .claude/agents/ocs_ci_jira/jira_cli.py search --jql 'project = DFBUGS AND key = DFBUGS-784'
  python .claude/agents/ocs_ci_jira/jira_cli.py on-qa --odf-version 4.22
  python .claude/agents/ocs_ci_jira/jira_cli.py comment --issue DFBUGS-784 --text "test" --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_AGENT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _AGENT_DIR.parents[2]
if str(_AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENT_DIR))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from jql import build_on_qa_jql
from operations import add_comment, get_issue, search_and_parse

log = logging.getLogger("ocs_ci_jira")


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )


def cmd_get(args: argparse.Namespace) -> int:
    issue = get_issue(args.issue, jira_config=args.jira_config)
    print(json.dumps(issue, indent=2))
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    issues = search_and_parse(args.jql, jira_config=args.jira_config)
    print(json.dumps(issues, indent=2))
    return 0


def cmd_on_qa(args: argparse.Namespace) -> int:
    jql = build_on_qa_jql(args.odf_version)
    log.info("JQL: %s", jql)
    issues = search_and_parse(jql, jira_config=args.jira_config)
    print(json.dumps({"jql": jql, "issues": issues, "count": len(issues)}, indent=2))
    return 0


def cmd_comment(args: argparse.Namespace) -> int:
    dry_run = args.dry_run and not args.no_dry_run
    result = add_comment(
        args.issue,
        args.text,
        jira_config=args.jira_config,
        dry_run=dry_run,
    )
    print(json.dumps(result, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OCS-CI JIRA agent CLI")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--jira-config", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    get = sub.add_parser("get", help="Fetch one issue by key")
    get.add_argument("--issue", required=True)
    get.set_defaults(func=cmd_get)

    search = sub.add_parser("search", help="Search with JQL")
    search.add_argument("--jql", required=True)
    search.set_defaults(func=cmd_search)

    on_qa = sub.add_parser("on-qa", help="Fetch ON_QA bugs for ODF z-stream")
    on_qa.add_argument("--odf-version", required=True)
    on_qa.set_defaults(func=cmd_on_qa)

    comment = sub.add_parser("comment", help="Add issue comment")
    comment.add_argument("--issue", required=True)
    comment.add_argument("--text", required=True)
    comment.add_argument("--dry-run", action="store_true", default=True)
    comment.add_argument("--no-dry-run", action="store_true")
    comment.set_defaults(func=cmd_comment)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
