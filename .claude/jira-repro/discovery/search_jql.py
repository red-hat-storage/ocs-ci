#!/usr/bin/env python3
"""Discover DFBUGS keys in ON_QA for a target ODF z-stream.

Requires JIRA credentials in env (Atlassian Cloud example):
  JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("search_jql: install requests", file=sys.stderr)
    sys.exit(1)


def jql(odf_version: str, status: str, project: str) -> str:
    return (
        f'project = {project} AND status = "{status}" '
        f'AND "ODF Version" ~ "{odf_version}" ORDER BY updated DESC'
    )


def search(jql_str: str) -> list[str]:
    base = os.environ.get("JIRA_URL", "").rstrip("/")
    email = os.environ.get("JIRA_EMAIL", "")
    token = os.environ.get("JIRA_API_TOKEN", "")
    if not all([base, email, token]):
        print(
            "search_jql: set JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN "
            "(or use JIRA MCP from Claude Code instead)",
            file=sys.stderr,
        )
        return []

    url = urljoin(base + "/", "rest/api/3/search/jql")
    resp = requests.post(
        url,
        auth=(email, token),
        json={"jql": jql_str, "maxResults": 100, "fields": ["key"]},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return [i["key"] for i in data.get("issues", [])]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--odf-version",
        default=os.environ.get("ODF_VERSION", ""),
        help="Target ODF z-stream (or set ODF_VERSION from load_run_context.sh)",
    )
    parser.add_argument("--status", default="ON_QA")
    parser.add_argument("--project", default="DFBUGS")
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()

    if not args.odf_version:
        print(
            "search_jql: pass --odf-version or export ODF_VERSION "
            '(eval "$(.claude/framework/lib/load_run_context.sh)")',
            file=sys.stderr,
        )
        sys.exit(2)

    keys = search(jql(args.odf_version, args.status, args.project))
    payload = {
        "odf_version": args.odf_version,
        "status": args.status,
        "issue_keys": keys,
    }
    text = json.dumps(payload, indent=2) + "\n"
    if args.out:
        from pathlib import Path

        Path(args.out).write_text(text)
    print(text)


if __name__ == "__main__":
    main()
