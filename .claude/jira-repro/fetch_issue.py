#!/usr/bin/env python3
"""Fetch one JIRA issue via REST (same auth/session as discovery)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    sys.exit(1)

from jira_client import DEFAULT_URL, adf_to_text, make_session


def fetch_issue(session: requests.Session, key: str) -> dict:
    base = getattr(session, "_base_url", DEFAULT_URL)
    for api in (f"{base}/rest/api/3/issue/{key}", f"{base}/rest/api/2/issue/{key}"):
        try:
            resp = session.get(
                api, params={"expand": "renderedFields,names"}, timeout=60
            )
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError:
            if api.endswith("/2/"):
                raise
            continue
    raise SystemExit(f"Could not fetch {key}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("issue_key")
    parser.add_argument("--out", type=Path, help="Write full JSON here")
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print JSON to stdout (default only when --out is not set)",
    )
    args = parser.parse_args()

    session = make_session()
    data = fetch_issue(session, args.issue_key.upper())
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(data, indent=2) + "\n")

    if args.summary_only:
        fields = data.get("fields") or {}
        desc = fields.get("description")
        text = adf_to_text(desc) if isinstance(desc, dict) else str(desc or "")
        print(
            json.dumps(
                {
                    "key": data["key"],
                    "summary": fields.get("summary"),
                    "status": (fields.get("status") or {}).get("name"),
                    "labels": fields.get("labels", []),
                    "description_text": text,
                },
                indent=2,
            )
        )
    elif args.print or not args.out:
        print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
