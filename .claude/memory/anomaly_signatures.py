#!/usr/bin/env python3
"""Persist anomaly signatures for cluster-health cross-run learning."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

STORE = Path(__file__).resolve().parent / "anomaly-signatures" / "index.json"


def _load() -> dict:
    if not STORE.is_file():
        return {"signatures": []}
    return json.loads(STORE.read_text())


def _save(data: dict) -> None:
    STORE.parent.mkdir(parents=True, exist_ok=True)
    STORE.write_text(json.dumps(data, indent=2) + "\n")


def _sig_id(signature: str) -> str:
    return hashlib.sha256(signature.encode()).hexdigest()[:16]


def record(signature: str, component: str, severity: str = "Major") -> str:
    data = _load()
    sid = _sig_id(signature)
    entry = {
        "id": sid,
        "signature": signature,
        "component": component,
        "severity": severity,
        "last_seen": datetime.now(timezone.utc).isoformat(),
        "count": 1,
    }
    for existing in data["signatures"]:
        if existing["id"] == sid:
            existing["count"] = int(existing.get("count", 0)) + 1
            existing["last_seen"] = entry["last_seen"]
            _save(data)
            return sid
    data["signatures"].append(entry)
    _save(data)
    return sid


def match(signature: str) -> dict | None:
    sid = _sig_id(signature)
    for existing in _load()["signatures"]:
        if existing["id"] == sid:
            return existing
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    rec = sub.add_parser("record")
    rec.add_argument("--signature", required=True)
    rec.add_argument("--component", required=True)
    rec.add_argument("--severity", default="Major")
    sub.add_parser("list")
    args = parser.parse_args()
    if args.cmd == "record":
        print(record(args.signature, args.component, args.severity))
    elif args.cmd == "list":
        print(json.dumps(_load(), indent=2))


if __name__ == "__main__":
    main()
