#!/usr/bin/env python3
"""Fail if reproduce.py is still a placeholder (assert True / TODO only)."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PLACEHOLDER_PATTERNS = (
    r"assert\s+True\b",
    r"#\s*TODO:\s*implement",
    r"Replace with steps from repro-steps",
)


def is_placeholder(path: Path) -> tuple[bool, str]:
    if not path.is_file():
        return True, "reproduce.py missing"
    text = path.read_text()
    if "test_verify" not in text and "def test_" not in text:
        return True, "no test functions found"
    for pat in PLACEHOLDER_PATTERNS:
        if re.search(pat, text, re.IGNORECASE):
            return True, f"placeholder pattern: {pat}"
    return False, "ok"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--art", type=Path, required=True)
    args = parser.parse_args()

    repro = args.art / "reproduce.py"
    bad, reason = is_placeholder(repro)
    if bad:
        print(f"script not generated: {reason}", file=sys.stderr)
        print(
            f"Run Claude with: {args.art / 'verification-generation-prompt.md'}",
            file=sys.stderr,
        )
        sys.exit(2)
    print("ok")


if __name__ == "__main__":
    main()
