#!/usr/bin/env bash
# Sync outcome JSON into SQLite + issue-history after an issue completes.
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
KEY="${1:-}"

if [[ -z "$KEY" ]]; then
  echo "sync_memory: usage: sync_memory.sh DFBUGS-XXXX" >&2
  exit 1
fi

OUTCOME="$WS/outcomes/${KEY}.json"
if [[ ! -f "$OUTCOME" ]]; then
  echo "sync_memory: no outcome file: $OUTCOME" >&2
  exit 0
fi

export PYTHONPATH="$ROOT/.claude/memory:${PYTHONPATH:-}"
python3 - "$KEY" "$OUTCOME" <<'PY'
import json
import sys
from pathlib import Path

from state import snapshot_outcome, upsert_issue

key, path = sys.argv[1], Path(sys.argv[2])
data = json.loads(path.read_text())
snapshot_outcome(key, data)
notes = data.get("notes") or ""
if data.get("dry_run"):
    notes = (notes + " [dry-run]").strip()
upsert_issue(
    key,
    processed=True,
    status=data.get("result"),
    confidence=data.get("confidence"),
    github_issue=data.get("github_issue"),
    notes=notes or None,
)
print(f"sync_memory: updated {key}")
PY
