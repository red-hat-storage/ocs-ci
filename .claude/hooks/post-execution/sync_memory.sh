#!/usr/bin/env bash
# Sync outcome JSON into run-state.json + issue-history after an issue completes.
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

export PYTHONPATH="$ROOT/.claude/framework/lib:${PYTHONPATH:-}"
python3 - "$KEY" "$OUTCOME" "$WS" <<'PY'
import json
import sys
from pathlib import Path

from run_state import mark_issue

key, outcome_path, ws = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
data = json.loads(outcome_path.read_text())

# Snapshot to issue-history
history = ws / "issue-history"
history.mkdir(parents=True, exist_ok=True)
(history / f"{key}.json").write_text(json.dumps(data, indent=2) + "\n")

notes = data.get("notes") or ""
if data.get("dry_run"):
    notes = (notes + " [dry-run]").strip()
mark_issue(
    ws,
    key,
    processed=True,
    status=data.get("result"),
    confidence=data.get("confidence"),
    notes=notes or None,
)
print(f"sync_memory: updated {key}")
PY

"$ROOT/.claude/framework/lib/log_run.sh" INFO "sync_memory: completed $KEY status=$(python3 -c "import json; print(json.load(open('$OUTCOME')).get('result',''))" 2>/dev/null || echo ?)"
