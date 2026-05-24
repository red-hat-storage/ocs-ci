#!/usr/bin/env bash
# Start a fresh run.log for a new run_id (archives the previous log).
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
mkdir -p "$WS/logs"
LOG="$WS/logs/run.log"
MARKER="$WS/logs/.current-run-id"

RUN_ID=""
if [[ -f "$WS/active-run.json" ]]; then
  RUN_ID="$(python3 -c "import json; print(json.load(open('$WS/active-run.json')).get('run_id',''))" 2>/dev/null || true)"
fi

if [[ -f "$LOG" && -f "$MARKER" ]] && [[ "$(cat "$MARKER" 2>/dev/null)" != "$RUN_ID" ]] && [[ -n "$RUN_ID" ]]; then
  ARCH="$WS/logs/archive"
  mkdir -p "$ARCH"
  OLD="$(cat "$MARKER")"
  mv "$LOG" "$ARCH/run-${OLD}.log"
  echo "Archived previous log to $ARCH/run-${OLD}.log" >&2
fi

: >"$LOG"
[[ -n "$RUN_ID" ]] && echo "$RUN_ID" >"$MARKER"

if [[ -f "$WS/active-run.json" ]]; then
  META="$(python3 -c "import json; print(json.dumps(json.load(open('$WS/active-run.json')), indent=2))" 2>/dev/null || echo '{}')"
else
  META="{}"
fi

{
  echo "================================================================================"
  echo "DFBUGS verification run log  run_id=${RUN_ID:-unknown}"
  echo "started_utc: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "workspace: $WS"
  echo "active-run:"
  echo "$META"
  echo "================================================================================"
} >>"$LOG"

"$ROOT/.claude/framework/lib/log_run.sh" INFO "run.log initialized run_id=${RUN_ID:-unknown}"

python3 "$ROOT/.claude/framework/lib/run_status.py" set --phase bootstrapped \
  --message "Bootstrap complete — run jira-repro/discovery/run.sh or start orchestrator-coordinator" 2>/dev/null || true
