#!/usr/bin/env bash
# Tail live verification logs from the workspace.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
LOG="$WS/logs/run.log"
MODE="run"

usage() {
  cat <<EOF
usage: watch.sh [--all] [--status]

Tail live logs for the current DFBUGS verification workspace.

  (default)   $LOG
  --all       Also follow artifact logs (pytest, cluster-health, etc.)
  --status    Print active-run + log path, then exit

Set JIRA_AGENT_WORKSPACE if not using default .claude/workspace

Examples:
  .claude/framework/orchestrator/watch.sh
  .claude/framework/orchestrator/watch.sh --all
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all) MODE="all"; shift ;;
    --status) MODE="status"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

echo "Workspace: $WS"

if [[ "$MODE" == "status" ]]; then
  python3 "$ROOT/.claude/framework/lib/run_status.py" show 2>/dev/null || true
  echo ""
  if [[ -f "$LOG" ]]; then
    echo "run.log: $LOG ($(wc -l <"$LOG" | tr -d ' ') lines) — last entries:"
    tail -8 "$LOG"
  else
    echo "run.log: (not created yet)"
  fi
  exit 0
fi

python3 "$ROOT/.claude/framework/lib/run_status.py" show 2>/dev/null || true
echo ""

mkdir -p "$WS/logs"
touch "$LOG"

echo "Following: $LOG"
echo "Press Ctrl+C to stop."
echo ""

if [[ "$MODE" == "all" ]]; then
  FILES=("$LOG")
  while IFS= read -r f; do
    [[ -n "$f" ]] && FILES+=("$f")
  done < <(find "$WS/artifacts" -type f \( -name '*.log' -o -name 'pytest.log' \) 2>/dev/null | sort -u)
  echo "Also following ${#FILES[@]} file(s) (--all)"
  tail -n 20 -F "${FILES[@]}"
else
  tail -n 40 -F "$LOG"
fi
