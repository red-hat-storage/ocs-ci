#!/usr/bin/env bash
# Append a timestamped line to workspace/logs/run.log
# Usage: .claude/framework/lib/log_run.sh INFO "message"
#        .claude/framework/lib/log_run.sh INFO "msg" >> also prints to stderr
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
LEVEL="${1:-INFO}"
shift || true
MSG="${*:-}"

if [[ -z "$MSG" ]]; then
  echo "usage: log_run.sh <LEVEL> <message>" >&2
  exit 1
fi

mkdir -p "$WS/logs"
LOG="$WS/logs/run.log"
TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
LINE="$TS [$LEVEL] $MSG"
echo "$LINE" >>"$LOG"
echo "$LINE" >&2
