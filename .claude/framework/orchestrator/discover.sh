#!/usr/bin/env bash
# Run JIRA discovery only (useful to test before full coordinator run).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
export JIRA_AGENT_WORKSPACE="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
"$ROOT/.claude/jira-repro/discovery/run.sh"
echo ""
python3 "$ROOT/.claude/framework/lib/run_status.py" show
