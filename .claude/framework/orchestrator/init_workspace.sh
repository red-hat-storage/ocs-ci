#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export JIRA_AGENT_WORKSPACE="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"

mkdir -p "$JIRA_AGENT_WORKSPACE"/{artifacts,outcomes,reports,discovery}
python3 "$ROOT/.claude/memory/init_state.py"
echo "Workspace: $JIRA_AGENT_WORKSPACE"
