#!/usr/bin/env bash
# Quick checks for run.sh argument parsing (flags after ODF version).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUN="$ROOT/.claude/framework/orchestrator/run.sh"
WS="$ROOT/.claude/workspace-test-$$"
export JIRA_AGENT_WORKSPACE="$WS"
trap 'rm -rf "$WS"' EXIT

dry="$(bash "$RUN" --workflow zstream-issue-verification 4.19 --dry-run 2>&1)"
echo "$dry" | grep -q "DRY-RUN" || { echo "FAIL: --dry-run after version not detected"; exit 1; }
python3 -c "import json; assert json.load(open('$WS/active-run.json'))['dry_run'] is True"

live="$(bash "$RUN" --workflow zstream-issue-verification 4.19 2>&1)"
echo "$live" | grep -q "Mode:         LIVE" || { echo "FAIL: live mode"; exit 1; }

echo "run.sh argument tests OK"
