#!/usr/bin/env bash
# Ensure JIRA_AGENT_WORKSPACE exists and memory DB is initialized.
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"

mkdir -p "$WS"/{artifacts,outcomes,reports,discovery}
python3 "$ROOT/.claude/memory/init_state.py" >/dev/null

if [[ -z "${KUBECONFIG:-}" ]]; then
  echo "check_workspace: warning — KUBECONFIG not set" >&2
fi

if "$ROOT/.claude/framework/lib/is_dry_run.sh"; then
  echo "check_workspace: DRY-RUN active — JIRA/GitHub writes disabled" >&2
fi

echo "check_workspace: ok ($WS)"
