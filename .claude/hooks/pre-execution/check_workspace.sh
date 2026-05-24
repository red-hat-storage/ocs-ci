#!/usr/bin/env bash
# Ensure JIRA_AGENT_WORKSPACE exists with required directories.
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"

mkdir -p "$WS"/{artifacts,outcomes,reports,discovery,logs}

if [[ -z "${KUBECONFIG:-}" ]]; then
  echo "check_workspace: warning — KUBECONFIG not set" >&2
fi

if "$ROOT/.claude/framework/lib/is_dry_run.sh"; then
  echo "check_workspace: DRY-RUN active — JIRA/GitHub writes disabled" >&2
fi

if [[ -f "$WS/active-run.json" ]]; then
  ODF="$(python3 "$ROOT/.claude/framework/lib/load_run_context.py" --field odf_version 2>/dev/null || true)"
  if [[ -n "$ODF" ]]; then
    echo "check_workspace: ODF_VERSION=$ODF (from active-run.json)" >&2
  fi
else
  echo "check_workspace: warning — no active-run.json; run orchestrator/run.sh first" >&2
fi

"$ROOT/.claude/framework/lib/log_run.sh" INFO "check_workspace: ok"
echo "check_workspace: ok ($WS) — tail logs: .claude/framework/orchestrator/watch.sh"
