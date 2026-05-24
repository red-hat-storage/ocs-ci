#!/usr/bin/env bash
# Prepare verification data for one DFBUGS issue.
# Does NOT run the full pipeline — use the jira-verify-worker agent for that.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
KEY="${1:-}"

usage() {
  echo "usage: execute_issue.sh DFBUGS-XXXX" >&2
  echo "Requires: run.sh bootstrap first; source workspace/mcp-env.sh for JIRA." >&2
  exit 1
}

[[ -n "$KEY" ]] || usage
KEY="$(echo "$KEY" | tr '[:lower:]' '[:upper:]')"

log() { "$ROOT/.claude/framework/lib/log_run.sh" "$@"; }

# --- workspace validation ---
if [[ ! -f "$WS/active-run.json" ]]; then
  echo "execute_issue: run .claude/framework/orchestrator/run.sh <odf-version> first" >&2
  exit 1
fi

# --- environment ---
export JIRA_AGENT_WORKSPACE="$WS"
export CLAUDE_PROJECT_DIR="$ROOT"
eval "$(python3 "$ROOT/.claude/framework/lib/load_run_context.py" --shell 2>/dev/null)" || true
[[ -f "$WS/mcp-env.sh" ]] && source "$WS/mcp-env.sh"

# --- artifact directories ---
ART="$WS/artifacts/$KEY"
mkdir -p "$ART/logs" "$ART/evidence" "$ART/planned-actions" "$ART/cluster-health"

log INFO "execute_issue: data-prep starting $KEY"

# --- jira fetch + analysis ---
python3 "$ROOT/.claude/jira-repro/fetch_issue.py" "$KEY" --out "$ART/jira-raw.json"
python3 "$ROOT/.claude/jira-repro/enrich_analysis.py" "$KEY" --art "$ART"

if grep -q '"skipped_by_label": true' "$ART/analysis.json" 2>/dev/null; then
  log WARN "execute_issue: $KEY skipped (skip-ocsci-agent label)"
  exit 0
fi

# --- repro context ---
python3 "$ROOT/.claude/jira-repro/build_repro_context.py" \
  --issue "$KEY" --art "$ART" --odf-version "${ODF_VERSION:-}"

# --- verification prompt ---
if [[ -f "$ROOT/.claude/jira-repro/render_verification_prompt.py" ]]; then
  python3 "$ROOT/.claude/jira-repro/render_verification_prompt.py" \
    --issue "$KEY" --art "$ART" --odf-version "${ODF_VERSION:-}"
fi

log INFO "execute_issue: data-prep complete $KEY"
echo ""
echo "Data prepared in $ART"
echo "Next: open $ART/verification-generation-prompt.md in Claude Code, or run the jira-verify-worker agent."
