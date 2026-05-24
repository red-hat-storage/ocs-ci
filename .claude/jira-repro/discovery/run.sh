#!/usr/bin/env bash
# Discover JIRA issues via MCP; write discovery/issues.json.
# This script is a legacy REST fallback — prefer the jira-discovery agent via Claude Code.
# If JIRA REST credentials are not set, exit with an error instead of returning 0 issues.
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
OUT="$WS/discovery/issues.json"

export JIRA_AGENT_WORKSPACE="$WS"
eval "$(python3 "$ROOT/.claude/framework/lib/load_run_context.py" --shell 2>/dev/null)" || true
[[ -f "$WS/mcp-env.sh" ]] && source "$WS/mcp-env.sh"

if [[ -z "${ODF_VERSION:-}" ]]; then
  echo "discover: load ODF_VERSION failed — run orchestrator/run.sh first" >&2
  exit 1
fi

# --- Require JIRA credentials or fail fast ---
if [[ -z "${JIRA_URL:-}" || -z "${JIRA_EMAIL:-}" || -z "${JIRA_API_TOKEN:-}" ]]; then
  "$ROOT/.claude/framework/lib/log_run.sh" ERROR \
    "jira-discovery: JIRA REST credentials not set. Use Claude Code with JIRA MCP instead."
  echo "" >&2
  echo "ERROR: JIRA REST credentials not configured." >&2
  echo "This workflow requires Claude Code with the redhat-jira MCP server." >&2
  echo "Run inside Claude Code:  /zstream-verify --dry-run $ODF_VERSION" >&2
  exit 1
fi

"$ROOT/.claude/framework/lib/log_run.sh" INFO "jira-discovery: starting version=$ODF_VERSION status=${JIRA_STATUS:-ON_QA} workflow=${WORKFLOW_ID:-}"
python3 "$ROOT/.claude/framework/lib/run_status.py" set --phase discovery --message "JIRA discovery in progress" 2>/dev/null || true

mkdir -p "$WS/discovery"
if ! python3 "$ROOT/.claude/jira-repro/discovery/search_jql.py" \
  --odf-version "$ODF_VERSION" \
  --status "${JIRA_STATUS:-ON_QA}" \
  --project "${JIRA_PROJECT:-DFBUGS}" \
  --out "$OUT"; then
  "$ROOT/.claude/framework/lib/log_run.sh" ERROR "jira-discovery: search_jql.py failed"
  python3 "$ROOT/.claude/framework/lib/run_status.py" set --phase discovery --message "Discovery failed (see stderr)" 2>/dev/null || true
  exit 1
fi

DISC_ERR="$(python3 -c "import json; d=json.load(open('$OUT')); print(d.get('error',''))" 2>/dev/null || true)"
COUNT="$(python3 -c "import json; print(len(json.load(open('$OUT')).get('issue_keys',[])))")"
EXCLUDED="$(python3 -c "import json; print(json.load(open('$OUT')).get('excluded_mismatch_count',0))" 2>/dev/null || echo 0)"
FILTER="$(python3 -c "import json; print(json.load(open('$OUT')).get('target_release_filter',''))" 2>/dev/null || true)"

if [[ -n "$DISC_ERR" && "$COUNT" -eq 0 ]]; then
  "$ROOT/.claude/framework/lib/log_run.sh" WARN "jira-discovery: $DISC_ERR"
  MSG="Discovery: $DISC_ERR"
elif [[ "$COUNT" -eq 0 ]]; then
  JQL_USED="$(python3 -c "import json; print(json.load(open('$OUT')).get('jql_used','') or '')" 2>/dev/null || true)"
  "$ROOT/.claude/framework/lib/log_run.sh" WARN "jira-discovery: 0 issues for version $ODF_VERSION — JQL: $JQL_USED"
  MSG="0 issues — empty JQL result (try: search_jql.py -v --print-jql; edit configs/jira-discovery.yaml)"
else
  "$ROOT/.claude/framework/lib/log_run.sh" INFO "jira-discovery: found $COUNT issue(s) matching Target Release=$FILTER (excluded $EXCLUDED mismatches) — $OUT"
  MSG="Discovery complete: $COUNT issue(s) for Target Release $FILTER"
fi

python3 -c "
import json
from pathlib import Path
import sys
sys.path.insert(0, '$ROOT/.claude/framework/lib')
from run_status import merge
keys = json.load(open('$OUT')).get('issue_keys', [])
merge({
    'phase': 'discovery_done',
    'discovery': {
        'completed': True,
        'issue_count': len(keys),
        'issue_keys': keys,
    },
    'issues_total': len(keys),
    'issues_processed': 0,
    'last_message': '$MSG',
})
"

echo ""
echo "Discovery result: $COUNT issue(s)"
echo "File: $OUT"
if [[ "$COUNT" -gt 0 ]]; then
  python3 -c "import json; print('Keys:', ', '.join(json.load(open('$OUT'))['issue_keys']))"
fi
