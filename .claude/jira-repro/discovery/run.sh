#!/usr/bin/env bash
# Discover ON_QA DFBUGS issues; write discovery/issues.json and log exact count.
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
OUT="$WS/discovery/issues.json"

export JIRA_AGENT_WORKSPACE="$WS"
eval "$(python3 "$ROOT/.claude/framework/lib/load_run_context.py" --shell 2>/dev/null)" || true
# REST discovery uses same credentials as redhat-jira MCP
[[ -f "$WS/mcp-env.sh" ]] && source "$WS/mcp-env.sh"

if [[ -z "${ODF_VERSION:-}" ]]; then
  echo "discover: load ODF_VERSION failed — run orchestrator/run.sh first" >&2
  exit 1
fi

"$ROOT/.claude/framework/lib/log_run.sh" INFO "jira-discovery: starting ODF=$ODF_VERSION status=${JIRA_STATUS:-ON_QA}"
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
  if [[ -z "${JIRA_URL:-}" || -z "${JIRA_EMAIL:-}" || -z "${JIRA_API_TOKEN:-}" ]]; then
    "$ROOT/.claude/framework/lib/log_run.sh" WARN "jira-discovery: found 0 issues — set JIRA_URL JIRA_EMAIL JIRA_API_TOKEN"
    MSG="0 issues — JIRA API creds missing"
  else
    JQL_USED="$(python3 -c "import json; print(json.load(open('$OUT')).get('jql_used','') or '')" 2>/dev/null || true)"
    "$ROOT/.claude/framework/lib/log_run.sh" WARN "jira-discovery: 0 issues for ODF $ODF_VERSION — JQL: $JQL_USED"
    MSG="0 issues — empty JQL (try: search_jql.py -v --print-jql; edit configs/jira-discovery.yaml)"
  fi
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
