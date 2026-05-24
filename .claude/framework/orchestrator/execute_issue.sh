#!/usr/bin/env bash
# Run the verification pipeline for one DFBUGS issue (terminal / Cursor — no Claude Code required).
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

export JIRA_AGENT_WORKSPACE="$WS"
export CLAUDE_PROJECT_DIR="$ROOT"
eval "$(python3 "$ROOT/.claude/framework/lib/load_run_context.py" --shell 2>/dev/null)" || true
[[ -f "$WS/mcp-env.sh" ]] && source "$WS/mcp-env.sh"

if [[ ! -f "$WS/active-run.json" ]]; then
  echo "execute_issue: run .claude/framework/orchestrator/run.sh <odf-version> first" >&2
  exit 1
fi

ART="$WS/artifacts/$KEY"
mkdir -p "$ART/logs" "$ART/evidence" "$ART/planned-actions" "$ART/cluster-health"

log() { "$ROOT/.claude/framework/lib/log_run.sh" "$@"; }

log INFO "coordinator: execute_issue.sh starting $KEY dry_run=${DRY_RUN:-${DFBUGS_DRY_RUN:-0}}"
python3 "$ROOT/.claude/framework/lib/run_status.py" set --phase "issue:$KEY" --message "Pipeline running for $KEY" 2>/dev/null || true

# --- jira-analysis ---
log INFO "phase: jira-analysis start $KEY"
python3 "$ROOT/.claude/jira-repro/fetch_issue.py" "$KEY" --out "$ART/jira-raw.json"
python3 "$ROOT/.claude/jira-repro/enrich_analysis.py" "$KEY" --art "$ART"

if grep -q '"skipped_by_label": true' "$ART/analysis.json" 2>/dev/null; then
  log WARN "jira-analysis: $KEY skipped (skip-ocsci-agent)"
  exit 0
fi
log INFO "phase: jira-analysis done $KEY"

# --- cluster-compat ---
# shellcheck source=/dev/null
source "$ROOT/.claude/framework/lib/run_with_timeout.sh"
log INFO "phase: cluster-compat start $KEY"
CLUSTER_OK=0
CLUSTER_FAIL_REASON=""
if ! command -v oc >/dev/null 2>&1; then
  CLUSTER_FAIL_REASON="oc not in PATH"
elif [[ -z "${KUBECONFIG:-}" ]]; then
  CLUSTER_FAIL_REASON="KUBECONFIG unset"
elif ! run_with_timeout 60 oc whoami >/dev/null 2>&1; then
  CLUSTER_FAIL_REASON="oc whoami failed (check KUBECONFIG=${KUBECONFIG})"
else
  CLUSTER_OK=1
  ODF_VER="$(run_with_timeout 90 oc get csv -n openshift-storage -o json 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for i in d.get('items',[]):
  n=i['metadata']['name']
  if 'odf' in n.lower():
    print(i['spec']['version']); break
" 2>/dev/null || true)"
  if [[ -n "${ODF_VERSION:-}" && -n "${ODF_VER:-}" ]]; then
    CLUSTER_ZS="$(python3 -c "import re; m=re.search(r'(\d+\.\d+)', '${ODF_VER}'); print(m.group(1) if m else '')")"
    TARGET_ZS="$(python3 -c "import re; m=re.search(r'(\d+\.\d+)', '${ODF_VERSION}'); print(m.group(1) if m else '')")"
    if [[ -n "$CLUSTER_ZS" && -n "$TARGET_ZS" && "$CLUSTER_ZS" != "$TARGET_ZS" ]]; then
      log WARN "cluster-compat: ODF on cluster ($ODF_VER) != workflow target ($ODF_VERSION) — verify may not apply"
    fi
  fi
fi

python3 - "$KEY" "$ART" "$CLUSTER_OK" "${ODF_VER:-unknown}" "${ODF_VERSION:-}" "${CLUSTER_FAIL_REASON:-}" <<'PY'
import json, sys
key, art, ok, odf_cluster, odf_target, fail_reason = sys.argv[1:7]
ok = ok == "1"
fit = {
    "issue_key": key,
    "compatible": ok,
    "reason": "cluster reachable" if ok else (fail_reason or "cluster not reachable"),
    "cluster_snapshot": {"odf_csv_version": odf_cluster, "target_odf_version": odf_target},
}
from pathlib import Path
Path(art).mkdir(parents=True, exist_ok=True)
Path(art, "cluster-fit.json").write_text(json.dumps(fit, indent=2) + "\n")
PY

if [[ "$CLUSTER_OK" -eq 0 ]]; then
  log WARN "cluster-compat: no cluster — ${CLUSTER_FAIL_REASON:-unknown} — verification will be skipped"
else
  log INFO "cluster-compat: cluster OK user=$(oc whoami 2>/dev/null || echo '?') odf_csv=${ODF_VER:-?}"
fi
VERIFY_PROCEED=0
BUILD_VERSION_OK=1
if [[ "$CLUSTER_OK" -eq 1 && -n "${ODF_VER:-}" ]]; then
  if python3 "$ROOT/.claude/framework/lib/version_gate.py" \
    --art "$ART" \
    --cluster-version "$ODF_VER" \
    --cluster-reachable 1 2>"$ART/logs/version-gate.stderr"; then
    VERIFY_PROCEED=1
    log INFO "cluster-compat: build version gate passed (cluster >= JIRA product build)"
  else
    BUILD_VERSION_OK=0
    VERIFY_PROCEED=0
    GATE_REASON="$(python3 -c "import json; print(json.load(open('$ART/cluster-fit.json')).get('build_version_check',{}).get('reason','version mismatch'))" 2>/dev/null || echo 'version mismatch')"
    log WARN "cluster-compat: build version gate FAILED — $GATE_REASON"
  fi
elif [[ "$CLUSTER_OK" -eq 1 ]]; then
  VERIFY_PROCEED=1
fi
log INFO "phase: cluster-compat done $KEY compatible=$CLUSTER_OK verify_proceed=$VERIFY_PROCEED"

# --- repro context (facts for AI) ---
log INFO "phase: repro-extraction start $KEY"
python3 "$ROOT/.claude/jira-repro/build_repro_context.py" \
  --issue "$KEY" --art "$ART" --odf-version "${ODF_VERSION:-}"

# --- script-generation (Claude generates repro-steps + tests) ---
log INFO "phase: script-generation start $KEY"
mkdir -p "$ART/logs"
chmod +x "$ROOT/.claude/jira-repro/generate_verification.sh"
DFBUGS_DRY_RUN="${DRY_RUN:-${DFBUGS_DRY_RUN:-0}}" \
  "$ROOT/.claude/jira-repro/generate_verification.sh" "$ART"

if ! python3 "$ROOT/.claude/jira-repro/check_script_generated.py" --art "$ART" 2>/dev/null; then
  log WARN "script-generation: reproduce.py not ready — run Claude on $ART/verification-generation-prompt.md"
  SCRIPT_READY=0
else
  SCRIPT_READY=1
  if [[ -f "$ART/repro-steps.yaml" ]]; then
    python3 "$ROOT/.claude/framework/lib/log_repro_steps.py" --issue "$KEY" --file "$ART/repro-steps.yaml"
  fi
fi
log INFO "phase: script-generation done $KEY ready=$SCRIPT_READY"

# --- safety hook ---
log INFO "phase: safety validate_script $KEY"
"$ROOT/.claude/hooks/safety/validate_script.sh" "$ART/verify.sh"

# --- verification-execution ---
log INFO "phase: verification-execution start $KEY"
PASSED=false
EXEC_NOTE="skipped_no_cluster"
if [[ "${SCRIPT_READY:-0}" -eq 0 ]]; then
  EXEC_NOTE="script_not_generated_by_ai"
  echo "verification skipped: run Claude on $ART/verification-generation-prompt.md" >"$ART/logs/execution-skipped.log"
elif [[ "${VERIFY_PROCEED:-0}" -eq 1 ]]; then
  if "$ROOT/.claude/jira-repro/verify/run.sh" "$ART"; then
    PASSED=true
    EXEC_NOTE="pytest_completed"
  else
    EXEC_NOTE="pytest_failed"
  fi
elif [[ "${BUILD_VERSION_OK:-1}" -eq 0 ]]; then
  EXEC_NOTE="skipped_build_version_mismatch"
  GATE_REASON="$(python3 -c "import json; g=json.load(open('$ART/cluster-fit.json')).get('build_version_check',{}); print(g.get('reason',''))" 2>/dev/null || true)"
  echo "verification skipped: build version mismatch — ${GATE_REASON}" >"$ART/logs/execution-skipped.log"
else
  echo "execution skipped: no cluster" >"$ART/logs/execution-skipped.log"
fi

python3 - "$KEY" "$ART" "$PASSED" "$EXEC_NOTE" <<'PY'
import json, sys
from pathlib import Path
key, art, passed, note = sys.argv[1], Path(sys.argv[2]), sys.argv[3] == "true", sys.argv[4]
(Path(art) / "execution.json").write_text(json.dumps({
    "issue_key": key,
    "passed": passed,
    "duration_sec": 0,
    "failure_signature": "" if passed else note,
    "log_paths": [str(p) for p in (art / "logs").glob("*") if p.is_file()],
    "note": note,
}, indent=2) + "\n")
PY
log INFO "phase: verification-execution done $KEY note=$EXEC_NOTE"

# --- cluster-health (best effort) ---
log INFO "phase: cluster-health start $KEY"
if [[ -x "$ROOT/.claude/jira-repro/cluster-health/collect.sh" ]] && [[ "${VERIFY_PROCEED:-0}" -eq 1 ]]; then
  "$ROOT/.claude/jira-repro/cluster-health/collect.sh" "$ART" 2>/dev/null || true
fi
echo '{"status":"SKIPPED","regression_detected":false,"note":"no cluster or collect skipped"}' \
  >"$ART/cluster-health-report.json"
log INFO "phase: cluster-health done $KEY"

# --- planned JIRA actions (dry-run) ---
DRY=0
[[ -f "$WS/.dry-run" ]] && DRY=1
if [[ "$DRY" -eq 1 ]] || [[ "${DRY_RUN:-0}" == "1" ]]; then
  cat >"$ART/planned-actions/jira.json" <<JSON
{
  "issue_key": "$KEY",
  "dry_run": true,
  "intended_actions": [
    {"action": "comment", "body": "QE dry-run: verification artifacts generated; see workspace artifacts."},
    {"action": "transition", "name": "VERIFIED", "when": "execution passed and cluster health OK"}
  ]
}
JSON
  log INFO "phase: dry-run planned-actions written $KEY"
fi

# --- outcome ---
RESULT="needs_cluster_execution"
if [[ "${SCRIPT_READY:-0}" -eq 0 ]]; then
  RESULT="awaiting_ai_script_generation"
elif [[ "${BUILD_VERSION_OK:-1}" -eq 0 ]]; then
  RESULT="skipped_build_version_mismatch"
elif [[ "$PASSED" == true ]]; then
  RESULT="verified"
fi
python3 - "$KEY" "$WS" "$ART" "$RESULT" "$EXEC_NOTE" <<'PY'
import json, sys
from pathlib import Path
key, ws, art, result, note = sys.argv[1:6]
dry = (Path(ws) / ".dry-run").is_file()
fit_path = Path(art) / "cluster-fit.json"
gate = {}
if fit_path.is_file():
    gate = json.loads(fit_path.read_text()).get("build_version_check") or {}
out = {
    "issue_key": key,
    "dry_run": dry,
    "result": result,
    "execution_note": note,
    "confidence": 0.75,
    "build_version_check": gate,
}
if gate.get("version_mismatch"):
    out["status_note"] = (
        f"Verification not run: cluster ODF {gate.get('cluster_installed_parsed')} "
        f"is lower than JIRA product build {gate.get('jira_required_minimum')}."
    )
Path(ws, "outcomes", f"{key}.json").write_text(json.dumps(out, indent=2) + "\n")
PY

python3 -c "
import sys
sys.path.insert(0, '$ROOT/.claude/memory')
from state import upsert_issue
upsert_issue('$KEY', processed=True, status='$RESULT', confidence=0.75, workflow_id='zstream-issue-verification', odf_version='${ODF_VERSION:-}', run_id='${RUN_ID:-}')
" 2>/dev/null || true

log INFO "coordinator: execute_issue.sh finished $KEY result=$RESULT"
python3 -c "
import json, sys
sys.path.insert(0, '$ROOT/.claude/framework/lib')
from run_status import merge
merge({'phase': 'issue_done', 'issues_processed': 1, 'last_message': 'Finished $KEY ($RESULT)'})
" 2>/dev/null || true

echo ""
echo "Outcome: $WS/outcomes/$KEY.json"
echo "Artifacts: $ART"
