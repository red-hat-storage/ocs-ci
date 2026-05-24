#!/usr/bin/env bash
# Bootstrap an orchestrator workflow (prepares workspace + coordinator prompt).
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
DEFAULT_WORKFLOW="zstream-issue-verification"

DRY_RUN=0
WORKFLOW="$DEFAULT_WORKFLOW"
LIST=0
RUN_DISCOVER=0
RUN_EXECUTE=0

usage() {
  cat <<EOF
usage: run.sh [options] <odf-version>

Bootstrap a registered verification workflow.

By default this ONLY prepares the workspace (like make init). Use --discover and/or
--execute to run more of the pipeline from the terminal without Claude Code coordinator.

options:
  --workflow <id>   Workflow from registry (default: $DEFAULT_WORKFLOW)
  --dry-run         Full workload; skip JIRA/GitHub writes
  --discover        Run JIRA discovery (writes discovery/issues.json)
  --execute         After bootstrap/discover, run execute_issue.sh for each discovered key
  --list-workflows  List available workflow ids and exit
  --status          Show active workspace workflow and exit
  -h, --help        This help

examples:
  run.sh <odf-version>
  run.sh --workflow zstream-issue-verification <odf-version>
  run.sh --workflow zstream-issue-verification <odf-version> --dry-run
  run.sh --discover --execute --dry-run 4.19   # bootstrap + discovery + all issues
  run.sh --list-workflows
  run.sh --status

Options may appear before or after the ODF version.

after bootstrap, check:
  .claude/framework/orchestrator/status.sh
  cat \$JIRA_AGENT_WORKSPACE/active-run.json
EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --discover) RUN_DISCOVER=1; shift ;;
    --execute) RUN_EXECUTE=1; shift ;;
    --workflow)
      WORKFLOW="${2:?--workflow requires an id}"
      shift 2
      ;;
    --list-workflows) LIST=1; shift ;;
    --status)
      exec "$DIR/status.sh"
      ;;
    -h|--help) usage; exit 0 ;;
    --)
      shift
      POSITIONAL+=("$@")
      break
      ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 1 ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [[ "$LIST" -eq 1 ]]; then
  exec python3 "$DIR/list_workflows.py"
fi

if [[ ${#POSITIONAL[@]} -gt 1 ]]; then
  echo "error: expected one ODF version, got: ${POSITIONAL[*]}" >&2
  usage >&2
  exit 1
fi

ODF_VERSION="${POSITIONAL[0]:-}"
if [[ -z "$ODF_VERSION" ]]; then
  usage >&2
  exit 1
fi
if [[ "$ODF_VERSION" == --* ]]; then
  echo "error: missing ODF version (positional <odf-version>, e.g. 4.18 or 4.19)" >&2
  usage >&2
  exit 1
fi

"$DIR/init_workspace.sh"
export JIRA_AGENT_WORKSPACE="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"

# Validate workflow exists and write active-run.json
RUN_META="$(python3 "$DIR/set_run_config.py" \
  --workspace "$JIRA_AGENT_WORKSPACE" \
  --odf-version "$ODF_VERSION" \
  --workflow "$WORKFLOW" \
  $([[ "$DRY_RUN" -eq 1 ]] && echo --dry-run))"

WORKFLOW_NAME="$(echo "$RUN_META" | python3 -c "import json,sys; print(json.load(sys.stdin)['workflow_name'])")"
WORKFLOW_ID="$(echo "$RUN_META" | python3 -c "import json,sys; print(json.load(sys.stdin)['workflow_id'])")"
RUN_ID="$(echo "$RUN_META" | python3 -c "import json,sys; print(json.load(sys.stdin)['run_id'])")"
PROMPT="$(echo "$RUN_META" | python3 -c "import json,sys; print(json.load(sys.stdin)['prompt_path'])")"
COORDINATOR="$(echo "$RUN_META" | python3 -c "import json,sys; print(json.load(sys.stdin)['coordinator_agent'])")"

"$ROOT/.claude/framework/lib/init_run_log.sh"
"$ROOT/.claude/framework/lib/log_run.sh" INFO "bootstrap: workflow=$WORKFLOW_ID odf=$ODF_VERSION dry_run=$DRY_RUN run_id=$RUN_ID"

"$DIR/preflight_mcp.sh"

python3 "$DIR/render_prompt.py" --workflow "$WORKFLOW_ID" \
  --odf-version "$ODF_VERSION" \
  $([[ "$DRY_RUN" -eq 1 ]] && echo --dry-run) \
  --out "$PROMPT"

DISC_FILE="$JIRA_AGENT_WORKSPACE/discovery/issues.json"
if [[ "$RUN_DISCOVER" -eq 1 ]]; then
  "$ROOT/.claude/jira-repro/discovery/run.sh"
  python3 "$ROOT/.claude/framework/lib/run_status.py" show
elif [[ -f "$DISC_FILE" ]]; then
  DISC_COUNT="$(python3 -c "import json; print(len(json.load(open('$DISC_FILE')).get('issue_keys',[])))" 2>/dev/null || echo 0)"
  "$ROOT/.claude/framework/lib/log_run.sh" INFO \
    "discovery: existing issues.json has $DISC_COUNT key(s) (re-run: jira-repro/discovery/run.sh or run.sh --discover)"
else
  "$ROOT/.claude/framework/lib/log_run.sh" WARN \
    "discovery: not run yet — run: .claude/jira-repro/discovery/run.sh"
fi

if [[ "$RUN_EXECUTE" -eq 0 ]]; then
  "$ROOT/.claude/framework/lib/log_run.sh" WARN \
    "execution paused: add --execute to run issues, or execute_issue.sh per key, or use coordinator in Claude Code"
fi

# --- optional: run per-issue pipeline for all discovered keys ---
if [[ "$RUN_EXECUTE" -eq 1 ]]; then
  if [[ ! -f "$DISC_FILE" ]]; then
    echo "error: --execute requires discovery/issues.json — use --discover or run jira-repro/discovery/run.sh first" >&2
    exit 1
  fi
  ISSUE_KEYS="$(
    python3 -c "import json; print(' '.join(json.load(open('$DISC_FILE')).get('issue_keys',[])))"
  )"
  if [[ -z "$ISSUE_KEYS" ]]; then
    "$ROOT/.claude/framework/lib/log_run.sh" WARN "execute: discovery returned 0 issues — nothing to execute"
  else
    # shellcheck disable=SC2086
    KEY_COUNT=$(echo "$ISSUE_KEYS" | wc -w | tr -d ' ')
    "$ROOT/.claude/framework/lib/log_run.sh" INFO \
      "execute: running pipeline for $KEY_COUNT issue(s): $ISSUE_KEYS"
    eval "$(python3 "$ROOT/.claude/framework/lib/load_run_context.py" --shell 2>/dev/null)" || true
    # shellcheck disable=SC2086
    for KEY in $ISSUE_KEYS; do
      "$DIR/execute_issue.sh" "$KEY" || {
        "$ROOT/.claude/framework/lib/log_run.sh" ERROR "execute: $KEY failed (continuing)"
      }
    done
    "$ROOT/.claude/framework/lib/log_run.sh" INFO "execute: finished $KEY_COUNT issue(s)"
  fi
fi

echo ""
echo "================================================================================"
if [[ "$RUN_EXECUTE" -eq 1 ]]; then
  echo " ORCHESTRATOR RUN COMPLETE (bootstrap + execute_issue for discovered keys)"
else
  echo " ORCHESTRATOR BOOTSTRAP COMPLETE (execution not started — use --execute)"
fi
echo "================================================================================"
echo " Workflow:     $WORKFLOW_ID"
echo " Name:         $WORKFLOW_NAME"
echo " Run ID:       $RUN_ID"
echo " ODF version:  $ODF_VERSION"
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo " Mode:         DRY-RUN (no JIRA/GitHub writes)"
else
  echo " Mode:         LIVE"
fi
echo " Coordinator:  $COORDINATOR  (.claude/agents/${COORDINATOR}.md)"
echo " Prompt file:  $PROMPT"
echo " Workspace:    $JIRA_AGENT_WORKSPACE"
echo "================================================================================"
echo ""
echo "How to confirm which workflow is active later:"
echo "  .claude/framework/orchestrator/status.sh"
echo "  cat \$JIRA_AGENT_WORKSPACE/active-run.json"
echo ""
echo "Load run context in agent shells:"
echo "  eval \"\$(.claude/framework/lib/load_run_context.sh)\""
echo ""
echo "Live logs (run in another terminal while agents execute):"
echo "  .claude/framework/orchestrator/watch.sh"
echo "  .claude/framework/orchestrator/watch.sh --all"
echo "  Log file: \$JIRA_AGENT_WORKSPACE/logs/run.log"
echo ""
echo "Next step (required — nothing runs automatically after this):"
echo "  1) Per issue (terminal): .claude/framework/orchestrator/execute_issue.sh DFBUGS-XXXX"
echo "  2) Full workflow: Claude Code agent '${COORDINATOR}' + prompt file above"
echo "     Or in Cursor: execute_issue.sh per key, or ask agent to follow the prompt."
echo ""
echo "Full terminal workflow (one command):"
echo "  run.sh --discover --execute --dry-run $ODF_VERSION"
echo ""
echo "Then: open artifacts/DFBUGS-XXXX/verification-generation-prompt.md in Claude to generate tests"
