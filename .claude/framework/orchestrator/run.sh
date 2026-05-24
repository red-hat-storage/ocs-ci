#!/usr/bin/env bash
# Bootstrap an orchestrator workflow (prepares workspace + coordinator prompt).
# Requires Claude Code — discovery and execution use JIRA MCP, not REST.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
DEFAULT_WORKFLOW="$(python3 "$DIR/../lib/workflow_registry.py" 2>/dev/null || echo zstream-issue-verification)"

DRY_RUN=0
WORKFLOW="$DEFAULT_WORKFLOW"
LIST=0

usage() {
  cat <<EOF
usage: run.sh [options] <version>

Bootstrap a registered verification workflow for Claude Code.

This prepares the workspace and renders the coordinator prompt. After
bootstrap, use /zstream-verify (or the coordinator agent) in Claude Code
to run discovery and execution via JIRA MCP.

options:
  --workflow <id>   Workflow from registry (default: $DEFAULT_WORKFLOW)
  --dry-run         Full workload; skip JIRA/GitHub writes
  --list-workflows  List available workflow ids and exit
  --status          Show active workspace workflow and exit
  -h, --help        This help

examples:
  run.sh 4.19
  run.sh --dry-run 4.19
  run.sh --workflow zstream-issue-verification --dry-run 4.19
  run.sh --list-workflows
  run.sh --status

after bootstrap, in Claude Code:
  /zstream-verify --dry-run 4.19

or check status:
  .claude/framework/orchestrator/status.sh
  cat \$JIRA_AGENT_WORKSPACE/active-run.json
EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
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
  echo "error: expected one version, got: ${POSITIONAL[*]}" >&2
  usage >&2
  exit 1
fi

ODF_VERSION="${POSITIONAL[0]:-}"
if [[ -z "$ODF_VERSION" ]]; then
  usage >&2
  exit 1
fi
if [[ "$ODF_VERSION" == --* ]]; then
  echo "error: missing version (positional <version>, e.g. 4.18 or 4.19)" >&2
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

echo ""
echo "================================================================================"
echo " BOOTSTRAP COMPLETE — use Claude Code to run the workflow"
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
echo "Next step — run in Claude Code:"
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "  /zstream-verify --dry-run $ODF_VERSION"
else
  echo "  /zstream-verify $ODF_VERSION"
fi
echo ""
echo "Discovery and execution require JIRA MCP (redhat-jira)."
echo "REST fallback is not supported — Claude Code is required."
echo ""
echo "Status / logs:"
echo "  .claude/framework/orchestrator/status.sh"
echo "  .claude/framework/orchestrator/watch.sh"
echo "  cat \$JIRA_AGENT_WORKSPACE/active-run.json"
