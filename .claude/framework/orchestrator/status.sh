#!/usr/bin/env bash
# Show which workflow is configured in the workspace (does not mean Claude is executing it).
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"

echo "Workspace: $WS"
echo ""

if [[ ! -d "$WS" ]]; then
  echo "No workspace yet. Bootstrap with:"
  echo "  .claude/framework/orchestrator/run.sh --workflow <id> <odf-version>"
  exit 0
fi

if [[ -f "$WS/active-run.json" ]]; then
  echo "=== Active run (last bootstrap) ==="
  python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1])), indent=2))" "$WS/active-run.json"
  echo ""
elif [[ -f "$WS/run-config.json" ]]; then
  echo "=== run-config.json ==="
  cat "$WS/run-config.json"
  echo ""
else
  echo "No active run. Bootstrap with run.sh"
  exit 0
fi

DRY_JSON="$(python3 -c "import json; print(json.load(open('$WS/active-run.json')).get('dry_run', False))" 2>/dev/null || echo False)"
if [[ "$DRY_JSON" == "True" ]] || [[ -f "$WS/.dry-run" ]]; then
  echo "Mode: DRY-RUN"
else
  echo "Mode: LIVE"
fi

if [[ -L "$WS/.active-workflow" || -f "$WS/.active-workflow" ]]; then
  echo "Active workflow id: $(cat "$WS/.active-workflow" 2>/dev/null || readlink "$WS/.active-workflow")"
fi

PROMPT=$(python3 -c "import json; print(json.load(open('$WS/active-run.json')).get('prompt_path',''))" 2>/dev/null || true)
if [[ -n "$PROMPT" && -f "$PROMPT" ]]; then
  echo "Coordinator prompt: $PROMPT"
fi

echo ""
echo "Note: run.sh only prepares the workspace and prompt."
echo "      Execution happens when you open the coordinator agent in Claude Code with that prompt."
