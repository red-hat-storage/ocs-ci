#!/usr/bin/env bash
# Bootstrap a z-stream verification run (human runs Claude Code with generated prompt).
set -euo pipefail

ODF_VERSION="${1:-}"
if [[ -z "$ODF_VERSION" ]]; then
  echo "usage: run.sh <odf-version>   e.g. 4.19" >&2
  exit 1
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"

"$DIR/init_workspace.sh"
export JIRA_AGENT_WORKSPACE="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
"$DIR/preflight_mcp.sh"

PROMPT="$JIRA_AGENT_WORKSPACE/workflow-zstream-prompt.md"
python3 "$DIR/render_prompt.py" --workflow zstream-issue-verification \
  --odf-version "$ODF_VERSION" --out "$PROMPT"

echo ""
echo "Next: In Claude Code, open orchestrator-coordinator with:"
echo "  $PROMPT"
