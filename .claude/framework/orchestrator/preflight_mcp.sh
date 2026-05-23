#!/usr/bin/env bash
# Verify MCP prerequisites before workflow execution.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"
EXAMPLE="$ROOT/.claude/configs/mcp/claude-code-mcp.example.json"

echo "preflight_mcp: required MCP servers (Claude Code must have these enabled):"
echo "  - redhat-jira  (atlassian-jira-mcp / JIRA_MCP_*)"
echo "  - github       (GitHub MCP for automation backlog)"
echo "preflight_mcp: config example → $EXAMPLE"
echo "preflight_mcp: KUBECONFIG=${KUBECONFIG:-<unset>}"

# Run setup (validates token/email, writes mcp-env.sh)
"$DIR/setup_mcp.sh"

if [[ ! -f "$WS/mcp-ready.json" ]]; then
  echo "preflight_mcp: ERROR — mcp-ready.json missing after setup_mcp" >&2
  exit 1
fi

if [[ -f "$WS/mcp-env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$WS/mcp-env.sh"
  echo "preflight_mcp: JIRA_URL=$JIRA_URL (REST fallback for discover.sh)"
fi

command -v uvx >/dev/null 2>&1 || echo "preflight_mcp: warning — uvx not in PATH (needed to run redhat-jira MCP)" >&2
command -v oc >/dev/null 2>&1 || echo "preflight_mcp: warning — oc not in PATH" >&2
command -v python3 >/dev/null 2>&1 || { echo "preflight_mcp: python3 required" >&2; exit 1; }

echo "preflight_mcp: OK — MCP env ready; ensure redhat-jira + github are enabled in Claude Code UI"
