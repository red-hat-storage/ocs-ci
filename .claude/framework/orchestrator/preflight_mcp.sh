#!/usr/bin/env bash
# Lightweight checks before a workflow run. Extend when MCP servers are configured.
set -euo pipefail

echo "preflight_mcp: ensure JIRA and GitHub MCP servers are enabled in Claude Code"
echo "preflight_mcp: KUBECONFIG=${KUBECONFIG:-<unset>}"

command -v oc >/dev/null 2>&1 || echo "preflight_mcp: warning — oc not in PATH" >&2
command -v python3 >/dev/null 2>&1 || { echo "preflight_mcp: python3 required" >&2; exit 1; }

exit 0
