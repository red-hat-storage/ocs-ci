#!/usr/bin/env bash
# Post-verification: collect cluster health artifacts before analysis agent runs.
set -euo pipefail

KEY="${1:-}"
ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"

if [[ -z "$KEY" ]]; then
  echo "cluster_health_collect: usage: cluster_health_collect.sh DFBUGS-XXXX" >&2
  exit 1
fi

ARTIFACT="$WS/artifacts/$KEY"
mkdir -p "$ARTIFACT/cluster-health"
"$ROOT/.claude/jira-repro/cluster-health/collect.sh" "$ARTIFACT"
echo "cluster_health_collect: done for $KEY"
