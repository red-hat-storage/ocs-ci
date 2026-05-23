#!/usr/bin/env bash
# Exit 0 if dry-run is active, 1 otherwise. Sources DFBUGS_DRY_RUN and workspace marker.
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
WS="${JIRA_AGENT_WORKSPACE:-$ROOT/.claude/workspace}"

if [[ "${DFBUGS_DRY_RUN:-}" =~ ^(1|true|yes|on)$ ]]; then
  exit 0
fi
if [[ -f "$WS/.dry-run" ]]; then
  exit 0
fi
python3 "$ROOT/.claude/framework/lib/dry_run.py" check 2>/dev/null && exit 0 || exit 1
