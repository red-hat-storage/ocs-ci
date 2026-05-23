#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
WS="$ROOT/.claude/workspace-test-ctx-$$"
export JIRA_AGENT_WORKSPACE="$WS"
mkdir -p "$WS"
trap 'rm -rf "$WS"' EXIT

echo '{"odf_version":"4.17","workflow_id":"zstream-issue-verification","dry_run":false}' >"$WS/active-run.json"

eval "$("$ROOT/.claude/framework/lib/load_run_context.sh")"
[[ "$ODF_VERSION" == "4.17" ]] || { echo "FAIL: ODF_VERSION=$ODF_VERSION"; exit 1; }
echo "load_run_context.sh eval OK"
