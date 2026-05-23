#!/usr/bin/env bash
# Run verification for an artifact directory after safety validation.
set -euo pipefail

ARTIFACT_DIR="${1:?artifact dir required}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRIPT="${ARTIFACT_DIR}/verify.sh"

KEY="$(basename "$ARTIFACT_DIR")"
mkdir -p "${ARTIFACT_DIR}/logs"
"$ROOT/.claude/framework/lib/log_run.sh" INFO "verify/run.sh: start $KEY"
"$ROOT/.claude/hooks/safety/validate_script.sh" "$SCRIPT" || exit 2
bash "$SCRIPT" 2>&1 | tee -a "${ARTIFACT_DIR}/logs/verify-run.log"
EXIT="${PIPESTATUS[0]}"
"$ROOT/.claude/framework/lib/log_run.sh" $([[ "$EXIT" -eq 0 ]] && echo INFO || echo ERROR) "verify/run.sh: finished $KEY exit=$EXIT"
exit "$EXIT"
