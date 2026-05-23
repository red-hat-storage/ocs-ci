#!/usr/bin/env bash
# Run verification for an artifact directory after safety validation.
set -euo pipefail

ARTIFACT_DIR="${1:?artifact dir required}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRIPT="${ARTIFACT_DIR}/verify.sh"

"$ROOT/.claude/hooks/safety/validate_script.sh" "$SCRIPT" || exit 2
bash "$SCRIPT"
