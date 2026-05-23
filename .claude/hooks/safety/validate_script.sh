#!/usr/bin/env bash
# Pre-execution safety: scan verify scripts for forbidden patterns before run.
set -euo pipefail

SCRIPT="${1:-}"
POLICY="${CLAUDE_PROJECT_DIR:-.}/.claude/configs/policies/safety.yaml"

if [[ -z "$SCRIPT" || ! -f "$SCRIPT" ]]; then
  echo "validate_script: no script path" >&2
  exit 0
fi

if [[ ! -f "$POLICY" ]]; then
  exit 0
fi

BLOCKED=0
while IFS= read -r line; do
  [[ "$line" =~ ^[[:space:]]*-[[:space:]]*\'(.+)\'$ ]] || continue
  pat="${BASH_REMATCH[1]}"
  if grep -qiE "$pat" "$SCRIPT" 2>/dev/null; then
    echo "validate_script: forbidden pattern in $SCRIPT: $pat" >&2
    BLOCKED=1
  fi
done < <(grep -E "^\s+-" "$POLICY" | head -20)

if [[ "$BLOCKED" -eq 1 ]]; then
  exit 2
fi
exit 0
