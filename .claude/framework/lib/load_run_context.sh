#!/usr/bin/env bash
# Print shell exports for eval in the *current* shell (do not source this file).
# Usage: eval "$(.claude/framework/lib/load_run_context.sh)"
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
python3 "$ROOT/.claude/framework/lib/load_run_context.py" --shell
