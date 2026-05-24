#!/usr/bin/env bash
# Render AI prompt for repro + scripts; optionally invoke Claude CLI to generate files.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ART="${1:?artifact dir}"
KEY="$(basename "$ART")"
ODF_VERSION="${ODF_VERSION:-}"
DRY_RUN="${DFBUGS_DRY_RUN:-0}"

PROMPT="$ART/verification-generation-prompt.md"
python3 "$ROOT/.claude/jira-repro/render_verification_prompt.py" \
  --issue "$KEY" \
  --art "$ART" \
  --odf-version "$ODF_VERSION" \
  --out "$PROMPT" \
  $([[ "$DRY_RUN" == "1" ]] && echo --dry-run)

"$ROOT/.claude/framework/lib/log_run.sh" INFO \
  "script-generation: AI prompt written — $PROMPT"

# Optional: non-interactive Claude Code CLI (user enables explicitly)
if [[ "${DFBUGS_AUTO_GENERATE:-0}" == "1" ]] && command -v claude >/dev/null 2>&1; then
  "$ROOT/.claude/framework/lib/log_run.sh" INFO \
    "script-generation: invoking claude CLI (DFBUGS_AUTO_GENERATE=1)"
  (
    cd "$ROOT"
    export CLAUDE_PROJECT_DIR="$ROOT"
    claude -p "$(cat "$PROMPT")" --allowedTools "Read,Write,Grep,Glob,Bash" 2>&1 | tee "$ART/logs/claude-generate.log"
  ) || {
    "$ROOT/.claude/framework/lib/log_run.sh" WARN \
      "script-generation: claude CLI failed — complete generation manually with the prompt file"
    exit 0
  }
else
  "$ROOT/.claude/framework/lib/log_run.sh" WARN \
    "script-generation: Claude must generate tests — open in Cursor/Claude Code: $PROMPT"
  "$ROOT/.claude/framework/lib/log_run.sh" WARN \
    "script-generation: or export DFBUGS_AUTO_GENERATE=1 with claude CLI in PATH"
fi
