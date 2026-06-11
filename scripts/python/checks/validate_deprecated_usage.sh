#!/usr/bin/env bash
#
# Detect new usage of deprecated functions in added lines.
#
# Modes:
#   precommit  – check staged changes (git diff --cached)
#   ci         – check branch changes vs origin/master
#
# Exit 0 if clean. precommit exits 1 on violations; ci exits 0 with warnings.

set -euo pipefail

MODE="${1:-precommit}"

case "$MODE" in
    precommit)
        DIFF_CMD="git diff --cached --diff-filter=ACM --unified=0"
        ;;
    ci)
        BASE="${2:-${GITHUB_BASE_REF:-master}}"
        # In GitHub Actions (shallow clone), fetch the base branch and create the tracking ref
        if ! git rev-parse --verify "origin/${BASE}" >/dev/null 2>&1; then
            git fetch --depth=1 origin "+refs/heads/${BASE}:refs/remotes/origin/${BASE}"
        fi
        DIFF_CMD="git diff origin/${BASE} HEAD --diff-filter=ACM --unified=0"
        ;;
    *)
        echo "Usage: $0 {precommit|ci [base_ref]}" >&2
        exit 2
        ;;
esac

# Deprecated patterns: "import <name>" or "<name>(" in added lines.
# Each entry: grep_pattern|human_readable_name|replacement
DEPRECATED_FUNCTIONS=(
    "run_cmd|run_cmd|exec_cmd"
    "download_file|download_file|download_with_retries"
    "log_step|log_step|logger.test_step()"
    "system_test|system_test mark|system mark"
)

# Build a single grep pattern for all deprecated names
NAMES=()
for entry in "${DEPRECATED_FUNCTIONS[@]}"; do
    IFS='|' read -r pattern _ _ <<< "$entry"
    NAMES+=("$pattern")
done
COMBINED=$(IFS='|'; echo "${NAMES[*]}")

# Run diff command separately so failures aren't masked by || true
DIFF_OUTPUT=$($DIFF_CMD -- '*.py') || {
    echo "ERROR: diff command failed: $DIFF_CMD" >&2
    exit 2
}

# Extract added Python lines from the diff
VIOLATIONS=$(echo "$DIFF_OUTPUT" | awk '
    /^diff --git/ {
        file = $NF
        sub("^b/", "", file)
    }
    /^@@ / {
        # Parse the +line from hunk header (e.g., @@ -10,3 +20,5 @@)
        # Match " +NNN" (space before +) to avoid greedy match on + in function context
        s = $0
        sub(/.* \+/, "", s)
        sub(/[,@ ].*/, "", s)
        line = s + 0
        next
    }
    /^\+[^+]/ {
        # Added line (skip the +++ header)
        code = substr($0, 2)
        printf "%s:%d: %s\n", file, line, code
        line++
    }
    /^ / { line++ }
' | grep -E "(from\s+\S+\s+import\s+.*\b(${COMBINED})\b|\b(${COMBINED})\s*\(|@\s*(${COMBINED})\b)" | \
    grep -vE "def (${COMBINED})\s*\(" | \
    grep -vE "\.\s*(${COMBINED})\s*\(" | \
    grep -vE "#\s*IgnoreDeprecation" || true)

if [ -z "$VIOLATIONS" ]; then
    exit 0
fi

REPLACEMENT_HELP=""
for entry in "${DEPRECATED_FUNCTIONS[@]}"; do
    IFS='|' read -r pattern name replacement <<< "$entry"
    REPLACEMENT_HELP+="  - ${name}  →  ${replacement}\n"
done

if [ "$MODE" = "ci" ]; then
    while IFS=: read -r file line_num code; do
        echo "::warning file=${file},line=${line_num}::Deprecated usage:${code## }"
    done <<< "$VIOLATIONS"
    echo ""
    echo "⚠ Deprecated function usage detected (warning only in CI)."
    echo -e "Replacements:\n${REPLACEMENT_HELP}"
    exit 0
fi

echo ""
echo "========================================================"
echo " DEPRECATED FUNCTION USAGE DETECTED IN NEW CODE"
echo "========================================================"
echo ""
echo "The following added lines use deprecated functions."
echo "Please use the recommended replacements:"
echo ""
echo -e "$REPLACEMENT_HELP"
echo ""
echo "Violations:"
echo "--------------------------------------------------------"
echo "$VIOLATIONS"
echo "--------------------------------------------------------"
echo ""
echo "Hint: add '# IgnoreDeprecation' to suppress false positives."
echo ""
exit 1
