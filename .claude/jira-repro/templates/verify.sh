#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARTIFACT_DIR="${1:-$DIR/..}"
cd "$ARTIFACT_DIR"
mkdir -p logs evidence
if [[ -f reproduce.py ]]; then
  pytest reproduce.py -v 2>&1 | tee logs/pytest.log
else
  echo "verify.sh: no reproduce.py in $ARTIFACT_DIR" >&2
  exit 1
fi
