#!/usr/bin/env bash
# Run the vector DB CLI from any working directory.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python "${SCRIPT_DIR}/vector_db_cli.py" "$@"
