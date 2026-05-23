#!/usr/bin/env bash
# Portable timeout wrapper (GNU timeout / gtimeout / direct on macOS).
# Usage: source this file; run_with_timeout <seconds> <command> [args...]
run_with_timeout() {
  local seconds="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$seconds" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$seconds" "$@"
  else
    "$@"
  fi
}
