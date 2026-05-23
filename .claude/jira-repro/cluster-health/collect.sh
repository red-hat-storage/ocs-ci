#!/usr/bin/env bash
# Collect post-verification cluster health artifacts (read-only).
set -euo pipefail

ARTIFACT_ROOT="${1:?artifact root required (e.g. workspace/artifacts/DFBUGS-1234)}"
OUT="${ARTIFACT_ROOT}/cluster-health"
mkdir -p "$OUT/stacktraces"

log() { echo "[cluster-health-collect] $*" >&2; }

if ! command -v oc >/dev/null 2>&1; then
  log "oc not in PATH — writing stub artifacts only"
  echo "oc unavailable" >"$OUT/collect-error.txt"
  exit 0
fi

oc get nodes -o wide >"$OUT/nodes.txt" 2>&1 || true
oc get pods -A --field-selector=status.phase!=Running,status.phase!=Succeeded >"$OUT/pods-unhealthy.txt" 2>&1 || true
oc get events -A --sort-by='.lastTimestamp' 2>/dev/null | tail -500 >"$OUT/events.log" || true
oc get pvc -A >"$OUT/pvc.txt" 2>&1 || true
oc get pv >"$OUT/pv.txt" 2>&1 || true

if oc get storagecluster -n openshift-storage >/dev/null 2>&1; then
  oc get storagecluster -n openshift-storage -o yaml >"$OUT/storagecluster.txt" 2>&1 || true
fi

if oc get deployment -n openshift-storage rook-ceph-tools >/dev/null 2>&1; then
  oc exec -n openshift-storage deploy/rook-ceph-tools -- ceph status >"$OUT/ceph-status.txt" 2>&1 || true
  oc exec -n openshift-storage deploy/rook-ceph-tools -- ceph health detail >>"$OUT/ceph-status.txt" 2>&1 || true
fi

# Failing pods with recent logs
while read -r ns pod _; do
  [[ -z "${ns:-}" || "$ns" == "NAMESPACE" ]] && continue
  echo "=== $ns/$pod ===" >>"$OUT/failing-pods.log"
  oc logs -n "$ns" "$pod" --tail=80 >>"$OUT/failing-pods.log" 2>&1 || true
done < <(oc get pods -A 2>/dev/null | awk '$4 ~ /CrashLoop|Error|OOM|Pending/ {print $1,$2,$4}' || true)

log "artifacts written to $OUT"
