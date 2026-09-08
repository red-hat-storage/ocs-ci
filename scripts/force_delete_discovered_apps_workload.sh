#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# force_delete_discovered_apps_workload.sh
#
# Manually force-delete a stuck RDR Discovered Apps workload by stripping
# finalizers from every blocking resource, disabling the rbd mirror group
# (CG clusters), removing PVCs/PVs, and deleting the workload namespace.
#
# This script mirrors the logic in:
#   ocs_ci/helpers/dr_helpers.py :: force_delete_discovered_apps_workload()
#
# Usage:
#   ./force_delete_discovered_apps_workload.sh \
#       --hub-kubeconfig   <path>            \
#       --c1-kubeconfig    <path>            \
#       --c2-kubeconfig    <path>            \
#       --namespace        <workload-ns>     \
#       --vrg-name         <vrg/drpc-name>   \
#       [--pool-name       <ceph-pool>]      \
#       [--rados-namespace <rados-ns>]       \
#       [--cg]                               \
#       [--dry-run]                          \
#       [--debug]
#
# Required args:
#   --hub-kubeconfig   kubeconfig for ACM hub cluster (DRPC / Placement live here)
#   --c1-kubeconfig    kubeconfig for primary managed cluster
#   --c2-kubeconfig    kubeconfig for secondary managed cluster
#   --namespace        workload namespace  (e.g. dist-workload-2a07c6-rbd)
#   --vrg-name         VRG / DRPC / Placement name (same value for discovered-apps)
#
# Optional args:
#   --pool-name        Ceph RBD pool name (default: ocs-storagecluster-cephblockpool)
#   --rados-namespace  rbd --namespace value for HCI/provider clusters; omit otherwise
#   --cg               Set when Consistency Group (CG) mirroring is enabled
#   --dry-run          Print every oc / toolbox command without executing it
#   --debug            Enable bash trace (set -x) + dump resource state at key points
#
# CG cluster: rbd group name resolution
#   VGR spec.volumeGroupReplicationContentName
#     → VolumeGroupReplicationContent spec.volumeGroupReplicationHandle
#       e.g. "0001-0011-openshift-storage-0000000000000002-<uuid>"
#         → last 5 dash-segments = UUID
#         → rbd group name = "csi-vol-group-<uuid>"
# -----------------------------------------------------------------------------

set -euo pipefail

# ── colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BLUE='\033[0;34m'; RESET='\033[0m'

# Timestamp prefix used by all log functions when --debug is active.
_ts() { date '+%H:%M:%S'; }

info()    { echo -e "${CYAN}[INFO]${RESET}  $(_ts) $*" >&2; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $(_ts) $*" >&2; }
success() { echo -e "${GREEN}[OK]${RESET}    $(_ts) $*" >&2; }
err()     { echo -e "${RED}[ERROR]${RESET} $(_ts) $*" >&2; }

# ── defaults ──────────────────────────────────────────────────────────────────
HUB_KC=""
C1_KC=""
C2_KC=""
NAMESPACE=""
VRG_NAME=""
POOL_NAME="ocs-storagecluster-cephblockpool"
RADOS_NS=""
CG=false
DRY_RUN=false
DEBUG=false
DR_OPS_NS="openshift-dr-ops"
# JSON patch that removes every element of the finalizers array.
# --type=json works on resources that already have deletionTimestamp set,
# unlike --type=merge which can be rejected on terminating objects.
PATCH='[{"op":"remove","path":"/metadata/finalizers"}]'
PATCH_TYPE="json"

# ── argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --hub-kubeconfig)   HUB_KC="$2";    shift 2 ;;
        --c1-kubeconfig)    C1_KC="$2";     shift 2 ;;
        --c2-kubeconfig)    C2_KC="$2";     shift 2 ;;
        --namespace)        NAMESPACE="$2"; shift 2 ;;
        --vrg-name)         VRG_NAME="$2";  shift 2 ;;
        --pool-name)        POOL_NAME="$2"; shift 2 ;;
        --rados-namespace)  RADOS_NS="$2";  shift 2 ;;
        --cg)               CG=true;        shift   ;;
        --dry-run)          DRY_RUN=true;   shift   ;;
        --debug)            DEBUG=true;     shift   ;;
        *) err "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── validation ────────────────────────────────────────────────────────────────
missing=()
[[ -z "$HUB_KC"    ]] && missing+=("--hub-kubeconfig")
[[ -z "$C1_KC"     ]] && missing+=("--c1-kubeconfig")
[[ -z "$C2_KC"     ]] && missing+=("--c2-kubeconfig")
[[ -z "$NAMESPACE" ]] && missing+=("--namespace")
[[ -z "$VRG_NAME"  ]] && missing+=("--vrg-name")
if [[ ${#missing[@]} -gt 0 ]]; then
    err "Missing required arguments: ${missing[*]}"
    exit 1
fi

# ── debug mode ────────────────────────────────────────────────────────────────
# Activated after arg validation so argument errors still print cleanly.
if $DEBUG; then
    # Print every command and its expanded arguments as bash executes them.
    # Output goes to fd 2 (stderr) with a + prefix so it is easy to grep out.
    set -x
fi

# dbg <kubeconfig> <label> — dump current finalizer state for all
# VRG/VR/VGR resources related to this workload.  Only runs when --debug is set.
dbg_state() {
    $DEBUG || return 0
    local kc="$1" label="$2"
    echo -e "${BLUE}[DBG]${RESET}   $(_ts) ── state dump: $label ──" >&2

    echo -e "${BLUE}[DBG]${RESET}   $(_ts) VolumeReplicationGroup (openshift-dr-ops):" >&2
    oc --kubeconfig "$kc" get volumereplicationgroup "$VRG_NAME" \
        -n "$DR_OPS_NS" --ignore-not-found=true \
        -o custom-columns='NAME:.metadata.name,DELETION:.metadata.deletionTimestamp,FINALIZERS:.metadata.finalizers' \
        2>/dev/null >&2 || true

    echo -e "${BLUE}[DBG]${RESET}   $(_ts) VolumeReplication ($NAMESPACE):" >&2
    oc --kubeconfig "$kc" get volumereplication \
        -n "$NAMESPACE" --ignore-not-found=true \
        -o custom-columns='NAME:.metadata.name,DELETION:.metadata.deletionTimestamp,FINALIZERS:.metadata.finalizers' \
        2>/dev/null >&2 || true

    if $CG; then
        echo -e "${BLUE}[DBG]${RESET}   $(_ts) VolumeGroupReplication ($NAMESPACE):" >&2
        oc --kubeconfig "$kc" get volumegroupreplication \
            -n "$NAMESPACE" --ignore-not-found=true \
            -o custom-columns='NAME:.metadata.name,DELETION:.metadata.deletionTimestamp,FINALIZERS:.metadata.finalizers' \
            2>/dev/null >&2 || true
    fi

    echo -e "${BLUE}[DBG]${RESET}   $(_ts) PVCs ($NAMESPACE):" >&2
    oc --kubeconfig "$kc" get pvc \
        -n "$NAMESPACE" --ignore-not-found=true \
        -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,DELETION:.metadata.deletionTimestamp,FINALIZERS:.metadata.finalizers' \
        2>/dev/null >&2 || true

    echo -e "${BLUE}[DBG]${RESET}   $(_ts) ── end dump ──" >&2
}

# ── helpers ───────────────────────────────────────────────────────────────────
oc_hub() {
    if $DRY_RUN; then
        echo "[DRY-RUN] oc --kubeconfig $HUB_KC $*"
    else
        # Keep stderr visible — callers that want silence add 2>/dev/null themselves.
        oc --kubeconfig "$HUB_KC" "$@" || true
    fi
}

oc_managed() {
    local kc="$1"; shift
    if $DRY_RUN; then
        echo "[DRY-RUN] oc --kubeconfig $kc $*"
    else
        oc --kubeconfig "$kc" "$@" || true
    fi
}

# Run a command inside the rook-ceph toolbox pod on a managed cluster.
# Usage: toolbox_exec <kubeconfig> <command-string>
toolbox_exec() {
    local kc="$1"; shift
    local cmd="$*"
    local toolbox_ns="openshift-storage"
    local toolbox_pod

    if $DRY_RUN; then
        echo "[DRY-RUN] oc --kubeconfig $kc rsh -n $toolbox_ns <toolbox-pod> $cmd"
        return
    fi

    toolbox_pod=$(oc --kubeconfig "$kc" get pod \
        -n "$toolbox_ns" \
        -l "app=rook-ceph-tools" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

    if [[ -z "$toolbox_pod" ]]; then
        warn "Toolbox pod not found on kubeconfig $kc — skipping: $cmd"
        return
    fi

    info "  toolbox($toolbox_pod): $cmd"
    oc --kubeconfig "$kc" rsh -n "$toolbox_ns" "$toolbox_pod" bash -c "$cmd" || true
}

# Strip metadata.finalizers from a resource on a managed cluster.
# Usage: patch_finalizers <kubeconfig> <kind> <name> <namespace>
#        namespace may be "" for cluster-scoped resources
patch_finalizers() {
    local kc="$1" kind="$2" name="$3" ns="$4"
    local ns_flag=""
    [[ -n "$ns" ]] && ns_flag="-n $ns"
    info "  Stripping all finalizers: $kind/$name${ns:+ (ns: $ns)}"
    # Check existence first — oc patch has no --ignore-not-found flag.
    # shellcheck disable=SC2086
    local exists
    exists=$(oc --kubeconfig "$kc" get "$kind" "$name" $ns_flag \
        --ignore-not-found=true -o name 2>/dev/null || true)
    if [[ -z "$exists" ]]; then
        info "    $kind/$name not found — skipping patch"
        return
    fi
    local out
    # shellcheck disable=SC2086
    out=$(oc --kubeconfig "$kc" patch "$kind" "$name" $ns_flag \
        --type="$PATCH_TYPE" -p "$PATCH" 2>&1) || true
    echo -e "    ${out}" >&2
}

# Strip metadata.finalizers from a resource on the hub cluster.
# Usage: patch_finalizers_hub <kind> <name> <namespace>
patch_finalizers_hub() {
    local kind="$1" name="$2" ns="$3"
    info "  [hub] Stripping all finalizers: $kind/$name (ns: $ns)"
    # Check existence first — oc patch has no --ignore-not-found flag.
    local exists
    exists=$(oc --kubeconfig "$HUB_KC" get "$kind" "$name" -n "$ns" \
        --ignore-not-found=true -o name 2>/dev/null || true)
    if [[ -z "$exists" ]]; then
        info "    $kind/$name not found on hub — skipping patch"
        return
    fi
    local out
    out=$(oc --kubeconfig "$HUB_KC" patch "$kind" "$name" -n "$ns" \
        --type="$PATCH_TYPE" -p "$PATCH" 2>&1) || true
    echo -e "    ${out}" >&2
}

# Derive the rbd group name for one VGR by walking:
#   VGR spec.volumeGroupReplicationContentName
#     → VGRContent spec.volumeGroupReplicationHandle
#       → last 5 dash-segments = UUID
#       → "csi-vol-group-<UUID>"
# Prints the group name to stdout, or prints nothing on failure.
# Usage: resolve_rbd_group_name <kubeconfig> <vgr-name> <vgr-namespace>
resolve_rbd_group_name() {
    local kc="$1" vgr="$2" vgr_ns="$3"

    # Step 1 – VGRContent name from VGR spec
    local vgrcontent_name
    vgrcontent_name=$(oc --kubeconfig "$kc" get volumegroupreplication \
        "$vgr" -n "$vgr_ns" \
        -o jsonpath='{.spec.volumeGroupReplicationContentName}' \
        2>/dev/null || true)

    if [[ -z "$vgrcontent_name" ]]; then
        warn "  VGR $vgr: spec.volumeGroupReplicationContentName is empty — cannot resolve group name" >&2
        return
    fi
    info "  VGR $vgr → VGRContent: $vgrcontent_name" >&2

    # Step 2 – volumeGroupReplicationHandle from VGRContent spec
    # VolumeGroupReplicationContent is cluster-scoped (no namespace flag)
    local handle
    handle=$(oc --kubeconfig "$kc" get volumegroupreplicationcontent \
        "$vgrcontent_name" \
        -o jsonpath='{.spec.volumeGroupReplicationHandle}' \
        2>/dev/null || true)

    if [[ -z "$handle" ]]; then
        warn "  VGRContent $vgrcontent_name: spec.volumeGroupReplicationHandle is empty — cannot resolve group name" >&2
        return
    fi
    info "  VGRContent handle: $handle" >&2

    # Step 3 – extract UUID = last 5 dash-separated segments of the handle
    # handle format: "<prefix>-<8hex>-<4hex>-<4hex>-<4hex>-<12hex>"
    # e.g. "0001-0011-openshift-storage-0000000000000002-156991e1-3e5a-41d3-bfd5-49b573b8ea3a"
    local uuid
    uuid=$(echo "$handle" | awk -F'-' '{n=NF; print $(n-4)"-"$(n-3)"-"$(n-2)"-"$(n-1)"-"$n}')

    echo "csi-vol-group-${uuid}"
}

# Poll until no resources of a given kind exist in a namespace, or timeout expires.
# For a specific named resource, pass "kind/name" as the kind argument and "" as ns
# to use cluster scope, or pass the real ns.
# Usage: wait_resource_deleted <kubeconfig> <kind-or-kind/name> <namespace> <timeout_seconds>
wait_resource_deleted() {
    local kc="$1" kind="$2" ns="$3" timeout="$4"
    local deadline=$(( $(date +%s) + timeout ))
    local ns_flag=""
    [[ -n "$ns" ]] && ns_flag="-n $ns"
    info "  Waiting up to ${timeout}s for $kind to disappear${ns:+ in $ns} ..." >&2
    while true; do
        local remaining
        # shellcheck disable=SC2086
        remaining=$(oc --kubeconfig "$kc" get "$kind" $ns_flag \
            --ignore-not-found=true \
            -o jsonpath='{.metadata.name}{range .items[*]}{.metadata.name}{"\n"}{end}' \
            2>/dev/null || true)
        if [[ -z "$remaining" ]]; then
            success "  $kind is gone${ns:+ in $ns}" >&2
            return 0
        fi
        if [[ $(date +%s) -ge $deadline ]]; then
            warn "  $kind still exists after ${timeout}s${ns:+ in $ns}" >&2
            return 1
        fi
        sleep 5
    done
}

# Poll until a namespace no longer exists, or timeout (seconds) expires.
# Usage: wait_ns_deleted <kubeconfig> <namespace> <timeout_seconds>
wait_ns_deleted() {
    local kc="$1" ns="$2" timeout="$3"
    local deadline=$(( $(date +%s) + timeout ))
    info "  Waiting up to ${timeout}s for namespace $ns to disappear ..."
    while true; do
        local exists
        exists=$(oc --kubeconfig "$kc" get namespace "$ns" \
            --ignore-not-found=true \
            -o jsonpath='{.metadata.name}' 2>/dev/null || true)
        if [[ -z "$exists" ]]; then
            success "  Namespace $ns is gone"
            return 0
        fi
        if [[ $(date +%s) -ge $deadline ]]; then
            warn "  Namespace $ns still exists after ${timeout}s"
            return 1
        fi
        sleep 5
    done
}

# Poll until all PVCs in a namespace are gone, or timeout expires.
# Usage: wait_pvcs_deleted <kubeconfig> <namespace> <timeout_seconds>
wait_pvcs_deleted() {
    local kc="$1" ns="$2" timeout="$3"
    local deadline=$(( $(date +%s) + timeout ))
    info "  Waiting up to ${timeout}s for all PVCs in $ns to disappear ..."
    while true; do
        local remaining
        remaining=$(oc --kubeconfig "$kc" get pvc \
            -n "$ns" \
            -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
        if [[ -z "$remaining" ]]; then
            success "  All PVCs in $ns are gone"
            return 0
        fi
        if [[ $(date +%s) -ge $deadline ]]; then
            warn "  PVCs still present after ${timeout}s: $remaining"
            return 1
        fi
        sleep 5
    done
}

# ── banner ────────────────────────────────────────────────────────────────────
echo ""
info "===== force_delete_discovered_apps_workload ====="
info "  namespace  : $NAMESPACE"
info "  vrg-name   : $VRG_NAME"
info "  pool       : $POOL_NAME"
info "  CG mode    : $CG"
info "  dry-run    : $DRY_RUN"
info "  debug      : $DEBUG"
[[ -n "$RADOS_NS" ]] && info "  rados-ns   : $RADOS_NS"
echo ""

PLACEMENT_NAME="${VRG_NAME}-plmnt-1"
MANAGED_KUBECONFIGS=("$C1_KC" "$C2_KC")

# ============================================================================
# Step 1 – Strip DRPC finalizers + delete (hub)
# Ramen stops reconciling VRGs on managed clusters once the DRPC is gone.
# We MUST wait for it to leave etcd before touching VRGs.
# ============================================================================
info "── Step 1: Strip + delete DRPC ${VRG_NAME} (hub) ──"
patch_finalizers_hub "drpc" "$VRG_NAME" "$DR_OPS_NS"
if $DRY_RUN; then
    echo "[DRY-RUN] oc --kubeconfig $HUB_KC delete drpc $VRG_NAME -n $DR_OPS_NS --wait=false"
    echo "[DRY-RUN] wait up to 120s for DRPC to disappear"
else
    oc --kubeconfig "$HUB_KC" delete drpc "$VRG_NAME" \
        -n "$DR_OPS_NS" --wait=false --ignore-not-found=true 2>&1 || true
    wait_resource_deleted "$HUB_KC" "drpc/$VRG_NAME" "$DR_OPS_NS" 120 || \
        warn "  DRPC $VRG_NAME still present on hub — ramen may still be reconciling VRGs"
fi

# ============================================================================
# Step 2 – Strip Placement finalizers + delete (hub)
# ============================================================================
info "── Step 2: Strip + delete Placement ${PLACEMENT_NAME} (hub) ──"
patch_finalizers_hub "placement" "$PLACEMENT_NAME" "$DR_OPS_NS"
if $DRY_RUN; then
    echo "[DRY-RUN] oc --kubeconfig $HUB_KC delete placement $PLACEMENT_NAME -n $DR_OPS_NS --wait=false"
else
    oc --kubeconfig "$HUB_KC" delete placement "$PLACEMENT_NAME" \
        -n "$DR_OPS_NS" --wait=false --ignore-not-found=true 2>&1 || true
fi

# ============================================================================
# Steps 3–5 – per managed cluster
# ============================================================================
for KC in "${MANAGED_KUBECONFIGS[@]}"; do
    CLUSTER_NAME=$(oc --kubeconfig "$KC" \
        config view --minify -o jsonpath='{.clusters[0].name}' 2>/dev/null \
        || basename "$KC")

    echo ""
    info "══ Cluster: ${CLUSTER_NAME} ══"

    # ── debug: snapshot state before we touch anything ──────────────────────
    dbg_state "$KC" "before-step-3a (${CLUSTER_NAME})"

    # -- 3a: VolumeReplicationGroup — strip finalizers then delete -------------
    info "── Step 3a: Strip + delete VRG ${VRG_NAME} (${DR_OPS_NS}) ──"
    patch_finalizers "$KC" "volumereplicationgroup" "$VRG_NAME" "$DR_OPS_NS"
    if $DRY_RUN; then
        echo "[DRY-RUN] oc --kubeconfig $KC delete volumereplicationgroup $VRG_NAME -n $DR_OPS_NS --wait=false"
    else
        oc_managed "$KC" delete volumereplicationgroup "$VRG_NAME" \
            -n "$DR_OPS_NS" --wait=false --ignore-not-found=true || true
    fi

    # -- 3b: VolumeReplication — strip finalizers, delete, wait ----------------
    info "── Step 3b: Strip + delete VolumeReplication resources (${NAMESPACE}) ──"
    if $DRY_RUN; then
        echo "[DRY-RUN] oc --kubeconfig $KC get volumereplication -n $NAMESPACE -o jsonpath=..."
        echo "[DRY-RUN]   per VR: strip all finalizers"
        echo "[DRY-RUN] oc --kubeconfig $KC delete volumereplication --all -n $NAMESPACE --wait=false"
        echo "[DRY-RUN] wait up to 60s for VRs to disappear"
    else
        vr_names=$(oc --kubeconfig "$KC" get volumereplication \
            -n "$NAMESPACE" \
            -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
        if [[ -z "$vr_names" ]]; then
            info "  No VolumeReplication resources found in $NAMESPACE"
        else
            for vr in $vr_names; do
                patch_finalizers "$KC" "volumereplication" "$vr" "$NAMESPACE"
            done
            oc_managed "$KC" delete volumereplication --all \
                -n "$NAMESPACE" --wait=false || true
            # Wait for VRs to be fully gone — as long as VRs exist the
            # replication controller keeps re-adding PVC finalizers.
            wait_resource_deleted "$KC" "volumereplication" "$NAMESPACE" 60 || \
                warn "  Some VolumeReplication resources may still exist in $NAMESPACE"
        fi
    fi

    # ── debug: after VR deletion, before VGR ────────────────────────────────
    dbg_state "$KC" "after-step-3b (${CLUSTER_NAME})"

    # -- 3c: VolumeGroupReplication + rbd mirror group disable (CG only) -------
    if $CG; then
        info "── Step 3c: Strip + delete VolumeGroupReplication + disable rbd mirror group (CG) ──"

        # Optional --namespace flag for rbd toolbox commands
        RBD_NS_FLAG=""
        [[ -n "$RADOS_NS" ]] && RBD_NS_FLAG="--namespace $RADOS_NS"

        if $DRY_RUN; then
            echo "[DRY-RUN] oc --kubeconfig $KC get volumegroupreplication -n $NAMESPACE -o jsonpath=..."
            echo "[DRY-RUN]   per VGR: strip finalizers"
            echo "[DRY-RUN]   per VGR: resolve VGRContent → volumeGroupReplicationHandle → csi-vol-group-<uuid>"
            echo "[DRY-RUN]   rbd mirror group disable --force <pool>/csi-vol-group-<uuid> ${RBD_NS_FLAG}"
            echo "[DRY-RUN] oc --kubeconfig $KC delete volumegroupreplication --all -n $NAMESPACE --wait=false"
            echo "[DRY-RUN] wait up to 60s for VGRs to disappear"
        else
            vgr_names=$(oc --kubeconfig "$KC" get volumegroupreplication \
                -n "$NAMESPACE" \
                -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)

            if [[ -z "$vgr_names" ]]; then
                info "  No VolumeGroupReplication resources found in $NAMESPACE"
            else
                for vgr in $vgr_names; do
                    # Strip VGR finalizers
                    patch_finalizers "$KC" "volumegroupreplication" "$vgr" "$NAMESPACE"

                    # Resolve rbd group name via VGR → VGRContent → handle
                    # (Must be done before we delete the VGR object)
                    rbd_group_name=$(resolve_rbd_group_name "$KC" "$vgr" "$NAMESPACE")

                    if [[ -z "$rbd_group_name" ]]; then
                        warn "  Could not resolve rbd group name for VGR $vgr — skipping rbd disable"
                    else
                        info "  Resolved rbd group name: $rbd_group_name"
                        info "  rbd mirror group disable --force: ${POOL_NAME}/${rbd_group_name}"
                        toolbox_exec "$KC" \
                            "rbd mirror group disable --force ${POOL_NAME}/${rbd_group_name} ${RBD_NS_FLAG}"
                    fi
                done
                oc_managed "$KC" delete volumegroupreplication --all \
                    -n "$NAMESPACE" --wait=false 2>/dev/null || true
                wait_resource_deleted "$KC" "volumegroupreplication" "$NAMESPACE" 60 || \
                    warn "  Some VolumeGroupReplication resources may still exist in $NAMESPACE"
            fi
        fi
    fi

    # -- 3d: Wait for VRG to be fully gone before touching PVCs ----------------
    # The VRG controller re-adds PVC finalizers as long as the VRG object
    # exists in etcd.  We must confirm it is gone before proceeding.
    # Strategy: wait 60s; if still present, re-strip finalizer and wait another
    # 120s.  Total budget: 3 minutes.  Only then proceed anyway with a warning.
    info "── Step 3d: Wait for VRG ${VRG_NAME} to disappear (${DR_OPS_NS}) ──"
    if $DRY_RUN; then
        echo "[DRY-RUN] wait up to 60s; re-strip if needed; wait another 120s"
    else
        if ! wait_resource_deleted "$KC" "volumereplicationgroup/$VRG_NAME" "$DR_OPS_NS" 60; then
            info "  VRG still present — re-stripping finalizer and waiting 120s more"
            patch_finalizers "$KC" "volumereplicationgroup" "$VRG_NAME" "$DR_OPS_NS"
            oc_managed "$KC" delete volumereplicationgroup "$VRG_NAME" \
                -n "$DR_OPS_NS" --wait=false --ignore-not-found=true || true
            wait_resource_deleted "$KC" "volumereplicationgroup/$VRG_NAME" "$DR_OPS_NS" 120 || \
                warn "  VRG $VRG_NAME still exists after 3 min — proceeding with PVC cleanup anyway"
        fi
    fi

    # ── debug: VRG gone, about to touch PVCs ────────────────────────────────
    dbg_state "$KC" "after-step-3d-before-pvc (${CLUSTER_NAME})"

    # -- 3e: Re-strip VR/VGR finalizers now that VRG is gone ------------------
    # The VGR controller may have re-added replication finalizers to VRs while
    # VGR was still alive in step 3d.  Do a fast second sweep — strip + delete
    # all remaining VR/VGR objects without waiting so we reach PVC cleanup
    # as quickly as possible.
    info "── Step 3e: Re-strip VR/VGR finalizers (post-VRG) (${NAMESPACE}) ──"
    if $DRY_RUN; then
        echo "[DRY-RUN]   per VR: strip finalizers again; delete volumereplication --all"
        $CG && echo "[DRY-RUN]   per VGR: strip finalizers again; delete volumegroupreplication --all"
    else
        vr_names_recheck=$(oc --kubeconfig "$KC" get volumereplication \
            -n "$NAMESPACE" \
            -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
        if [[ -n "$vr_names_recheck" ]]; then
            for vr in $vr_names_recheck; do
                patch_finalizers "$KC" "volumereplication" "$vr" "$NAMESPACE"
            done
            oc_managed "$KC" delete volumereplication --all \
                -n "$NAMESPACE" --wait=false || true
        else
            info "  No VolumeReplication resources remain in $NAMESPACE"
        fi

        if $CG; then
            vgr_names_recheck=$(oc --kubeconfig "$KC" get volumegroupreplication \
                -n "$NAMESPACE" \
                -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
            if [[ -n "$vgr_names_recheck" ]]; then
                for vgr in $vgr_names_recheck; do
                    patch_finalizers "$KC" "volumegroupreplication" "$vgr" "$NAMESPACE"
                done
                oc_managed "$KC" delete volumegroupreplication --all \
                    -n "$NAMESPACE" --wait=false || true
            else
                info "  No VolumeGroupReplication resources remain in $NAMESPACE"
            fi
        fi
    fi

    # -- 4: pods, PVCs, and PVs -------------------------------------------------
    # Delete pods and workload controllers first so kubernetes.io/pvc-protection
    # is released before we remove the PVCs.
    # VRG/VGR/VR controllers are now gone; replication finalizers won't be re-added.
    #
    # Wrapped in a retry loop: if the namespace is still alive after step 5,
    # re-run strip+delete of VR/VGR/pods/PVCs and try again (up to 3 attempts).
    NS_CLEANUP_ATTEMPTS=3
    ns_attempt=0
    while [[ $ns_attempt -lt $NS_CLEANUP_ATTEMPTS ]]; do
        ns_attempt=$(( ns_attempt + 1 ))
        [[ $ns_attempt -gt 1 ]] && \
            info "  ── Retry attempt $ns_attempt / $NS_CLEANUP_ATTEMPTS ──"

        info "── Step 4: Delete pods/controllers + strip finalizers + delete PVCs (${NAMESPACE}) ──"
        if $DRY_RUN; then
            echo "[DRY-RUN] oc --kubeconfig $KC delete deployment,statefulset,replicaset,pod --all -n $NAMESPACE --wait=false"
            echo "[DRY-RUN] sleep 5"
            echo "[DRY-RUN] oc --kubeconfig $KC get pvc -n $NAMESPACE  (collect names)"
            echo "[DRY-RUN]   per PVC: strip PVC finalizers; strip backing PV finalizers"
            echo "[DRY-RUN] oc --kubeconfig $KC delete pvc --all -n $NAMESPACE --wait=false"
            echo "[DRY-RUN] wait up to 60s for all PVCs to disappear"
        else
            # Delete workload pods and controllers so pvc-protection is released.
            for kind in deployment statefulset replicaset pod; do
                oc_managed "$KC" delete "$kind" --all \
                    -n "$NAMESPACE" --wait=false --ignore-not-found=true 2>/dev/null || true
            done
            # Brief pause — kubelet needs a moment to detach volumes and drop
            # the pvc-protection hold before we list/patch PVCs.
            sleep 5

            # Re-strip VR/VGR finalizers one more time in case controllers
            # re-added them while pods were terminating.
            vr_names_fast=$(oc --kubeconfig "$KC" get volumereplication \
                -n "$NAMESPACE" \
                -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
            if [[ -n "$vr_names_fast" ]]; then
                for vr in $vr_names_fast; do
                    patch_finalizers "$KC" "volumereplication" "$vr" "$NAMESPACE"
                done
                oc_managed "$KC" delete volumereplication --all \
                    -n "$NAMESPACE" --wait=false || true
            fi
            if $CG; then
                vgr_names_fast=$(oc --kubeconfig "$KC" get volumegroupreplication \
                    -n "$NAMESPACE" \
                    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
                if [[ -n "$vgr_names_fast" ]]; then
                    for vgr in $vgr_names_fast; do
                        patch_finalizers "$KC" "volumegroupreplication" "$vgr" "$NAMESPACE"
                    done
                    oc_managed "$KC" delete volumegroupreplication --all \
                        -n "$NAMESPACE" --wait=false || true
                fi
            fi

            # Collect current PVC list (may have shrunk since step 3e).
            all_pvcs=$(oc --kubeconfig "$KC" get pvc \
                -n "$NAMESPACE" \
                -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)

            if [[ -n "$all_pvcs" ]]; then
                for pvc in $all_pvcs; do
                    info "  Stripping finalizers from PVC $pvc"
                    oc_managed "$KC" patch pvc "$pvc" -n "$NAMESPACE" \
                        --type="$PATCH_TYPE" -p "$PATCH" || true
                    # Also strip the backing PV finalizer so the PV doesn't
                    # block namespace termination after the PVC is gone.
                    pv_name=$(oc --kubeconfig "$KC" get pvc "$pvc" \
                        -n "$NAMESPACE" \
                        -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)
                    if [[ -n "$pv_name" ]]; then
                        info "  Stripping finalizers from PV $pv_name"
                        oc --kubeconfig "$KC" patch pv "$pv_name" \
                            --type="$PATCH_TYPE" -p "$PATCH" 2>/dev/null || true
                    fi
                done

                oc_managed "$KC" delete pvc --all -n "$NAMESPACE" --wait=false 2>/dev/null || true

                if ! wait_pvcs_deleted "$KC" "$NAMESPACE" 60; then
                    warn "  Some PVCs still stuck — re-stripping and continuing to namespace delete"
                    for pvc in $all_pvcs; do
                        oc_managed "$KC" patch pvc "$pvc" -n "$NAMESPACE" \
                            --type="$PATCH_TYPE" -p "$PATCH" || true
                        pv_name=$(oc --kubeconfig "$KC" get pvc "$pvc" \
                            -n "$NAMESPACE" \
                            -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)
                        if [[ -n "$pv_name" ]]; then
                            oc --kubeconfig "$KC" patch pv "$pv_name" \
                                --type="$PATCH_TYPE" -p "$PATCH" 2>/dev/null || true
                        fi
                    done
                fi
            else
                info "  No PVCs found in $NAMESPACE"
            fi
        fi

        # ── debug: after PVC deletion ──────────────────────────────────────
        dbg_state "$KC" "after-step-4-pvc attempt-${ns_attempt} (${CLUSTER_NAME})"

        # -- 5: namespace -------------------------------------------------------
        info "── Step 5: Delete namespace ${NAMESPACE} (attempt ${ns_attempt}) ──"
        if $DRY_RUN; then
            echo "[DRY-RUN] oc --kubeconfig $KC delete project $NAMESPACE --ignore-not-found=true"
            echo "[DRY-RUN] wait up to 60s for namespace to disappear"
            break
        fi

        oc_managed "$KC" delete project "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true

        if wait_ns_deleted "$KC" "$NAMESPACE" 60; then
            break   # namespace is gone — move on to the next cluster
        fi

        if [[ $ns_attempt -ge $NS_CLEANUP_ATTEMPTS ]]; then
            warn "  Namespace $NAMESPACE still exists after $NS_CLEANUP_ATTEMPTS attempts on $CLUSTER_NAME — manual cleanup may be needed"
        else
            warn "  Namespace $NAMESPACE not gone after 60s — re-running strip+delete cycle (attempt $ns_attempt)"
            # Re-strip VR/VGR finalizers at the top of the next loop iteration
            # (steps 3e and 4 repeat automatically).
        fi
    done

    success "Completed cluster ${CLUSTER_NAME}"
done

echo ""
success "===== force-cleanup finished: ns=${NAMESPACE}, vrg=${VRG_NAME} ====="
