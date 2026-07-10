#!/bin/bash

# Default settings
MIGRATE_PV=true
MIGRATE_LOCAL_PV=false
LABEL_QUERY=""
KUBECONFIG_C1=""
KUBECONFIG_C2=""
VGR_NAME=""
VGR_NS=""
VGR_CLASS=""
SPECIFIED_NS=""
SECRET_NAME="volsync-rsync-tls-secret"

# Local SC Config
LOCAL_SC="localblock"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --no-pv              Skip PV migration (PVC only)"
    echo "  --local-pv           Provision Local PVs matching source volumeName"
    echo "  --namespace          Scope to this namespace (Optional)"
    echo "  --label              Label query"
    echo "  --from               Source kubeconfig (C1)"
    echo "  --to                 Target kubeconfig (C2)"
    echo "  --vgr-name           VGR Name"
    echo "  --vgr-ns             VGR Namespace"
    echo "  --vgr-class          VGR StorageClass"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-pv) MIGRATE_PV=false; shift ;;
        --local-pv) MIGRATE_LOCAL_PV=true; MIGRATE_PV=false; shift ;;
        --namespace) SPECIFIED_NS="$2"; shift 2 ;;
        --namespace=*) SPECIFIED_NS="${1#*=}"; shift ;;
        --label) LABEL_QUERY="$2"; shift 2 ;;
        --label=*) LABEL_QUERY="${1#*=}"; shift ;;
        --from) KUBECONFIG_C1="$2"; shift 2 ;;
        --from=*) KUBECONFIG_C1="${1#*=}"; shift ;;
        --to) KUBECONFIG_C2="$2"; shift 2 ;;
        --to=*) KUBECONFIG_C2="${1#*=}"; shift ;;
        --vgr-name) VGR_NAME="$2"; shift 2 ;;
        --vgr-name=*) VGR_NAME="${1#*=}"; shift ;;
        --vgr-ns) VGR_NS="$2"; shift 2 ;;
        --vgr-ns=*) VGR_NS="${1#*=}"; shift ;;
        --vgr-class) VGR_CLASS="$2"; shift 2 ;;
        --vgr-class=*) VGR_CLASS="${1#*=}"; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -z "$LABEL_QUERY" || -z "$KUBECONFIG_C1" || -z "$KUBECONFIG_C2" || -z "$VGR_NAME" || -z "$VGR_NS" || -z "$VGR_CLASS" ]]; then
    echo "Error: Missing required arguments."; usage
fi

# DISCOVERY PHASE
NS_FLAG=${SPECIFIED_NS:+-n $SPECIFIED_NS}
[[ -z "$NS_FLAG" ]] && NS_FLAG="-A"

PVCS=$(kubectl --kubeconfig="$KUBECONFIG_C1" get pvc $NS_FLAG -l "$LABEL_QUERY" -o jsonpath='{range .items[*]}{.metadata.namespace}{":"}{.metadata.name}{" "}{end}')

if [[ -z "$PVCS" ]]; then
    echo "Error: No PVCs found for '$LABEL_QUERY'. Aborting."; exit 1
fi

# FUNCTION: Dynamic Local Storage Setup
provision_dynamic_local_storage() {
    echo "--------------------------------------------------"
    echo "[Local-PV] Ensuring StorageClass '$LOCAL_SC' exists on $KUBECONFIG_C2..."

    kubectl --kubeconfig="$KUBECONFIG_C2" delete sc "$LOCAL_SC" --ignore-not-found=true

    cat <<EOF | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: $LOCAL_SC
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
EOF

    for entry in $PVCS; do
        NAMESPACE=$(echo "$entry" | cut -d':' -f1)
        PVC_NAME=$(echo "$entry" | cut -d':' -f2)

        PV_NAME=$(kubectl --kubeconfig="$KUBECONFIG_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}')

        if [[ "$PV_NAME" =~ local-pv-disk-(compute-[0-9]+)-([0-9]+) ]]; then
            NODE="${BASH_REMATCH[1]}"
            IDX="${BASH_REMATCH[2]}"
        else
            echo "  [WARN] PVC $PVC_NAME volumeName ($PV_NAME) pattern unexpected. Skipping local provisioning."
            continue
        fi

        echo "[Local-PV] Provisioning disk target for $PV_NAME on Node $NODE (Disk index: $IDX)..."

        TARGET_DEVICE=$(oc --kubeconfig="$KUBECONFIG_C2" debug node/$NODE -- chroot /host /bin/bash -c \
            "lsblk -dno NAME,TYPE | grep 'disk' | grep -E '^sd[b-z]|^nvme[1-9]n1'" 2>/dev/null | awk -v idx="$((IDX + 1))" 'NR==idx {print $1}')

        if [[ -z "$TARGET_DEVICE" ]]; then
            echo "  [ERROR] No unassigned data disk at index $IDX found on $NODE. Skipping." >&2; continue
        fi

        ID=$(oc --kubeconfig="$KUBECONFIG_C2" debug node/$NODE -- chroot /host /bin/bash -c \
            "ls -l /dev/disk/by-id/ | grep 'wwn-' | grep '../../$TARGET_DEVICE'" 2>/dev/null | awk '{print $9}')

        if [[ -z "$ID" ]]; then
            echo "  [ERROR] No WWN ID found for $TARGET_DEVICE on $NODE. Skipping." >&2; continue
        fi

        cat <<EOF | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: $PV_NAME
  labels:
    kubernetes.io/hostname: $NODE
spec:
  capacity:
    storage: 512Gi
  accessModes: ["ReadWriteOnce"]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: $LOCAL_SC
  volumeMode: Filesystem
  local:
    path: /dev/disk/by-id/$ID
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: ["$NODE"]
EOF
        echo "  [SUCCESS] Provisioned target PV $PV_NAME pointing to device $TARGET_DEVICE ($ID)"
    done
}

sync_volsync_secret() {
    local target_ns=$1
    EXISTING_PSK=$(kubectl --kubeconfig="$KUBECONFIG_C1" -n "$target_ns" get secret "$SECRET_NAME" -o jsonpath='{.data.psk\.txt}' 2>/dev/null | base64 -d)
    if [[ -z "$EXISTING_PSK" ]]; then
        PSK="volsync-mock:$(openssl rand -base64 64)"
        kubectl --kubeconfig="$KUBECONFIG_C1" -n "$target_ns" create secret generic "$SECRET_NAME" --from-literal=psk.txt="$PSK" --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_C1" apply -f -
    else
        PSK="$EXISTING_PSK"
    fi
    kubectl --kubeconfig="$KUBECONFIG_C2" create namespace "$target_ns" --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
    kubectl --kubeconfig="$KUBECONFIG_C2" -n "$target_ns" create secret generic "$SECRET_NAME" --from-literal=psk.txt="$PSK" --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
}

# RUN LOGIC
echo "--- RamenDR, VolSync & Storage Migration Tool ---"
echo "Source: $KUBECONFIG_C1 | Target: $KUBECONFIG_C2"

if [[ "$MIGRATE_LOCAL_PV" == "true" ]]; then
    provision_dynamic_local_storage
fi

UNIQUE_NS=$(echo "$PVCS" | tr ' ' '\n' | cut -d':' -f1 | sort -u)
for ns in $UNIQUE_NS; do sync_volsync_secret "$ns"; done

RESTORE_ANN="volumereplicationgroups.ramendr.openshift.io/ramen-restore"
CG_LABEL="ramendr.openshift.io/consistency-group"
PREFIX_ACM="apps.open-cluster-management.io"
PREFIX_VOLSYNC="volsync.backube"
PREFIX_ARGO="argocd.argoproj.io"

BASE_CLEAN='del(.metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp, .metadata.managedFields, .metadata.ownerReferences, .status)'
JQ_FILTER_PV="$BASE_CLEAN | del(.spec.claimRef, .metadata.annotations) | .metadata.annotations = {(\$ann): \"True\"} | .metadata.labels = {(\$cg_key): .metadata.labels[\$cg_key]}"
JQ_FILTER_PVC="$BASE_CLEAN | del(.metadata.finalizers) | .metadata.annotations //= {} | .metadata.annotations |= (with_entries(select(.key | (startswith(\"$PREFIX_ACM\") or startswith(\"$PREFIX_VOLSYNC\") or startswith(\"$PREFIX_ARGO\")))) + {(\$ann): \"True\"}) | .metadata.labels = {(\$cg_key): .metadata.labels[\$cg_key]}"

for entry in $PVCS; do
    NAMESPACE=$(echo "$entry" | cut -d':' -f1)
    PVC_NAME=$(echo "$entry" | cut -d':' -f2)

    if [[ "$MIGRATE_PV" == "true" ]]; then
        PV_NAME=$(kubectl --kubeconfig="$KUBECONFIG_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}')
        if [[ -n "$PV_NAME" ]]; then
            kubectl --kubeconfig="$KUBECONFIG_C1" get pv "$PV_NAME" -o json | jq --arg ann "$RESTORE_ANN" --arg cg_key "$CG_LABEL" "$JQ_FILTER_PV" | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
        fi
    fi

    echo "[PVC] Migrating: $NAMESPACE/$PVC_NAME"
    kubectl --kubeconfig="$KUBECONFIG_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o json | jq --arg ann "$RESTORE_ANN" --arg cg_key "$CG_LABEL" "$JQ_FILTER_PVC" | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
done

# Finalize VGR with Exact Label Mirroring
echo "---------------------------------------------------"
echo "Syncing VGR metadata and creating resource on $KUBECONFIG_C2..."

CG_VALUE=$(echo "$LABEL_QUERY" | cut -d'=' -f2)

kubectl --kubeconfig="$KUBECONFIG_C2" create namespace "$VGR_NS" --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -

SOURCE_VGR_LABELS=$(kubectl --kubeconfig="$KUBECONFIG_C1" -n "$VGR_NS" get volumegroupreplication "$VGR_NAME" -o jsonpath='{.metadata.labels}' 2>/dev/null)
if [[ -z "$SOURCE_VGR_LABELS" ]]; then
    SOURCE_VGR_LABELS="{}"
fi

jq -n \
  --arg name "$VGR_NAME" \
  --arg ns "$VGR_NS" \
  --arg class "$VGR_CLASS" \
  --arg cg_key "$CG_LABEL" \
  --arg cg_val "$CG_VALUE" \
  --argjson src_labels "$SOURCE_VGR_LABELS" \
  '
  {
    apiVersion: "replication.storage.openshift.io/v1alpha1",
    kind: "VolumeGroupReplication",
    metadata: {
      name: $name,
      namespace: $ns,
      labels: $src_labels
    },
    spec: {
      external: true,
      replicationState: "secondary",
      source: {
        selector: {
          matchLabels: {
            ($cg_key): $cg_val
          }
        }
      },
      volumeGroupReplicationClassName: $class
    }
  }
  ' | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -

echo "Done."
