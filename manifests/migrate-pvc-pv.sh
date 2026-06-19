#!/bin/bash

if [ "$#" -ne 6 ]; then
    echo "Usage: $0 <LABEL_QUERY> <KUBECONFIG_C1> <KUBECONFIG_C2> <VGR_NAME> <VGR_NAMESPACE> <VGR_CLASS>"
    echo "Example: $0 'ramendr.openshift.io/consistency-group=my-cg' c1 c2 vgr-1 ramen-system vgrc-1"
    exit 1
fi

LABEL_QUERY=$1
KUBECONFIG_C1=$2
KUBECONFIG_C2=$3
VGR_NAME=$4
VGR_NAMESPACE=$5
VGR_CLASS=$6

CG_VALUE=$(echo "$LABEL_QUERY" | cut -d'=' -f2)

RESTORE_ANN="volumereplicationgroups.ramendr.openshift.io/ramen-restore"
ACM_PREFIX="apps.open-cluster-management.io"
CG_LABEL="ramendr.openshift.io/consistency-group"

BASE_CLEAN='del(
    .metadata.resourceVersion,
    .metadata.uid,
    .metadata.creationTimestamp,
    .metadata.managedFields,
    .metadata.ownerReferences,
    .status
)'

JQ_FILTER_PV="$BASE_CLEAN | del(.spec.claimRef, .metadata.annotations)
  | .metadata.annotations = {(\$ann): \"True\"}
  | .metadata.labels = {(\$cg_key): .metadata.labels[\$cg_key]}"

JQ_FILTER_PVC="$BASE_CLEAN | del(.metadata.finalizers)
  | .metadata.annotations //= {}
  | .metadata.annotations |= (with_entries(select(.key | startswith(\"$ACM_PREFIX\"))) + {(\$ann): \"True\"})
  | .metadata.labels = {(\$cg_key): .metadata.labels[\$cg_key]}"

echo "Starting migration: $KUBECONFIG_C1 -> $KUBECONFIG_C2"

PVCS=$(kubectl --kubeconfig="$KUBECONFIG_C1" get pvc -A -l "$LABEL_QUERY" -o jsonpath='{range .items[*]}{.metadata.namespace}{":"}{.metadata.name}{" "}{end}')

if [ -z "$PVCS" ]; then
    echo "No PVCs found for $LABEL_QUERY"
else
    for entry in $PVCS; do
        NAMESPACE=$(echo "$entry" | cut -d':' -f1)
        PVC_NAME=$(echo "$entry" | cut -d':' -f2)

        PV_NAME=$(kubectl --kubeconfig="$KUBECONFIG_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}')

        if [ -n "$PV_NAME" ]; then
            echo "[PV]  Migrating: $PV_NAME"
            kubectl --kubeconfig="$KUBECONFIG_C1" get pv "$PV_NAME" -o json | \
            jq --arg ann "$RESTORE_ANN" --arg cg_key "$CG_LABEL" "$JQ_FILTER_PV" | \
            kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
        fi

        kubectl --kubeconfig="$KUBECONFIG_C2" create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -

        echo "[PVC] Migrating (Filtering ACM Annotations): $NAMESPACE/$PVC_NAME"
        kubectl --kubeconfig="$KUBECONFIG_C1" -n "$NAMESPACE" get pvc "$PVC_NAME" -o json | \
        jq --arg ann "$RESTORE_ANN" --arg cg_key "$CG_LABEL" "$JQ_FILTER_PVC" | \
        kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
    done
fi

echo "---------------------------------------------------"
echo "Creating VolumeGroupReplication (VGR) on $KUBECONFIG_C2..."

kubectl --kubeconfig="$KUBECONFIG_C2" create namespace "$VGR_NAMESPACE" --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -

cat <<EOF | kubectl --kubeconfig="$KUBECONFIG_C2" apply -f -
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeGroupReplication
metadata:
  labels:
    ramendr.openshift.io/created-by-ramen: "true"
  name: $VGR_NAME
  namespace: $VGR_NAMESPACE
spec:
  external: true
  replicationState: secondary
  source:
    selector:
      matchLabels:
        $CG_LABEL: $CG_VALUE
  volumeGroupReplicationClassName: $VGR_CLASS
EOF

echo "Migration and VGR creation complete."
