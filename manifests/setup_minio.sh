#!/bin/bash
# Setup MinIO on an OpenShift managed cluster for Ramen DR
# Usage: ./setup_minio.sh
#
# Prerequisites:
#   - oc CLI logged into the target managed cluster
#   - KUBECONFIG set to the target cluster

set -euo pipefail

echo "=== Step 1: Deploy MinIO ==="
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oc apply -f "${SCRIPT_DIR}/../ocs_ci/templates/ocs-deployment/multicluster/minio.yaml"

echo "=== Step 2: Grant anyuid SCC and remove hostPort ==="
oc adm policy add-scc-to-user anyuid -z default -n minio
oc patch deployment minio -n minio --type=json \
  -p '[{"op":"remove","path":"/spec/template/spec/containers/0/ports/0/hostPort"}]'

echo "=== Step 3: Replace hostPath volume with emptyDir ==="
oc patch deployment minio -n minio --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/volumes/0","value":{"name":"storage","emptyDir":{}}}]'

echo "=== Step 4: Wait for MinIO pod to be running ==="
oc rollout status deployment/minio -n minio --timeout=120s

echo "=== Step 5: Create bucket ==="
oc run mc-client --image=quay.io/minio/mc --rm -it --restart=Never -n minio --command \
  -- /bin/sh -c "mc alias set myminio http://minio.minio.svc:9000 minio minio123 && mc mb myminio/bucket"

echo "=== Step 6: Expose MinIO via route ==="
oc expose svc/minio -n minio --port=9000
MINIO_ROUTE=$(oc get route minio -n minio -o jsonpath='{.spec.host}')

echo "=== MinIO setup complete ==="
echo "Internal Endpoint: http://minio.minio.svc:9000"
echo "External Endpoint: http://${MINIO_ROUTE}"
echo "Bucket: bucket"
echo "Access Key: minio"
echo "Secret Key: minio123"
