---
name: oc-cluster-ops
description: OpenShift oc/kubectl patterns for DFBUGS verification on live clusters
---

# OpenShift / oc

## Preconditions

- `KUBECONFIG` points at target cluster
- User has read access; write only when script explicitly requires it

## Common checks

```bash
oc version
oc get clusterversion
oc get storagecluster -n openshift-storage -o yaml
oc get cephcluster -n openshift-storage
oc exec -n openshift-storage deploy/rook-ceph-tools -- ceph status
```

## Evidence collection

```bash
oc get events -A --sort-by='.lastTimestamp' | tail -100
oc logs -n openshift-storage -l app=rook-ceph-operator --tail=200
```

## Safety

Never run commands matching `.claude/configs/policies/safety.yaml` forbidden patterns.
