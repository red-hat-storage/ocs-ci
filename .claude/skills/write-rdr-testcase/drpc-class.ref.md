## DRPC Class Methods Reference

The `DRPC` class wraps a `DRPlacementControl` CR. Import from `ocs_ci.ocs.resources.drpc`.

### Construction

```python
from ocs_ci.ocs.resources.drpc import DRPC

# Subscription workload
drpc_sub = DRPC(namespace=wl.workload_namespace)

# AppSet workload
drpc_appset = DRPC(
    namespace=constants.GITOPS_CLUSTER_NAMESPACE,
    resource_name=f"{wl.appset_placement_name}-drpc",
)

# Discovered-apps workload
drpc_discovered = DRPC(
    namespace=constants.DR_OPS_NAMESPACE,
    resource_name=wl.discovered_apps_placement_name,
)
```

> The `DRPC` constructor auto-calls `config.switch_acm_ctx()`. No manual context switch needed before construction.

### Properties

| Property | Type | What it returns |
|---|---|---|
| `drpolicy` | `str` | Name of the `DRPolicy` referenced by this DRPC |
| `drpolicy_obj` | `OCS` | Full `DRPolicy` CR object (lazy-loaded). Use to read `schedulingInterval`, `drClusters`, `peerClasses`. |

### Sync-time methods (raw — use `dr_helpers` wrappers in tests)

```python
last_sync = drpc_obj.get_last_group_sync_time()
last_kubeobj = drpc_obj.get_last_kubeobject_protection_time()
```

### Status wait methods

| Method | Signature | When to use |
|---|---|---|
| `wait_for_peer_ready_status` | `(timeout=300, sleep=10)` | After DR policy change or cluster recovery — before initiating new DR action. |
| `wait_for_clusterdataprotected_status` | `(timeout=300, sleep=10)` | Confirms full data-protection cycle completed. |
| `wait_for_progression_status` | `(status, timeout=300, sleep=10, success_if_deleted=False)` | Poll `DRPC.status.progression` until it matches `status`. |

```python
# After failover via hub context
config.switch_acm_ctx()
drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

# After triggering relocate for discovered apps
drpc_obj.wait_for_progression_status(status=constants.STATUS_RELOCATING, timeout=600)

# In teardown — safe variant when DRPC may already be deleted
drpc_obj.wait_for_progression_status(
    status=constants.STATUS_COMPLETED,
    timeout=300,
    success_if_deleted=True,
)
```

### When to call DRPC wait methods vs dr_helpers wrappers

| Task | Use this |
|---|---|
| Verify `lastGroupSyncTime` + assert age | `dr_helpers.verify_last_group_sync_time(drpc_obj, interval)` |
| Verify `lastKubeObjectProtectionTime` + assert age | `dr_helpers.verify_last_kubeobject_protection_time(drpc_obj, interval)` |
| Wait for DRPC progression = COMPLETED after hub-context action | `drpc_obj.wait_for_progression_status(constants.STATUS_COMPLETED)` |
| Confirm data protection cycle completed | `drpc_obj.wait_for_clusterdataprotected_status()` |
| Confirm DRPC peer ready before next DR action | `drpc_obj.wait_for_peer_ready_status()` |
| Teardown: DRPC may or may not exist | `drpc_obj.wait_for_progression_status(..., success_if_deleted=True)` |
