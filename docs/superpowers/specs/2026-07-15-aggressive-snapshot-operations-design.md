# Aggressive Snapshot Operations Design

**Date:** 2026-07-15
**Status:** Approved for implementation planning
**Related:** `ocs_ci/krkn_chaos/aggressive_clone_operations.py`

## Goal

Add `aggressive_snapshot_operation` as a near 1:1 counterpart to `aggressive_clone_operation`: dedicated-loop background stress for PVC VolumeSnapshot create/restore/verify/IO/cleanup while leaving running workload PVCs untouched (read-only sources only).

## Approach

**Parallel module** (not shared base class, not folded into the clone file):

- New: `ocs_ci/krkn_chaos/aggressive_snapshot_operations.py`
- Class: `AggressiveSnapshotOperations`
- Wire a second dedicated loop in `background_cluster_operations.py`, mirroring the clone loop pattern

## Architecture & Wiring

### Module contract

Same public lifecycle as clones:

| Method | Purpose |
|--------|---------|
| `run()` | Entry point for one loop iteration |
| `request_shutdown()` / `is_shutdown_requested()` | Cooperative stop |
| `has_tracked_resources()` | True if snapshot pool, restore pool, or active Vdbench remains |
| `cleanup_all()` | Stop Vdbench, delete restores, delete snapshots, orphan sweep |

### Background ops integration

In `background_cluster_operations.py`:

1. Add `"aggressive_snapshot_operation"` to `PVC_DEPENDENT_BACKGROUND_OPERATIONS`
2. Register in the operation map → `_aggressive_snapshot_operation`
3. When enabled: start dedicated thread `_aggressive_snapshot_loop` (excluded from shared random-op loop)
4. On stop: `request_shutdown` → join with `stop_join_timeout` → `cleanup_all`
5. Lazily construct `AggressiveSnapshotOperations(self)` like clones

### Config

Add to both `conf/ocsci/krkn_chaos_config.yaml` and `conf/ocsci/resiliency_tests_config.yaml`:

```yaml
aggressive_snapshot_config:
  max_parallel: 5
  large_scale_count: 8
  max_snapshot_pool_size: 40
  max_restore_pool_size: 20
  repeated_snapshot_count: 3
  concurrent_create_count: 3
  concurrent_delete_count: 2
  actions_per_invocation: 3
  vdbench_on_restore_elapsed: 120
  vdbench_on_restore_interval: 30
  max_vdbench_on_restore_parallel: 2
  loop_interval: 60
  stop_join_timeout: 300

enabled_operations:
  - aggressive_snapshot_operation  # enabled by default
```

Defaults live in the module as `_DEFAULT_AGGRESSIVE_SNAPSHOT_CONFIG` and merge with user config (same pattern as clones).

Also document the op in `ocs_ci/krkn_chaos/README.md`.

## Dual Pools

Thread-safe pools:

### `_snapshot_pool`

Each entry:

```python
{
    "snapshot": snapshot_obj,
    "source_pvc": source_pvc,      # original workload PVC (or restore parent for nested)
    "is_nested": bool,
    "latency_seconds": float,
    "created_at": float,
}
```

### `_restore_pool`

Each entry:

```python
{
    "pvc": restored_pvc,
    "snapshot": snapshot_obj,
    "source_pvc": source_pvc,
    "latency_seconds": float,
    "created_at": float,
}
```

Workload PVCs are never deleted or mutated by this module (except being snapshotted).

## Action Catalog

### Create / grow actions

| Action | Behavior |
|--------|----------|
| `_single_snapshot` | Snapshot one workload PVC; track in `_snapshot_pool` |
| `_parallel_snapshots_multi_source` | Parallel snaps from different workload PVCs |
| `_parallel_snapshots_same_source` | Parallel snaps from one PVC |
| `_repeated_snapshots` | `repeated_snapshot_count` snaps from one PVC |
| `_nested_snapshot` | Snapshot a restored PVC; if restore pool empty, create base snap+restore first |
| `_restore_random_snapshot` | Restore a random snapshot into `_restore_pool` |
| `_vdbench_on_restore` | Short Vdbench on one restored PVC (create restore if needed) |
| `_vdbench_on_restores_parallel` | Concurrent Vdbench on up to `max_vdbench_on_restore_parallel` restores |
| `_concurrent_create_delete` | Parallel snapshot creates + snapshot/restore deletes |
| `_large_scale_parallel` | Create `large_scale_count` snapshots in parallel |

### Pool actions

| Action | Behavior |
|--------|----------|
| `_mount_verify_restores` | Attach test pod to restored PVC; basic verify |
| `_integrity_verify_restores` | Checksum compare restore vs source (clone-style) |
| `_expand_random_restore` | Resize a restored PVC |
| `_clone_random_restore` | Clone a restored PVC then delete the clone (mirror of clone’s `_snapshot_random_clone`) |
| `_delete_random_snapshots` | Delete up to `concurrent_delete_count` tracked snapshots |
| `_delete_random_restores` | Delete up to `concurrent_delete_count` tracked restores |
| `_cleanup_snapshots` | Delete all tracked restores then all tracked snapshots |

### Action selection

Mirror clone logic:

1. If snapshot pool at `max_snapshot_pool_size` **or** restore pool at `max_restore_pool_size` → prioritize `_cleanup_snapshots`
2. Else if snapshot pool empty and workload PVCs exist → force at least one create action, then optionally sample pool actions
3. Else → random sample of `actions_per_invocation` from the full action list
4. Respect `_shutdown_event` between actions

## Data Flow (`run()`)

1. Skip if shutdown requested or namespace missing
2. Load config; collect workload PVCs; log context summary
3. Skip if no workload PVCs **and** both pools empty
4. Select and execute actions; catch per-action exceptions (log + failure metric)
5. Log metrics (successes, failures, avg snapshot/restore latency, pool sizes)

## Create / Restore Helpers

Reuse existing helpers already used by `snapshot_lifecycle`:

- Create: `workload_pvc.create_snapshot(wait=True, timeout=180)`
- Restore: `pvc_helpers.create_restore_pvc(...)` with source capacity ≥ snapshot size, same SC / volume mode / access mode
- Wait Bound on restored PVCs before tracking

Parallel create uses `ThreadPoolExecutor` like clones.

## Shutdown, Cleanup, Orphans

1. `request_shutdown()` sets the event; Vdbench waits use interruptible sleep chunks
2. `cleanup_all()`:
   - Stop/cleanup active Vdbench workloads
   - Delete all `_restore_pool` entries
   - Delete all `_snapshot_pool` entries
   - Orphan sweep for leftover restore PVCs, VolumeSnapshots, test pods, and Vdbench deployments tied to restores
3. Verify pools are empty after cleanup

## Error Handling

- Missing namespace / no PVC sources → skip with clear logs
- Unsupported / attach failures → skip individual verifies (same as clones)
- Create/restore helpers return `None` on failure and increment failure metrics
- Teardown deletes use `contextlib.suppress` so cleanup always progresses

## Out of Scope

- Refactoring clone ops into a shared base class
- New unit/integration test files (validate via existing Krkn/resiliency runs with the op enabled)
- Changing behavior of existing `snapshot_lifecycle` or `longevity_operations`

## Files Touched

| File | Change |
|------|--------|
| `ocs_ci/krkn_chaos/aggressive_snapshot_operations.py` | **New** module |
| `ocs_ci/krkn_chaos/background_cluster_operations.py` | Dedicated loop + wiring |
| `conf/ocsci/krkn_chaos_config.yaml` | Config + enable op |
| `conf/ocsci/resiliency_tests_config.yaml` | Config + enable op |
| `ocs_ci/krkn_chaos/README.md` | Document the operation |

## Success Criteria

- Enabling `aggressive_snapshot_operation` starts a dedicated loop that stresses snapshot/restore without touching workload PVC lifecycle
- Teardown drains tracked resources and joins the loop within `stop_join_timeout`
- Behavior and knobs are recognizably parallel to `aggressive_clone_operation`
