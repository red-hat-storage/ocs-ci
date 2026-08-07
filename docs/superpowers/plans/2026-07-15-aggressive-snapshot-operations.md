# Aggressive Snapshot Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `aggressive_snapshot_operation` as a dedicated-loop background stress module that mirrors `aggressive_clone_operation` for VolumeSnapshot create/restore/verify/IO/cleanup.

**Architecture:** New parallel module `aggressive_snapshot_operations.py` with dual pools (`_snapshot_pool`, `_restore_pool`). Wire a second dedicated thread in `background_cluster_operations.py`. Enable via `aggressive_snapshot_config` + `enabled_operations` in both Krkn and resiliency YAML configs.

**Tech Stack:** Python 3, OCS-CI (`PVC.create_snapshot`, `pvc_helpers.create_restore_pvc`, `helpers.wait_for_resource_state`), threading / ThreadPoolExecutor, VdbenchWorkload.

## Global Constraints

- Near 1:1 behavioral mirror of `aggressive_clone_operations.py` (do not refactor clones into a shared base).
- Workload PVCs are read-only sources only (never deleted by this module).
- Dual pools: `_snapshot_pool` and `_restore_pool`.
- Dedicated loop; excluded from shared random-op scheduler.
- Enabled by default in both `krkn_chaos_config.yaml` and `resiliency_tests_config.yaml`.
- Spec: `docs/superpowers/specs/2026-07-15-aggressive-snapshot-operations-design.md`.
- Prefer adapting the existing clone module over inventing new patterns.
- No new pytest files required; verify with import/syntax checks and structural greps.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `ocs_ci/krkn_chaos/aggressive_snapshot_operations.py` | **Create** — `AggressiveSnapshotOperations` manager + all actions |
| `ocs_ci/krkn_chaos/background_cluster_operations.py` | **Modify** — register op, dedicated loop, start/stop/cleanup |
| `conf/ocsci/krkn_chaos_config.yaml` | **Modify** — `aggressive_snapshot_config` + enable op |
| `conf/ocsci/resiliency_tests_config.yaml` | **Modify** — same |
| `ocs_ci/krkn_chaos/README.md` | **Modify** — document operation |

---

### Task 1: Scaffold `AggressiveSnapshotOperations` module

**Files:**
- Create: `ocs_ci/krkn_chaos/aggressive_snapshot_operations.py`
- Reference: `ocs_ci/krkn_chaos/aggressive_clone_operations.py` (full file)

**Interfaces:**
- Consumes: `background_ops` (`BackgroundClusterOperations`) with `_namespace_exists`, `_get_all_workload_pvcs`, `_collect_workload_pvc_refs`, `_can_attach_pod`, `_create_test_pod`, `_verify_pod_data`, `_get_pvc_storage_capacity`, `namespace`, `workloads`
- Produces: `class AggressiveSnapshotOperations` with `run()`, `request_shutdown()`, `is_shutdown_requested()`, `has_tracked_resources()`, `cleanup_all()`, `_get_config() -> Dict[str, Any]`

- [ ] **Step 1: Copy clone module as the starting point**

```bash
cp ocs_ci/krkn_chaos/aggressive_clone_operations.py \
   ocs_ci/krkn_chaos/aggressive_snapshot_operations.py
```

- [ ] **Step 2: Apply module-level renames and defaults**

In `aggressive_snapshot_operations.py`:

1. Update module docstring to describe snapshot/restore stress (not clones).
2. Rename `_DEFAULT_AGGRESSIVE_CLONE_CONFIG` → `_DEFAULT_AGGRESSIVE_SNAPSHOT_CONFIG` with these keys/values:

```python
_DEFAULT_AGGRESSIVE_SNAPSHOT_CONFIG = {
    "max_parallel": 5,
    "large_scale_count": 8,
    "max_snapshot_pool_size": 40,
    "max_restore_pool_size": 20,
    "repeated_snapshot_count": 3,
    "concurrent_create_count": 3,
    "concurrent_delete_count": 2,
    "actions_per_invocation": 3,
    "vdbench_on_restore_elapsed": 120,
    "vdbench_on_restore_interval": 30,
    "max_vdbench_on_restore_parallel": 2,
    "loop_interval": 60,
    "stop_join_timeout": 300,
}
```

3. Rename class `AggressiveCloneOperations` → `AggressiveSnapshotOperations`.
4. In `__init__`, replace `_clone_pool` with dual pools and locks:

```python
self._pool_lock = threading.Lock()
self._vdbench_lock = threading.Lock()
self._snapshot_pool: List[Dict[str, Any]] = []
self._restore_pool: List[Dict[str, Any]] = []
self._active_vdbench_workloads: List[Any] = []
self._shutdown_event = threading.Event()
self._metrics = {"latencies": [], "successes": 0, "failures": 0}
```

5. Update log strings: `"clone"` → `"snapshot"` / `"restore"` where appropriate.
6. `_get_config()` must read `aggressive_snapshot_config` and merge with `_DEFAULT_AGGRESSIVE_SNAPSHOT_CONFIG`.
7. `has_tracked_resources()`:

```python
def has_tracked_resources(self) -> bool:
    with self._vdbench_lock:
        if self._active_vdbench_workloads:
            return True
    with self._pool_lock:
        return bool(self._snapshot_pool or self._restore_pool)
```

- [ ] **Step 3: Rewrite `run()` action lists and selection**

Replace create/pool action lists with:

```python
create_actions = [
    self._single_snapshot,
    self._parallel_snapshots_multi_source,
    self._parallel_snapshots_same_source,
    self._repeated_snapshots,
    self._nested_snapshot,
    self._restore_random_snapshot,
    self._vdbench_on_restore,
    self._vdbench_on_restores_parallel,
    self._concurrent_create_delete,
    self._large_scale_parallel,
]
pool_actions = [
    self._mount_verify_restores,
    self._integrity_verify_restores,
    self._expand_random_restore,
    self._clone_random_restore,
    self._delete_random_snapshots,
    self._delete_random_restores,
    self._cleanup_snapshots,
]
actions = create_actions + pool_actions
```

Selection rules (mirror clones, dual capacity):

```python
with self._pool_lock:
    snap_size = len(self._snapshot_pool)
    restore_size = len(self._restore_pool)

if (
    snap_size >= cfg["max_snapshot_pool_size"]
    or restore_size >= cfg["max_restore_pool_size"]
):
    selected = [self._cleanup_snapshots]
elif snap_size == 0 and workload_pvcs:
    max_actions = min(cfg["actions_per_invocation"], len(actions))
    selected = [random.choice(create_actions)]
    remaining_slots = max_actions - len(selected)
    if remaining_slots > 0:
        selected.extend(
            random.sample(pool_actions, min(remaining_slots, len(pool_actions)))
        )
else:
    count = random.randint(1, min(cfg["actions_per_invocation"], len(actions)))
    selected = random.sample(actions, count)
```

Skip when no workload PVCs **and** both pools empty.

- [ ] **Step 4: Verify module imports**

Run:

```bash
python -c "from ocs_ci.krkn_chaos.aggressive_snapshot_operations import AggressiveSnapshotOperations; print(AggressiveSnapshotOperations)"
```

Expected: prints the class (may still have AttributeError later if old method names remain — fix any leftover `_clone_*` method names in subsequent tasks).

- [ ] **Step 5: Commit**

```bash
git add ocs_ci/krkn_chaos/aggressive_snapshot_operations.py
git commit -m "$(cat <<'EOF'
Add AggressiveSnapshotOperations module scaffold

Start from the aggressive clone module with dual snapshot/restore pools
and snapshot-oriented config defaults.
EOF
)"
```

---

### Task 2: Snapshot create / track / delete helpers and create actions

**Files:**
- Modify: `ocs_ci/krkn_chaos/aggressive_snapshot_operations.py`

**Interfaces:**
- Consumes: `PVC.create_snapshot(wait=True, timeout=180)`, Task 1 scaffold
- Produces:
  - `_create_and_track_snapshot(source_pvc: PVC, is_nested: bool, original_source: Optional[PVC] = None) -> Optional[Any]`
  - `_delete_tracked_snapshot(entry: Dict[str, Any]) -> None`
  - `_create_snapshots_parallel(sources: List[PVC]) -> None`
  - `_pick_random_snapshot_entry() -> Optional[Dict[str, Any]]`
  - Actions: `_single_snapshot`, `_parallel_snapshots_multi_source`, `_parallel_snapshots_same_source`, `_repeated_snapshots`, `_nested_snapshot`, `_large_scale_parallel`

- [ ] **Step 1: Replace clone create helpers with snapshot helpers**

Remove `_get_clone_yaml`, `_create_and_track_clone`, `_delete_tracked_clone`, `_create_clones_parallel`, `_pick_random_pool_entry` (clone version).

Add:

```python
def _pick_random_snapshot_entry(self) -> Optional[Dict[str, Any]]:
    with self._pool_lock:
        if not self._snapshot_pool:
            return None
        return random.choice(self._snapshot_pool)

def _create_and_track_snapshot(
    self,
    source_pvc: PVC,
    is_nested: bool,
    original_source: Optional[PVC] = None,
) -> Optional[Any]:
    start = time.time()
    try:
        source_pvc.reload()
        snapshot_obj = source_pvc.create_snapshot(wait=True, timeout=180)
        latency = time.time() - start
        entry = {
            "snapshot": snapshot_obj,
            "source_pvc": original_source or source_pvc,
            "is_nested": is_nested,
            "latency_seconds": latency,
            "created_at": time.time(),
        }
        with self._pool_lock:
            self._snapshot_pool.append(entry)
        self._metrics["latencies"].append(latency)
        self._metrics["successes"] += 1
        log.info(
            "Created snapshot %s from %s in %.2fs (nested=%s)",
            snapshot_obj.name,
            source_pvc.name,
            latency,
            is_nested,
        )
        return snapshot_obj
    except Exception as err:
        self._metrics["failures"] += 1
        log.error("Failed to create snapshot from %s: %s", source_pvc.name, err)
        return None

def _delete_tracked_snapshot(self, entry: Dict[str, Any]):
    snapshot_obj = entry.get("snapshot")
    if snapshot_obj is None:
        return
    try:
        with suppress(Exception):
            snapshot_obj.reload()
        snapshot_obj.delete()
        snapshot_obj.ocp.wait_for_delete(
            resource_name=snapshot_obj.name, timeout=180
        )
        log.info("Deleted aggressive snapshot %s", snapshot_obj.name)
    except Exception as err:
        log.warning(
            "Failed to delete snapshot %s: %s",
            getattr(snapshot_obj, "name", "?"),
            err,
        )
    finally:
        with self._pool_lock:
            if entry in self._snapshot_pool:
                self._snapshot_pool.remove(entry)

def _create_snapshots_parallel(self, sources: List[PVC]):
    with ThreadPoolExecutor(max_workers=min(len(sources), 8)) as executor:
        futures = [
            executor.submit(self._create_and_track_snapshot, src, False)
            for src in sources
        ]
        for future in as_completed(futures):
            with suppress(Exception):
                future.result()
```

- [ ] **Step 2: Implement create actions**

```python
def _single_snapshot(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
    if not self._require_workload_pvcs("_single_snapshot", workload_pvcs):
        return
    self._create_and_track_snapshot(random.choice(workload_pvcs), is_nested=False)

def _parallel_snapshots_multi_source(self, workload_pvcs, cfg):
    if not self._require_workload_pvcs("_parallel_snapshots_multi_source", workload_pvcs):
        return
    count = min(cfg["max_parallel"], len(workload_pvcs))
    sources = random.sample(workload_pvcs, count)
    self._create_snapshots_parallel(sources)

def _parallel_snapshots_same_source(self, workload_pvcs, cfg):
    if not self._require_workload_pvcs("_parallel_snapshots_same_source", workload_pvcs):
        return
    source = random.choice(workload_pvcs)
    self._create_snapshots_parallel([source] * cfg["max_parallel"])

def _repeated_snapshots(self, workload_pvcs, cfg):
    if not self._require_workload_pvcs("_repeated_snapshots", workload_pvcs):
        return
    source = random.choice(workload_pvcs)
    for _ in range(cfg["repeated_snapshot_count"]):
        self._create_and_track_snapshot(source, is_nested=False)

def _nested_snapshot(self, workload_pvcs, cfg):
    with self._pool_lock:
        restore_entries = list(self._restore_pool)
    if not restore_entries:
        log.info(
            "No restored PVCs for nested snapshot; creating base snapshot+restore first"
        )
        if self._require_workload_pvcs("_nested_snapshot", workload_pvcs):
            snap = self._create_and_track_snapshot(
                random.choice(workload_pvcs), is_nested=False
            )
            if snap:
                self._restore_random_snapshot(workload_pvcs, cfg)
        return
    parent_entry = random.choice(restore_entries)
    parent_pvc = parent_entry["pvc"]
    parent_pvc.reload()
    if parent_pvc.status != constants.STATUS_BOUND:
        log.warning(
            "Parent restore %s not bound, skipping nested snapshot", parent_pvc.name
        )
        return
    self._create_and_track_snapshot(
        parent_pvc,
        is_nested=True,
        original_source=parent_entry.get("source_pvc"),
    )

def _large_scale_parallel(self, workload_pvcs, cfg):
    if not self._require_workload_pvcs("_large_scale_parallel", workload_pvcs):
        return
    count = cfg["large_scale_count"]
    sources = [random.choice(workload_pvcs) for _ in range(count)]
    log.info("Large-scale parallel snapshot creation: %s snapshots", count)
    self._create_snapshots_parallel(sources)
```

Rename `_require_pool_entry` helpers appropriately:
- `_require_snapshot_entry(action_name)` → picks from `_snapshot_pool`
- `_require_restore_entry(action_name)` → picks from `_restore_pool`
- `_require_workload_pvcs` message should say "snapshot sources" not "clone sources"

- [ ] **Step 3: Grep for leftover clone identifiers in create path**

Run:

```bash
rg -n "clone|_clone_pool|_create_and_track_clone" ocs_ci/krkn_chaos/aggressive_snapshot_operations.py
```

Expected: only intentional mentions (e.g. `_clone_random_restore` action name, comments about mirroring clones). Fix accidental leftovers.

- [ ] **Step 4: Commit**

```bash
git add ocs_ci/krkn_chaos/aggressive_snapshot_operations.py
git commit -m "$(cat <<'EOF'
Implement aggressive snapshot create and track helpers

Add parallel/repeated/nested/large-scale snapshot creation into the
snapshot pool.
EOF
)"
```

---

### Task 3: Restore helpers and pool actions

**Files:**
- Modify: `ocs_ci/krkn_chaos/aggressive_snapshot_operations.py`
- Reference: `ocs_ci/krkn_chaos/background_cluster_operations.py` `_snapshot_lifecycle_operation` (restore pattern ~lines 532–568)

**Interfaces:**
- Consumes: `pvc_helpers.create_restore_pvc`, `constants.CSI_RBD_PVC_RESTORE_YAML`, `constants.CSI_CEPHFS_PVC_RESTORE_YAML`, `pvc_helpers.create_pvc_clone` (for `_clone_random_restore`)
- Produces:
  - `_get_restore_yaml(source_pvc: PVC) -> Optional[str]`
  - `_create_and_track_restore(snapshot_entry: Dict[str, Any]) -> Optional[PVC]`
  - `_delete_tracked_restore(entry: Dict[str, Any]) -> None`
  - `_pick_random_restore_entry() -> Optional[Dict[str, Any]]`
  - `_source_capacity(source_pvc: PVC) -> str`
  - Actions listed below

- [ ] **Step 1: Add restore helpers**

```python
def _get_restore_yaml(self, source_pvc: PVC) -> Optional[str]:
    provisioner = (getattr(source_pvc, "provisioner", "") or "").strip().lower()
    if "rbd" in provisioner:
        return constants.CSI_RBD_PVC_RESTORE_YAML
    if "cephfs" in provisioner:
        return constants.CSI_CEPHFS_PVC_RESTORE_YAML
    log.warning("Unsupported provisioner for aggressive restore: %r", provisioner)
    return None

def _source_capacity(self, source_pvc: PVC) -> str:
    source_pvc.reload()
    capacity = (
        source_pvc.data.get("status", {}).get("capacity", {}).get("storage")
    )
    if capacity:
        return capacity
    return (
        source_pvc.data.get("spec", {})
        .get("resources", {})
        .get("requests", {})
        .get("storage", f"{source_pvc.size}Gi")
    )

def _pick_random_restore_entry(self) -> Optional[Dict[str, Any]]:
    with self._pool_lock:
        if not self._restore_pool:
            return None
        return random.choice(self._restore_pool)

def _create_and_track_restore(
    self, snapshot_entry: Dict[str, Any]
) -> Optional[PVC]:
    snapshot_obj = snapshot_entry["snapshot"]
    source_pvc = snapshot_entry["source_pvc"]
    start = time.time()
    try:
        restore_yaml = self._get_restore_yaml(source_pvc)
        if not restore_yaml:
            return None
        capacity = self._source_capacity(source_pvc)
        restored_pvc = pvc_helpers.create_restore_pvc(
            sc_name=snapshot_obj.parent_sc,
            snap_name=snapshot_obj.name,
            namespace=self.namespace,
            size=capacity,
            pvc_name=f"restored-{snapshot_obj.name[:20]}",
            volume_mode=snapshot_obj.parent_volume_mode,
            restore_pvc_yaml=restore_yaml,
            access_mode=snapshot_obj.parent_access_mode,
        )
        helpers.wait_for_resource_state(
            resource=restored_pvc, state=constants.STATUS_BOUND, timeout=300
        )
        latency = time.time() - start
        entry = {
            "pvc": restored_pvc,
            "snapshot": snapshot_obj,
            "source_pvc": source_pvc,
            "latency_seconds": latency,
            "created_at": time.time(),
        }
        with self._pool_lock:
            self._restore_pool.append(entry)
        self._metrics["latencies"].append(latency)
        self._metrics["successes"] += 1
        log.info(
            "Restored PVC %s from snapshot %s in %.2fs",
            restored_pvc.name,
            snapshot_obj.name,
            latency,
        )
        return restored_pvc
    except Exception as err:
        self._metrics["failures"] += 1
        log.error(
            "Failed to restore from snapshot %s: %s",
            getattr(snapshot_obj, "name", "?"),
            err,
        )
        return None

def _delete_tracked_restore(self, entry: Dict[str, Any]):
    restored_pvc = entry.get("pvc")
    if restored_pvc is None:
        return
    try:
        restored_pvc.reload()
        if restored_pvc.is_deleted:
            with self._pool_lock:
                if entry in self._restore_pool:
                    self._restore_pool.remove(entry)
            return
        restored_pvc.delete()
        restored_pvc.ocp.wait_for_delete(
            resource_name=restored_pvc.name, timeout=180
        )
        log.info("Deleted aggressive restored PVC %s", restored_pvc.name)
    except Exception as err:
        log.warning(
            "Failed to delete restored PVC %s: %s",
            getattr(restored_pvc, "name", "?"),
            err,
        )
    finally:
        with self._pool_lock:
            if entry in self._restore_pool:
                self._restore_pool.remove(entry)
```

- [ ] **Step 2: Implement restore / pool actions**

```python
def _restore_random_snapshot(self, workload_pvcs, cfg):
    entry = self._pick_random_snapshot_entry()
    if not entry:
        if workload_pvcs:
            self._create_and_track_snapshot(
                random.choice(workload_pvcs), is_nested=False
            )
            entry = self._pick_random_snapshot_entry()
        if not entry:
            self._log_skip("_restore_random_snapshot", "snapshot pool is empty")
            return
    with self._pool_lock:
        if len(self._restore_pool) >= cfg["max_restore_pool_size"]:
            self._log_skip(
                "_restore_random_snapshot", "restore pool at capacity"
            )
            return
    self._create_and_track_restore(entry)

def _mount_verify_restores(self, workload_pvcs, cfg):
    entry = self._require_restore_entry("_mount_verify_restores")
    if not entry:
        return
    # Same pattern as clone _mount_verify_clones against entry["pvc"]

def _integrity_verify_restores(self, workload_pvcs, cfg):
    entry = self._require_restore_entry("_integrity_verify_restores")
    if not entry:
        return
    self._verify_restore_integrity(entry["source_pvc"], entry["pvc"])
    # Adapt _verify_clone_integrity → _verify_restore_integrity (same checksum logic)

def _expand_random_restore(self, workload_pvcs, cfg):
    entry = self._require_restore_entry("_expand_random_restore")
    if not entry:
        return
    # Same resize pattern as _expand_random_clone on entry["pvc"]

def _clone_random_restore(self, workload_pvcs, cfg):
    """Mirror of clone module's _snapshot_random_clone: clone a restore, then delete."""
    entry = self._require_restore_entry("_clone_random_restore")
    if not entry:
        return
    restored_pvc = entry["pvc"]
    clone_pvc = None
    try:
        restored_pvc.reload()
        provisioner = (getattr(restored_pvc, "provisioner", "") or "").strip().lower()
        if "rbd" in provisioner:
            clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        elif "cephfs" in provisioner:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
        else:
            self._log_skip("_clone_random_restore", f"unsupported provisioner {provisioner!r}")
            return
        capacity = self.bg_ops._get_pvc_storage_capacity(restored_pvc)
        clone_pvc = pvc_helpers.create_pvc_clone(
            sc_name=restored_pvc.backed_sc,
            parent_pvc=restored_pvc.name,
            clone_yaml=clone_yaml,
            namespace=self.namespace,
            storage_size=capacity,
            access_mode=restored_pvc.get_pvc_access_mode,
            volume_mode=restored_pvc.get()["spec"]["volumeMode"],
        )
        helpers.wait_for_resource_state(
            resource=clone_pvc, state=constants.STATUS_BOUND, timeout=300
        )
        log.info(
            "Created clone %s from restored PVC %s",
            clone_pvc.name,
            restored_pvc.name,
        )
    except Exception as err:
        log.warning("Clone of restore %s failed: %s", restored_pvc.name, err)
    finally:
        with suppress(Exception):
            if clone_pvc:
                clone_pvc.delete()
                clone_pvc.ocp.wait_for_delete(
                    resource_name=clone_pvc.name, timeout=180
                )

def _delete_random_snapshots(self, workload_pvcs, cfg):
    with self._pool_lock:
        if not self._snapshot_pool:
            self._log_skip("_delete_random_snapshots", "snapshot pool is empty")
            return
        count = min(cfg["concurrent_delete_count"], len(self._snapshot_pool))
        entries = random.sample(self._snapshot_pool, count)
    for entry in entries:
        self._delete_tracked_snapshot(entry)

def _delete_random_restores(self, workload_pvcs, cfg):
    with self._pool_lock:
        if not self._restore_pool:
            self._log_skip("_delete_random_restores", "restore pool is empty")
            return
        count = min(cfg["concurrent_delete_count"], len(self._restore_pool))
        entries = random.sample(self._restore_pool, count)
    for entry in entries:
        self._delete_tracked_restore(entry)

def _concurrent_create_delete(self, workload_pvcs, cfg):
    if not self._require_workload_pvcs("_concurrent_create_delete", workload_pvcs):
        return

    def create_task():
        self._create_and_track_snapshot(random.choice(workload_pvcs), is_nested=False)

    def delete_task():
        # Prefer deleting a restore if present, else a snapshot
        restore_entry = self._pick_random_restore_entry()
        if restore_entry:
            self._delete_tracked_restore(restore_entry)
            return
        snap_entry = self._pick_random_snapshot_entry()
        if snap_entry:
            self._delete_tracked_snapshot(snap_entry)

    # Same ThreadPoolExecutor fan-out as clones using concurrent_* counts

def _cleanup_snapshots(self, workload_pvcs, cfg):
    with self._pool_lock:
        restores = list(self._restore_pool)
        snaps = list(self._snapshot_pool)
    log.info(
        "Cleaning up %s restores and %s snapshots", len(restores), len(snaps)
    )
    for entry in restores:
        self._delete_tracked_restore(entry)
    for entry in snaps:
        self._delete_tracked_snapshot(entry)
    self._verify_pool_reclamation()

def _verify_pool_reclamation(self):
    with self._pool_lock:
        remaining_snaps = [
            e["snapshot"].name for e in self._snapshot_pool if e.get("snapshot")
        ]
        remaining_restores = [
            e["pvc"].name for e in self._restore_pool if e.get("pvc")
        ]
    if remaining_snaps or remaining_restores:
        log.warning(
            "Pools not fully reclaimed: snapshots=%s restores=%s",
            remaining_snaps,
            remaining_restores,
        )
    else:
        log.info("All aggressive snapshot/restore resources reclaimed")
```

Fully port `_verify_clone_integrity` → `_verify_restore_integrity` (same md5 logic; rename log messages).

- [ ] **Step 3: Implement `cleanup_all` and orphan sweep**

```python
def cleanup_all(self):
    self.request_shutdown()
    # stop vdbench workloads (same as clones)
    with self._pool_lock:
        restores = list(self._restore_pool)
        snaps = list(self._snapshot_pool)
    for entry in restores:
        self._delete_tracked_restore(entry)
    for entry in snaps:
        self._delete_tracked_snapshot(entry)
    self._cleanup_orphan_aggressive_resources()
    self._verify_pool_reclamation()
```

Orphan sweep targets:
- PVCs whose names start with `restored-`
- VolumeSnapshot resources created in the namespace (best-effort: delete snapshots that are not workload-owned; prefer deleting by listing `VolumeSnapshot` kind and removing those still tracked / orphaned after pool clear — at minimum delete any remaining pool names, then sweep `restored-*` PVCs and `test-pod-*` attached to them)
- Deployments `vdbench-workload-*` whose PVC claim starts with `restored-`

Use `OCP(kind="VolumeSnapshot", namespace=...)` and `OCP(kind="PersistentVolumeClaim", ...)` like the clone orphan sweep.

- [ ] **Step 4: Update `_log_metrics` to report both pool sizes**

```python
log.info(
    "Aggressive snapshot metrics: successes=%s failures=%s "
    "avg_latency=%.2fs success_rate=%.1f%% snapshot_pool=%s restore_pool=%s",
    ...
)
```

- [ ] **Step 5: Verify method surface**

Run:

```bash
python - <<'PY'
from ocs_ci.krkn_chaos.aggressive_snapshot_operations import AggressiveSnapshotOperations
required = [
    "run", "request_shutdown", "cleanup_all", "has_tracked_resources",
    "_single_snapshot", "_restore_random_snapshot", "_mount_verify_restores",
    "_integrity_verify_restores", "_vdbench_on_restore", "_cleanup_snapshots",
    "_clone_random_restore", "_nested_snapshot",
]
missing = [m for m in required if not hasattr(AggressiveSnapshotOperations, m)]
assert not missing, missing
print("OK", len(required), "methods present")
PY
```

Expected: `OK 12 methods present`

- [ ] **Step 6: Commit**

```bash
git add ocs_ci/krkn_chaos/aggressive_snapshot_operations.py
git commit -m "$(cat <<'EOF'
Add restore pool actions for aggressive snapshot ops

Implement restore, verify, expand, clone-from-restore, delete, and
cleanup paths with orphan reclamation.
EOF
)"
```

---

### Task 4: Vdbench on restored PVCs

**Files:**
- Modify: `ocs_ci/krkn_chaos/aggressive_snapshot_operations.py`

**Interfaces:**
- Consumes: `VdbenchWorkload`, `create_temp_config_file`, restore pool helpers from Task 3
- Produces: `_vdbench_on_restore`, `_vdbench_on_restores_parallel`, `_get_or_create_restore_for_io`, `_get_or_create_restores_for_io`, `_run_vdbench_on_pvc`, `_create_vdbench_config_file` using `vdbench_on_restore_elapsed` / `vdbench_on_restore_interval`

- [ ] **Step 1: Adapt Vdbench helpers from clone naming**

Rename config keys in `_create_vdbench_config_file` / `_run_vdbench_on_pvc`:
- `vdbench_on_clone_elapsed` → `vdbench_on_restore_elapsed`
- `vdbench_on_clone_interval` → `vdbench_on_restore_interval`
- `max_vdbench_on_clone_parallel` → `max_vdbench_on_restore_parallel`

Replace `_get_or_create_clone_for_io` with:

```python
def _get_or_create_restore_for_io(self, workload_pvcs, action_name):
    entry = self._pick_random_restore_entry()
    if entry:
        return entry["pvc"]
    # ensure a snapshot exists, then restore
    snap_entry = self._pick_random_snapshot_entry()
    if not snap_entry and workload_pvcs:
        self._create_and_track_snapshot(random.choice(workload_pvcs), False)
        snap_entry = self._pick_random_snapshot_entry()
    if not snap_entry:
        self._log_skip(action_name, "no snapshot available to restore for IO")
        return None
    return self._create_and_track_restore(snap_entry)
```

Parallel variant fills up to `count` restores similarly.

Pattern names inside generated configs may say `restore_random_write` instead of `clone_random_write`.

- [ ] **Step 2: Import check after Vdbench port**

```bash
python -c "from ocs_ci.krkn_chaos import aggressive_snapshot_operations as m; print('vdbench_on_restore' in dir(m.AggressiveSnapshotOperations))"
```

Expected: `True`

- [ ] **Step 3: Commit**

```bash
git add ocs_ci/krkn_chaos/aggressive_snapshot_operations.py
git commit -m "$(cat <<'EOF'
Add Vdbench IO stress on aggressive snapshot restores

Run short single and parallel Vdbench workloads against restored PVCs.
EOF
)"
```

---

### Task 5: Wire dedicated loop into `BackgroundClusterOperations`

**Files:**
- Modify: `ocs_ci/krkn_chaos/background_cluster_operations.py`

**Interfaces:**
- Consumes: `AggressiveSnapshotOperations` from Task 1–4
- Produces: dedicated loop lifecycle for `aggressive_snapshot_operation`

- [ ] **Step 1: Register PVC dependency and operation map**

In `PVC_DEPENDENT_BACKGROUND_OPERATIONS` frozenset, add:

```python
"aggressive_snapshot_operation",
```

In `available_operations`, add:

```python
"aggressive_snapshot_operation": self._aggressive_snapshot_operation,
```

- [ ] **Step 2: Init / start / stop / random filter**

Alongside clone fields in `__init__`:

```python
self._aggressive_snapshot_ops = None
self._aggressive_snapshot_thread: Optional[threading.Thread] = None
```

```python
self._aggressive_snapshot_enabled = (
    "aggressive_snapshot_operation" in self.enabled_operations
)
self._random_operations = {
    name: func
    for name, func in self.enabled_operations.items()
    if name
    not in ("aggressive_clone_operation", "aggressive_snapshot_operation")
}
```

Log when snapshot dedicated loop is enabled (mirror clone log).

In `start()`, after clone thread start:

```python
if self._aggressive_snapshot_enabled:
    self._aggressive_snapshot_thread = threading.Thread(
        target=self._aggressive_snapshot_loop,
        name="AggressiveSnapshotOperationLoop",
        daemon=True,
    )
    self._aggressive_snapshot_thread.start()
    log.info("Aggressive snapshot operation loop started")
```

In `stop()`:
1. Early-return guard must also consider snapshot thread alive / `has_tracked_resources()`.
2. Call `self._aggressive_snapshot_ops.request_shutdown()` when ops exist (alongside clone).
3. Join `_aggressive_snapshot_thread` with `_get_aggressive_snapshot_stop_join_timeout()`.
4. On cleanup, call `self._aggressive_snapshot_ops.cleanup_all()`.
5. Set `_aggressive_snapshot_thread = None`.

- [ ] **Step 3: Add loop + helpers + operation entrypoint**

Add methods mirroring clone (place near clone section ~line 743):

```python
def _aggressive_snapshot_loop(self):
    """Continuously run aggressive snapshot operations in a dedicated loop."""
    log.info("Aggressive snapshot operation loop started")
    interval = self._get_aggressive_snapshot_loop_interval()
    while self._running:
        try:
            if not self._namespace_exists():
                break
            if (
                self._aggressive_snapshot_ops is not None
                and self._aggressive_snapshot_ops.is_shutdown_requested()
            ):
                break
            self._run_operation_safe(
                "aggressive_snapshot_operation",
                self._aggressive_snapshot_operation,
            )
            if not self._running or (
                self._aggressive_snapshot_ops is not None
                and self._aggressive_snapshot_ops.is_shutdown_requested()
            ):
                break
            if not self._sleep_interruptible_aggressive_snapshot_loop(interval):
                break
        except Exception as e:
            log.error(f"Error in aggressive snapshot operation loop: {e}")
            if not self._sleep_interruptible_aggressive_snapshot_loop(10):
                break
    log.info("Aggressive snapshot operation loop stopped")

def _sleep_interruptible_aggressive_snapshot_loop(self, seconds: float) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline and self._running:
        if (
            self._aggressive_snapshot_ops is not None
            and self._aggressive_snapshot_ops.is_shutdown_requested()
        ):
            return False
        time.sleep(min(5, max(0, deadline - time.time())))
    return self._running

def _get_aggressive_snapshot_stop_join_timeout(self) -> int:
    try:
        if self._aggressive_snapshot_ops is None:
            from ocs_ci.krkn_chaos.aggressive_snapshot_operations import (
                AggressiveSnapshotOperations,
            )
            self._aggressive_snapshot_ops = AggressiveSnapshotOperations(self)
        return int(
            self._aggressive_snapshot_ops._get_config().get(
                "stop_join_timeout", 300
            )
        )
    except Exception:
        return 300

def _get_aggressive_snapshot_loop_interval(self) -> int:
    try:
        if self._aggressive_snapshot_ops is None:
            from ocs_ci.krkn_chaos.aggressive_snapshot_operations import (
                AggressiveSnapshotOperations,
            )
            self._aggressive_snapshot_ops = AggressiveSnapshotOperations(self)
        return self._aggressive_snapshot_ops._get_config().get(
            "loop_interval", self.operation_interval
        )
    except Exception:
        return self.operation_interval

def _aggressive_snapshot_operation(self):
    """Run aggressive PVC snapshot stress operations."""
    if self._aggressive_snapshot_ops is None:
        from ocs_ci.krkn_chaos.aggressive_snapshot_operations import (
            AggressiveSnapshotOperations,
        )
        self._aggressive_snapshot_ops = AggressiveSnapshotOperations(self)
    self._aggressive_snapshot_ops.run()
```

Do **not** overload `_sleep_interruptible_aggressive_loop` (clone) — keep a separate snapshot sleeper.

- [ ] **Step 4: Verify wiring**

Run:

```bash
python - <<'PY'
from ocs_ci.krkn_chaos.background_cluster_operations import (
    BackgroundClusterOperations,
    PVC_DEPENDENT_BACKGROUND_OPERATIONS,
)
assert "aggressive_snapshot_operation" in PVC_DEPENDENT_BACKGROUND_OPERATIONS
assert hasattr(BackgroundClusterOperations, "_aggressive_snapshot_operation")
assert hasattr(BackgroundClusterOperations, "_aggressive_snapshot_loop")
print("wiring OK")
PY
```

Expected: `wiring OK`

- [ ] **Step 5: Commit**

```bash
git add ocs_ci/krkn_chaos/background_cluster_operations.py
git commit -m "$(cat <<'EOF'
Wire aggressive_snapshot_operation dedicated background loop

Register the snapshot stress op, exclude it from the random scheduler,
and manage start/stop/cleanup alongside aggressive clones.
EOF
)"
```

---

### Task 6: Config YAML + README

**Files:**
- Modify: `conf/ocsci/krkn_chaos_config.yaml`
- Modify: `conf/ocsci/resiliency_tests_config.yaml`
- Modify: `ocs_ci/krkn_chaos/README.md`
- Optional: `docs/superpowers/specs/2026-07-15-aggressive-snapshot-operations-design.md` (include in same commit if still uncommitted)

- [ ] **Step 1: Add config blocks (both YAML files)**

Immediately after `aggressive_clone_config`, add:

```yaml
      # Aggressive snapshot operation tuning (optional)
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
        loop_interval: 60  # Seconds between aggressive snapshot loop iterations
        stop_join_timeout: 300  # Max wait for aggressive snapshot loop to exit on teardown
```

In `enabled_operations`, add (next to aggressive clone):

```yaml
        - aggressive_snapshot_operation  # Parallel/nested snapshot+restore stress testing
```

- [ ] **Step 2: Update README operations table**

In `ocs_ci/krkn_chaos/README.md` Supported Operations table, add row after aggressive clone:

```markdown
| `aggressive_snapshot_operation` | Parallel/nested snapshot+restore stress, Vdbench on restores, integrity, scale (dedicated loop) | Aggressive snapshot validation |
```

- [ ] **Step 3: Sanity-check YAML parses**

```bash
python - <<'PY'
import yaml
for path in (
    "conf/ocsci/krkn_chaos_config.yaml",
    "conf/ocsci/resiliency_tests_config.yaml",
):
    with open(path) as f:
        data = yaml.safe_load(f)
    bg = data["ENV_DATA"]
    key = "krkn_config" if "krkn" in path else "resiliency_config"
    ops = bg[key]["background_cluster_operations"]
    assert "aggressive_snapshot_config" in ops
    assert "aggressive_snapshot_operation" in ops["enabled_operations"]
    print(path, "OK")
PY
```

Expected: both files print `OK`.

- [ ] **Step 4: Commit**

```bash
git add conf/ocsci/krkn_chaos_config.yaml \
        conf/ocsci/resiliency_tests_config.yaml \
        ocs_ci/krkn_chaos/README.md \
        docs/superpowers/specs/2026-07-15-aggressive-snapshot-operations-design.md \
        docs/superpowers/plans/2026-07-15-aggressive-snapshot-operations.md
git commit -m "$(cat <<'EOF'
Enable aggressive_snapshot_operation in Krkn and resiliency configs

Add tuning knobs, default enablement, README docs, and design/plan
artifacts for the snapshot stress background operation.
EOF
)"
```

---

## Spec Coverage Checklist

| Spec requirement | Task |
|------------------|------|
| Parallel module `aggressive_snapshot_operations.py` | 1–4 |
| Dual pools | 1, 3 |
| Dedicated loop + exclude from random scheduler | 5 |
| Create action catalog | 2 |
| Restore/verify/expand/clone/delete/cleanup | 3 |
| Vdbench on restores | 4 |
| Config in both YAMLs + enabled by default | 6 |
| README | 6 |
| PVC_DEPENDENT registration | 5 |
| Shutdown / cleanup_all / orphan sweep | 3, 5 |
| Workload PVCs untouched | 2–3 (no delete of workload PVCs) |

## Plan Self-Review Notes

- No TBD/placeholder steps remain.
- Method names are consistent across tasks (`_snapshot_pool` / `_restore_pool`, `vdbench_on_restore_*`).
- Clone loop sleeper intentionally left separate from snapshot sleeper to avoid cross-op shutdown coupling.
