## Discovered Apps Workload
<!-- v1.1.0 — added use_statefulsets subsection -->

**Key differences from the core skeleton:**

### Workload deployment

```python
rdr_workloads = discovered_apps_dr_workload(
    pvc_interface=pvc_interface, kubeobject=1, recipe=1
)
first_workload = rdr_workloads[0]
```

### DRPC construction — different namespace and resource_name

```python
drpc_objs = [
    DRPC(
        namespace=constants.DR_OPS_NAMESPACE,
        resource_name=wl.discovered_apps_placement_name,
    )
    for wl in rdr_workloads
]
```

### Cluster identification — pass `discovered_apps=True`

```python
primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
    first_workload.workload_namespace,
    discovered_apps=True,
    resource_name=first_workload.discovered_apps_placement_name,
)
secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
    first_workload.workload_namespace,
    discovered_apps=True,
    resource_name=first_workload.discovered_apps_placement_name,
)
scheduling_interval = dr_helpers.get_scheduling_interval(
    first_workload.workload_namespace,
    discovered_apps=True,
    resource_name=first_workload.discovered_apps_placement_name,
)
```

### Sync checks BEFORE failover — both group sync AND kubeobject protection

> Discovered-apps tests check **both** time fields before triggering any DR action.
> Check them after the initial IO wait, before stopping nodes or triggering failover.

```python
wait_time = 2 * scheduling_interval  # minutes
logger.info(f"Waiting for {wait_time} minutes to run IOs")
sleep(wait_time * 60)

logger.info("Checking for lastKubeObjectProtectionTime before failover")
for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
    dr_helpers.verify_last_kubeobject_protection_time(
        drpc_obj, rdr_workload.kubeobject_capture_interval_int
    )

logger.info("Checking for lastGroupSyncTime before failover")
for drpc_obj in drpc_objs:
    dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
```

### Sync checks BEFORE relocate — both fields again

```python
logger.info("Checking for lastKubeObjectProtectionTime before relocate")
for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
    dr_helpers.verify_last_kubeobject_protection_time(
        drpc_obj, rdr_workload.kubeobject_capture_interval_int
    )

logger.info("Checking for lastGroupSyncTime before relocate")
for drpc_obj in drpc_objs:
    dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
```

### Failover — pass `discovered_apps=True` and `old_primary`

```python
for rdr_workload in rdr_workloads:
    dr_helpers.failover(
        failover_cluster=secondary_cluster_name,
        namespace=rdr_workload.workload_namespace,
        discovered_apps=True,
        workload_placement_name=rdr_workload.discovered_apps_placement_name,
        old_primary=primary_cluster_name,
        skip_odf_cli_validation=primary_cluster_down,
    )
```

### Post-failover cleanup — mandatory for discovered apps

```python
for rdr_workload in rdr_workloads:
    dr_helpers.do_discovered_apps_cleanup(
        drpc_name=rdr_workload.discovered_apps_placement_name,
        old_primary=primary_cluster_name,
        workload_namespace=rdr_workload.workload_namespace,
        workload_dir=rdr_workload.workload_dir,
        vrg_name=rdr_workload.discovered_apps_placement_name,
    )
```

### Resource creation verification — extra params required

```python
config.switch_to_cluster_by_name(secondary_cluster_name)
for rdr_workload in rdr_workloads:
    dr_helpers.wait_for_all_resources_creation(
        rdr_workload.workload_pvc_count,
        rdr_workload.workload_pod_count,
        rdr_workload.workload_namespace,
        timeout=1200,
        discovered_apps=True,
        vrg_name=rdr_workload.discovered_apps_placement_name,
        performed_dr_action=True,
    )

if pvc_interface == constants.CEPHBLOCKPOOL:
    dr_helpers.wait_for_mirroring_status_ok(
        replaying_images=sum(wl.workload_pvc_count for wl in rdr_workloads)
    )
```

### Relocate — pass `discovered_apps=True` and `old_primary`

```python
for rdr_workload in rdr_workloads:
    dr_helpers.relocate(
        preferred_cluster=primary_cluster_name,
        namespace=rdr_workload.workload_namespace,
        workload_placement_name=rdr_workload.discovered_apps_placement_name,
        discovered_apps=True,
        old_primary=secondary_cluster_name,
        workload_instance=rdr_workload,
    )
```

### Sync checks AFTER relocate — both fields one final time

```python
logger.info("Checking for lastKubeObjectProtectionTime post relocate")
for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
    dr_helpers.verify_last_kubeobject_protection_time(
        drpc_obj, rdr_workload.kubeobject_capture_interval_int
    )
```

---

### StatefulSet variant — `use_statefulsets=True`

Pass `use_statefulsets=True` to the factory to deploy the StatefulSet-flavoured busybox workload
instead of the default Deployment-based one.
Only valid for the default busybox path (`workloads=None`). Has no effect when `workloads` is
`"filebrowser"` or `"mongodb"` — those have no StatefulSet variants.

```python
rdr_workloads = discovered_apps_dr_workload(
    pvc_interface=pvc_interface,
    kubeobject=1,
    recipe=1,
    use_statefulsets=True,          # ← selects StatefulSet config key
)
```

**Config keys routed to:**

| `pvc_interface`         | `use_statefulsets` | Config key                                          |
|-------------------------|--------------------|-----------------------------------------------------|
| `constants.CEPHBLOCKPOOL` | `False` (default) | `dr_workload_discovered_apps_rbd`                   |
| `constants.CEPHBLOCKPOOL` | `True`             | `dr_workload_discovered_apps_rbd_statefulsets`      |
| `constants.CEPHFILESYSTEM` | `False` (default) | `dr_workload_discovered_apps_cephfs`               |
| `constants.CEPHFILESYSTEM` | `True`             | `dr_workload_discovered_apps_cephfs_statefulsets`  |

The workload namespace suffix, DRPC construction, sync checks, failover, cleanup, and relocate
patterns are **identical** to the regular discovered-apps flow — nothing else changes.

### OCS ≥ 4.22 — ApplicationCleanupPending alert verification

```python
from ocs_ci.utility.version import get_semantic_ocs_version_from_config
from semantic_version import Version
from ocs_ci.helpers.dr_helpers_ui import (
    verify_pending_cleanup_alert_firing,
    verify_pending_cleanup_alert_resolved,
)

if get_semantic_ocs_version_from_config() >= Version("4.22", partial=True):
    wait_time_for_alert = constants.ALERT_APPLICATION_CLEANUP_PENDING_THRESHOLD + 180
    sleep(wait_time_for_alert)
    acm_obj = AcmAddClusters()
    for rdr_workload in rdr_workloads:
        verify_pending_cleanup_alert_firing(
            acm_obj, "Failover",
            drpc_name=rdr_workload.discovered_apps_placement_name,
        )
    for rdr_workload in rdr_workloads:
        verify_pending_cleanup_alert_resolved(
            acm_obj, "Failover",
            drpc_name=rdr_workload.discovered_apps_placement_name,
        )
```
