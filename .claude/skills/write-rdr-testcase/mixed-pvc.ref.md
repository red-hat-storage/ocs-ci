## Mixed PVC Interface Tests (RBD + CephFS simultaneously)

Deploy **two separate workload groups** in one test — one for RBD and one for CephFS.
Used in `test_failover_after_multiple_pods_failure.py` and `test_site_failure_recovery_and_failover.py`.

```python
rbd_workloads = dr_workload(
    num_of_subscription=1, num_of_appset=1,
    pvc_interface=constants.CEPHBLOCKPOOL,
)
cephfs_workloads = dr_workload(
    num_of_subscription=1, num_of_appset=1,
    pvc_interface=constants.CEPHFILESYSTEM,
)
all_workloads = rbd_workloads + cephfs_workloads
```

### Per-workload interface dispatch

```python
# CephFS pre-check on secondary
config.switch_to_cluster_by_name(secondary_cluster_name)
for wl in all_workloads:
    if wl.pvc_interface == constants.CEPHFILESYSTEM:
        dr_helpers.wait_for_replication_destinations_creation(
            wl.workload_pvc_count, wl.workload_namespace
        )

# Post-action: mirroring check counts only RBD PVCs
dr_helpers.wait_for_mirroring_status_ok(
    replaying_images=sum(
        wl.workload_pvc_count for wl in all_workloads
        if wl.pvc_interface == constants.CEPHBLOCKPOOL
    )
)
```

> Tier for these tests is usually `@tier4` + `@tier4c`.
