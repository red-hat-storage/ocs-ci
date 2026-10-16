## CNV Workload

**Key differences from the core skeleton:**

### Additional imports

```python
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
```

### Setup — download virtctl binary first

```python
CNVInstaller().download_and_extract_virtctl_binary()
```

### Workload deployment

```python
cnv_workloads = cnv_dr_workload(
    num_of_vm_subscription=1,
    num_of_vm_appset_push=1,
    num_of_vm_appset_pull=1,
    vm_type=vm_type,   # parametrize: VM_VOLUME_PVC / VM_VOLUME_DV / VM_VOLUME_DVT
)
```

### Data integrity — write files and track MD5 sums

```python
md5sum_original = []
vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

for cnv_wl in cnv_workloads:
    md5sum_original.append(
        run_dd_io(
            vm_obj=cnv_wl.vm_obj,
            file_path=vm_filepaths[0],
            username=cnv_wl.vm_username,
            verify=True,
        )
    )
```

### Failover — use `cnv_workload_placement_name` for non-subscription types

```python
for cnv_wl in cnv_workloads:
    dr_helpers.failover(
        failover_cluster=secondary_cluster_name,
        namespace=cnv_wl.workload_namespace,
        workload_type=cnv_wl.workload_type,
        workload_placement_name=(
            cnv_wl.cnv_workload_placement_name
            if cnv_wl.workload_type != constants.SUBSCRIPTION
            else None
        ),
        skip_odf_cli_validation=primary_cluster_down,
    )
```

### After failover — verify VM is running, then validate data integrity

```python
config.switch_to_cluster_by_name(secondary_cluster_name)
for cnv_wl in cnv_workloads:
    dr_helpers.wait_for_all_resources_creation(
        cnv_wl.workload_pvc_count, cnv_wl.workload_pod_count, cnv_wl.workload_namespace
    )
    dr_helpers.wait_for_cnv_workload(
        vm_name=cnv_wl.vm_name,
        namespace=cnv_wl.workload_namespace,
        phase=constants.STATUS_RUNNING,
    )

validate_data_integrity_vm(cnv_workloads, vm_filepaths[0], md5sum_original, "Failover")
```

### CNV uses RBD only — always call `wait_for_mirroring_status_ok`

```python
dr_helpers.wait_for_mirroring_status_ok(
    replaying_images=sum(cnv_wl.workload_pvc_count for cnv_wl in cnv_workloads)
)
```

### No `DRPC` / `lastGroupSyncTime` tracking for CNV tests

CNV tests verify data integrity via MD5 checksums instead of DRPC sync time.
