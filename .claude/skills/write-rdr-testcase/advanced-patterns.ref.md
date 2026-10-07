## Advanced Patterns

### Structured step logging with `logger.test_step`

Use `logger.test_step` for any test with ≥6 major phases (especially disruptive or CNV tests):

```python
logger.test_step("Deploy GitOps/ApplicationSet workload")
logger.test_step("Write initial data to VMs and record checksums")
logger.test_step(f"Failover to secondary cluster {secondary_cluster_name}")
logger.test_step("Validate data integrity after failover")
logger.test_step("Recover the primary managed cluster")
logger.test_step("Relocate workloads back to original primary cluster")
logger.test_step("Validate data integrity after relocate")
```

Use plain `logger.info` for minor intermediate status within a phase.

---

### Waiting for DRPC progression to complete

After a failover/relocate that goes through the ACM hub context (discovered-apps, CNV discovered tests):

```python
config.switch_acm_ctx()
drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
```

> Only needed when driving DR actions through the hub context. Standard CLI subscription/appset tests do not need this — `failover()` and `relocate()` already poll for completion internally.

---

### Class-level parametrize (single-method classes)

```python
@rdr
@tier1
@turquoise_squad
@pytest.mark.parametrize(
    argnames=["pvc_interface"],
    argvalues=[
        pytest.param(*[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-4772")),
        pytest.param(*[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-4735")),
    ],
)
class TestSequentialRelocate:
    def test_sequential_relocate_to_secondary(self, pvc_interface, dr_workload):
        ...
```

> Use when ALL methods in the class share the same parametrize axes.

---

### Parallel (concurrent) DR actions with `ThreadPoolExecutor`

```python
from concurrent.futures import ThreadPoolExecutor

config.switch_acm_ctx()
results = []
with ThreadPoolExecutor() as executor:
    for wl in workloads:
        results.append(
            executor.submit(
                dr_helpers.failover,
                failover_cluster=secondary_cluster_name,
                namespace=wl.workload_namespace,
                workload_type=wl.workload_type,
                workload_placement_name=(
                    wl.appset_placement_name
                    if wl.workload_type == constants.APPLICATION_SET
                    else None
                ),
                skip_odf_cli_validation=primary_cluster_down,
            )
        )
        time.sleep(5)  # stagger — prevents race on hub API

for r in results:
    r.result()  # raises if any submission failed
```

> Always call `.result()` on every future — this re-raises exceptions from the thread and prevents silent failures.
