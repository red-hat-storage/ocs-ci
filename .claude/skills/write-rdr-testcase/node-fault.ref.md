## Node Fault / Concurrent Operations

**Used in `test_node_operations_during_failover_relocate.py` and similar.**

### Additional imports

```python
from concurrent.futures.thread import ThreadPoolExecutor
from ocs_ci.ocs.node import unschedule_nodes, drain_nodes, schedule_nodes
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs import defaults
```

### Required fixtures

Add `node_drain_teardown` alongside `node_restart_teardown`.

### Single-workload deployment pattern (common for fault tests)

```python
if workload_type == constants.SUBSCRIPTION:
    rdr_workload = dr_workload(num_of_subscription=1)[0]
else:
    rdr_workload = dr_workload(num_of_subscription=0, num_of_appset=1)[0]
```

### Node selection by pod label

```python
config.switch_to_cluster_by_name(secondary_cluster_name)
node_name = (
    get_pods_having_label(
        label=constants.RAMEN_DR_CLUSTER_OPERATOR_APP_LABEL,
        namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
    )[0]
    .get("spec")
    .get("nodeName")
)
```

### Concurrent drain + failover pattern

```python
unschedule_nodes([node_name])
executor = ThreadPoolExecutor(max_workers=1)
drain_operation = executor.submit(drain_nodes, [node_name])
sleep(2)  # slight delay to let drain start before failover

config.switch_to_cluster_by_name(primary_cluster_name)
dr_helpers.failover(...)

# After verification, assert drain completed cleanly
drain_operation.result()
schedule_nodes([node_name])
```

### Tier for fault tests

Use `@tier4b` (not `@tier1`) — these are stress/disruptive scenarios.
