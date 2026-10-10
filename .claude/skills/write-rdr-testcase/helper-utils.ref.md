## Helper Utilities Quick Reference

### Context helpers

| Helper | Import | Use |
|---|---|---|
| `dr_helpers.set_current_primary_cluster_context(namespace)` | `from ocs_ci.helpers import dr_helpers` | Switch context to current primary cluster. Shorthand for `get_current_primary_cluster_name` + `switch_to_cluster_by_name`. |
| `config.RunWithPrimaryConfigContext()` | `from ocs_ci.framework import config` | Context manager: temporarily switches to primary cluster config. Used in 2AZ tests. |
| `get_non_acm_cluster_config()` | `from ocs_ci.ocs.utils import get_non_acm_cluster_config` | Returns list of all managed cluster config objects. Use for per-cluster loops. |

### Retry decorator

```python
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed

@retry(CommandFailed, tries=5, delay=10, backoff=1)
def get_zone_nodes_with_retry():
    return get_nodes_having_label(label=zone_label)

zone_nodes = get_zone_nodes_with_retry()
```

### Marking a test to skip the RDR health check

```python
@pytest.mark.skip_rdr_health_check
def test_mco_operator_rebranding_ui(self, setup_acm_ui):
    ...
```

### Archiving Ceph crashes in teardown

```python
from ocs_ci.utility.utils import archive_ceph_crashes
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

archive_ceph_crashes(get_ceph_tools_pod())
ceph_health_check(tries=40, delay=60)
```

### Disable DR

```python
dr_helpers.disable_dr_rdr(discovered_apps=False)

dr_helpers.wait_for_replication_resources_deletion(
    workload.workload_namespace,
    timeout=300,
    check_state=False,
)

dr_helpers.wait_for_all_resources_creation(
    workload.workload_pvc_count,
    workload.workload_pod_count,
    workload.workload_namespace,
    skip_replication_resources=True,
)
```
