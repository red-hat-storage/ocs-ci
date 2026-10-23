## Hub Recovery Tests

Used in `test_site_failure_recovery_and_failover.py` and `test_neutral_hub_failure_and_recovery.py`.

### Required class-level marks

```python
@tier4a    # or @tier2 for neutral-hub
@turquoise_squad
@dr_hub_recovery          # REQUIRED — gates test to hub-recovery environments
@pytest.mark.order("last")  # ALWAYS — hub recovery must run after all other tests
class TestHubRecovery:
```

> ⚠️ Do NOT use `@rdr` on hub-recovery classes. Use `@dr_hub_recovery` alone.

### Workload deployment with `switch_ctx`

```python
from ocs_ci.helpers.dr_helpers import get_passive_acm_index

rdr_workload = dr_workload(
    num_of_subscription=1,
    num_of_appset=1,
    pvc_interface=constants.CEPHBLOCKPOOL,
    switch_ctx=get_passive_acm_index(),
)
```

### Context switches use `switch_ctx(index)` not `switch_to_cluster_by_name`

```python
from ocs_ci.ocs.utils import get_active_acm_index
from ocs_ci.helpers.dr_helpers import get_passive_acm_index

config.switch_ctx(get_active_acm_index())
config.switch_ctx(get_passive_acm_index())
```

### Hub recovery sequence

```python
from ocs_ci.helpers.dr_helpers import (
    restore_backup,
    verify_drpolicy_cli,
    verify_restore_is_completed,
    create_klusterlet_config,
    remove_parameter_klusterlet_config,
    configure_rdr_hub_recovery,
)
from ocs_ci.ocs.acm.acm import validate_cluster_import
from ocs_ci.utility.utils import TimeoutSampler

assert configure_rdr_hub_recovery()

nodes_multicluster[active_hub_index].stop_nodes(active_hub_nodes)
time.sleep(wait_time)

config.switch_ctx(get_passive_acm_index())
create_klusterlet_config()
restore_backup()
time.sleep(wait_time)
verify_restore_is_completed()

for sample in TimeoutSampler(
    timeout=1800, sleep=15,
    func=validate_cluster_import,
    cluster_name=secondary_cluster_name,
    switch_ctx=get_passive_acm_index(),
):
    if sample:
        break
    raise UnexpectedBehaviour(f"import of {secondary_cluster_name} failed")

verify_drpolicy_cli(switch_ctx=get_passive_acm_index())

dr_helpers.failover(
    failover_cluster=secondary_cluster_name,
    namespace=wl.workload_namespace,
    workload_type=wl.workload_type,
    switch_ctx=get_passive_acm_index(),
)

remove_parameter_klusterlet_config()
```

### Auto-import recovered cluster after restart

```python
from ocs_ci.ocs.acm.acm import get_clusters_env, copy_kubeconfig
from ocs_ci.utility import templating

clusters_env = get_clusters_env()
down_cluster_kubeconfig = copy_kubeconfig(
    file=clusters_env.get(f"kubeconfig_location_c{primary_index}"),
    return_str=True,
)
auto_import_secret = templating.load_yaml(
    "ocs_ci/templates/acm-deployment/auto-import-secret.yaml"
)
auto_import_secret["metadata"]["namespace"] = down_cluster_name
auto_import_secret["stringData"]["autoImportRetry"] = "50"
auto_import_secret["stringData"]["kubeconfig"] = down_cluster_kubeconfig
config.switch_ctx(get_passive_acm_index())
run_cmd(f"oc apply -f {auto_import_secret_yaml.name}")
```
