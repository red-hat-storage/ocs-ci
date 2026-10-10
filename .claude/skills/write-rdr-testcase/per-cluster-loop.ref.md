## Per-Cluster Loop Pattern

Used in `test_managed_cluster_node_failure.py` and `test_rdr_bug_verification.py`.

```python
from ocs_ci.ocs.utils import get_non_acm_cluster_config

managed_clusters = get_non_acm_cluster_config()
for cluster in managed_clusters:
    index = cluster.MULTICLUSTER["multicluster_index"]
    config.switch_ctx(index)
    cluster_name = cluster.ENV_DATA["cluster_name"]
    logger.info(f"Operating on managed cluster: {cluster_name}")
    # ... per-cluster action ...
```

### Custom teardown within a test class

```python
@pytest.fixture(autouse=True)
def teardown(self, request):
    def finalizer():
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            archive_ceph_crashes(get_ceph_tools_pod())
            ceph_health_check(tries=40, delay=60)
    request.addfinalizer(finalizer)
```

### `TimeoutSampler` for polling conditions

```python
from ocs_ci.utility.utils import TimeoutSampler

for sample in TimeoutSampler(
    timeout=600,
    sleep=15,
    func=some_check_function,
    arg1=value1,
):
    if sample:
        logger.info("Condition met")
        break
    logger.warning("Condition not yet met, retrying...")
```
