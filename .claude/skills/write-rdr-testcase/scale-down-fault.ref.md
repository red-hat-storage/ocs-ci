## Scale-Down Fault Injection Pattern

Used in `test_failover_after_multiple_pods_failure.py`.

```python
config.switch_to_cluster_by_name(primary_cluster_name)
scale_deployments("down")
time.sleep(120)

dr_helpers.failover(...)

primary_cluster_index = config.cur_index
primary_worker_nodes = get_nodes()
nodes_multicluster[primary_cluster_index].restart_nodes(primary_worker_nodes)

scale_deployments("up")
wait_for_pods_to_be_running(
    namespace=constants.OPENSHIFT_STORAGE_NAMESPACE, timeout=420, sleep=30
)
wait_for_pods_to_be_running(
    namespace=constants.SUBMARINER_OPERATOR_NAMESPACE, timeout=420, sleep=30
)
ceph_health_check()
```

> Always pass an explicit `timeout` to `wait_for_pods_to_be_running` in fault tests — the default may expire before pods recover.
