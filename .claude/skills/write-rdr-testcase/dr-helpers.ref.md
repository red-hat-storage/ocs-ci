## `dr_helpers.py` Function Reference

All functions live in `ocs_ci/helpers/dr_helpers.py`. Import via `from ocs_ci.helpers import dr_helpers`.

### Cluster context helpers

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `get_current_primary_cluster_name` | `(namespace, workload_type=SUBSCRIPTION, discovered_apps=False, resource_name=None)` | `str` | Reads DRPC spec. If action is `Failover` returns `failoverCluster`, else `preferredCluster`. |
| `get_current_secondary_cluster_name` | `(namespace, workload_type=SUBSCRIPTION, discovered_apps=False, resource_name=None)` | `str` | Reads DRPolicy `drClusters` list and returns the cluster that is NOT the primary. |
| `set_current_primary_cluster_context` | `(namespace, workload_type=SUBSCRIPTION)` | `None` | Shorthand: calls `get_current_primary_cluster_name` + `switch_to_cluster_by_name`. |
| `set_current_secondary_cluster_context` | `(namespace, workload_type=SUBSCRIPTION)` | `None` | Same pattern for secondary cluster. |
| `get_scheduling_interval` | `(namespace, workload_type=SUBSCRIPTION, discovered_apps=False, resource_name=None)` | `int` | Returns integer minutes from `DRPolicy.spec.schedulingInterval`. |

### DR actions

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `failover` | `(failover_cluster, namespace, workload_type=SUBSCRIPTION, workload_placement_name=None, switch_ctx=None, discovered_apps=False, old_primary=None, skip_odf_cli_validation=False)` | `None` | Patches DRPC with `ACTION_FAILOVER`, waits for `STATUS_FAILEDOVER` (360s). Skips odf-cli when `skip_odf_cli_validation=True`. Returns immediately if any cluster has `is_hosted=True`. |
| `relocate` | `(preferred_cluster, namespace, workload_type=SUBSCRIPTION, workload_placement_name=None, switch_ctx=None, discovered_apps=False, old_primary=None, workload_instance=None, multi_ns=False, workload_instances_shared=None, vm_auto_cleanup=False, skip_odf_cli_validation=False)` | `None` | Patches DRPC with `ACTION_RELOCATE`, waits for `STATUS_RELOCATED` (1200s). |

### Mirroring / storage checks

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `check_rbd_mirror_running` | `(namespace=None)` | `bool` | Verifies rbd-mirror daemon has ≥1 ready replica. |
| `wait_for_mirroring_status_ok` | `(replaying_images=None, replaying_groups=None, timeout=900)` | `bool` | Polls mirroring status on every non-ACM cluster. Raises `TimeoutExpiredError` on failure. |
| `check_mirroring_status_for_custom_pool` | `(pool_name, namespace=OPENSHIFT_STORAGE_NAMESPACE, min_replaying=1)` | `bool` | Validates custom `CephBlockPoolRadosNamespace` mirroring. |
| `verify_custom_pool_image_isolation` | `(pool_name)` | — | Asserts RBD images in a custom pool are not present in the default pool. |

### Sync-time verification

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `verify_last_group_sync_time` | `(drpc_obj, scheduling_interval, initial_last_group_sync_time=None)` | `str` timestamp | When `initial` given: polls until value changes. Asserts age `< 3 × scheduling_interval` minutes. |
| `verify_last_kubeobject_protection_time` | `(drpc_obj, kubeobject_sync_interval)` | `str` timestamp | Asserts `lastKubeObjectProtectionTime` age `< 2 × kubeobject_sync_interval` minutes. |

### Resource wait helpers

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `wait_for_all_resources_creation` | `(pvc_count, pod_count, namespace, timeout=900, skip_replication_resources=False, discovered_apps=False, vrg_name="", skip_vrg_check=False, performed_dr_action=False)` | `None` | Waits for PVCs→Bound, Pods→Running, then replication CRs. |
| `wait_for_all_resources_deletion` | `(namespace, timeout=1500, discovered_apps=False, workload_cleanup=False, vrg_name="", skip_vrg_check=False)` | `None` | Waits for pods, CRs, PVCs, PVs deleted. |
| `wait_for_cnv_workload` | `(vm_name, namespace, phase=STATUS_RUNNING, timeout=600)` | `None` | Waits for `VirtualMachineInstance` to reach specified phase. |
| `wait_for_replication_destinations_creation` | `(rep_dest_count, namespace, timeout=900)` | `None` | CephFS: waits for N `ReplicationDestination` objects. |
| `wait_for_replication_destinations_deletion` | `(namespace, timeout=900)` | `None` | CephFS: waits for all `ReplicationDestination` objects absent. |
| `wait_for_replication_resources_deletion` | `(namespace, timeout, check_state, discovered_apps, vrg_name, skip_vrg_check, workload_cleanup)` | `None` | Lower-level deletion poller for VR/VRG and related CRs. |
| `wait_for_resource_existence` | `(kind, namespace, resource_name="", should_exist=True, timeout=900)` | `None` | Generic poll: waits for a named CR to exist or be absent. |
| `wait_for_resource_count` | `(kind, namespace, expected_count=1, timeout=900)` | `None` | Polls until resource count equals `expected_count`. |
| `wait_for_resource_state` | `(kind, state, namespace, resource_name="", timeout=900)` | `None` | Polls until resource reaches target phase/status. |
| `wait_for_vrg_state` | `(vrg_state, vrg_namespace, resource_name, timeout=900)` | `None` | Waits for `VolumeReplicationGroup` to reach target state. |

### Backend volume helpers

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `get_backend_volumes_for_pvcs` | `(namespace)` | `dict` | Returns PVC name → backend RBD image name mapping. |
| `verify_backend_volume_deletion` | `(backend_volumes, timeout=600)` | — | Asserts backend RBD images removed from Ceph on both clusters. |
| `wait_for_backend_volume_deletion` | `(backend_volumes, timeout=600)` | — | Polls until backend images are gone. |

### DR policy and cluster info

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `get_all_drpolicy` | `()` | `list` | Returns `DRPolicy` objects. Handles HCP prefix automatically. |
| `get_all_drclusters` | `()` | `list[str]` | Returns names of all `DRCluster` objects from the hub. |
| `get_dr_topology_clusters` | `()` | `list[str]` | DRCluster names for topology UI validation (excludes `local-cluster`). |
| `get_dr_topology_policy_details` | `()` | `dict` | Returns `{name, connected_clusters, scheduling_interval}`. |
| `validate_drpolicy_replication_ids` | `(drpolicy_name, sc_names)` | — | Validates `groupreplicationID` in DRPolicy `peerClasses`. |
| `validate_drpolicy_grouping` | `(drpolicy_name=None)` | `bool` | OCS ≥ 4.21: validates DRPolicy has `grouping=true` in every `peerClasses` storageClass. No-op on older versions. |
| `validate_vgrc_count` | `()` | — | Validates `VolumeGroupReplicationContent` count across clusters. |
| `verify_drpolicy_cli` | `(switch_ctx=None)` | — | Verifies DRPolicy is in `Validated` state via odf-cli. |
| `verify_restore_is_completed` | `()` | — | Asserts ACM backup restore completed successfully. |

### Fencing helpers

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `enable_fence` | `(drcluster_name, switch_ctx=None)` | — | Patches `DRCluster` to `fencing: Fenced`. |
| `enable_unfence` | `(drcluster_name, switch_ctx=None)` | — | Patches `DRCluster` to `fencing: Unfenced`. |
| `fence_state` | `(drcluster_name, fence_state, switch_ctx=None)` | — | Generic fence state setter. |
| `get_fence_state` | `(drcluster_name, switch_ctx=None)` | `str` | Returns current fencing state of a DRCluster. |
| `configure_drcluster_for_fencing` | `()` | — | Applies required annotations for fencing support. |
| `gracefully_reboot_ocp_nodes` | `(drcluster_name, disable_eviction=False)` | — | Cordons, drains, then reboots all nodes of a managed cluster. |

### Discovered-apps cleanup

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `do_discovered_apps_cleanup` | `(drpc_name, old_primary, workload_namespace, workload_dir, vrg_name, skip_resource_deletion_verification=False)` | — | Removes DRPC, VRG, namespace, and manifests from old primary after failover/relocate. |
| `do_discovered_apps_cleanup_multi_ns` | `(old_primary, workload_instance)` | — | Same for multi-namespace discovered-apps. |

### ODF CLI validation

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `validate_application_odf_cli` | `(drpc_name, namespace, action="validate", dr_action=None, retries=5, retry_interval=100)` | `str\|None` | Runs `odf dr validate application`. Returns `None` when any cluster has `is_hosted=True`. |
| `validate_cluster_odf_cli` | `(retries=5, retry_interval=60)` | — | Cluster-level DR config validation via odf-cli. |
| `update_odf_cli_dr_config_kubeconfigs` | `()` | — | Updates odf-cli kubeconfig paths for all DR clusters. |

### Disconnected / mirror helpers

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `generate_rdr_mirror_images` | `()` | `list[str]` | Clones DR workload repo, extracts container image refs. Returns `[]` if `disconnected=False`. |
| `apply_itms_to_managed_clusters` | `(itms_file_path)` | — | Applies `ImageTagMirrorSet` to all managed clusters, waits for MCP rollout. |
| `get_cdi_registry_credentials` | `()` | `tuple[str,str]` | Extracts `(username, password)` for mirror registry. |
| `create_cdi_pull_secret` | `(namespace, secret_name="quayadmin")` | — | Creates Opaque secret with CDI registry auth. |
| `fetch_mirror_registry_cert` | `()` | `str` | Fetches TLS cert from mirror registry via openssl. |
| `create_cdi_cert_configmap` | `(namespace, configmap_name="user-ca-bundle")` | — | Creates ConfigMap with mirror registry CA cert for CDI trust. |
| `create_ingress_cert_dr` | `()` | — | Builds combined ingress CA bundle. Called during deployment, not in tests. |

### Miscellaneous

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `verify_volsync` | `()` | — | Verifies volsync pod running in `volsync-system` on every managed cluster. |
| `verify_cluster_data_protected_status` | `(workload_type, namespace, workload_placement_name=None)` | — | Polls DRPC until `clusterDataProtected` is `True`. |
| `disable_dr_rdr` | `(discovered_apps=False)` | — | Removes DR protection. |
| `is_cg_cephfs_enabled` | `()` | `bool` | `True` if CG is enabled for CephFS on the current cluster. |
| `is_cg_enabled` | `()` | `bool` | `True` if CG is enabled for any storage class. |
| `validate_protection_label` | `(kind, namespace, protection_name=None)` | — | Validates workload has the correct DR protection label. |
| `add_label_to_appsub` | `(workloads, label="test", value="test1")` | — | Adds label to application subscription resources. |
