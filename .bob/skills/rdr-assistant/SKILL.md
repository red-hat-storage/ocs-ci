---
name: rdr-assistant
description: Interactive Regional Disaster Recovery (RDR) assistant. Use when the user wants to inspect, run RDR validation on supplied clusters, deploy, failover, relocate, or manage RDR workloads (BusyBox, CNV, AppSets, Subscriptions, Discovered Apps) on OpenShift ACM Hub and managed clusters via CLI or ACM Web Console.
---

# RDR Assistant Skill

Guides Bob through managing Regional Disaster Recovery (RDR) operations interactively with the user on OpenShift Data Foundation (ODF) and Advanced Cluster Management (ACM).

**Scope: RDR only.** Metro DR (MDR), MDR-PC and stretch-cluster/arbiter setups are out of scope. If the environment is not RDR, report it and stop.

---

## Codebase Map

All DR logic lives under the `ocs-ci` repo. Key locations to know before acting:

| What | Path |
|---|---|
| **Core DR helpers (failover/relocate/CG/VRG)** | `ocs_ci/helpers/dr_helpers.py` |
| **ACM UI DR helpers (topology, failover/relocate via UI)** | `ocs_ci/helpers/dr_helpers_ui.py` |
| **Workload classes (BusyBox, AppSet, CNV, DiscoveredApps)** | `ocs_ci/ocs/dr/dr_workload.py` |
| **DRPC resource class** | `ocs_ci/ocs/resources/drpc.py` |
| **ACM UI navigator / page objects** | `ocs_ci/ocs/ui/acm_ui.py` |
| **ACM constants** | `ocs_ci/ocs/acm/acm_constants.py` |
| **DR constants** (ACTION_FAILOVER, APPLICATION_SET, DISCOVERED_APPS, …) | `ocs_ci/ocs/constants.py` |
| **RDR tests** | `tests/functional/disaster-recovery/regional-dr/` |
| **RDR conftest (fixtures, health checks)** | `tests/functional/disaster-recovery/regional-dr/conftest.py` |
| **Global conftest (dr_workload, cnv_dr_workload, discovered_apps_dr_workload fixtures)** | `tests/conftest.py` (lines ~8195–8850; `setup_acm_ui` is at ~6333) |
| **RDR config** | `conf/ocsci/multicluster_mode_rdr.yaml` |
| **DR workload config (all workload definitions)** | `conf/ocsci/dr_workload.yaml` |
| **ACM hub deploy configs** | `conf/ocsci/acm_hub_deploy.yaml`, `acm_2.9.yaml` … `acm_2.10.yaml` |
| **DRPC template YAML** | `ocs_ci/templates/DR/drpc.yaml` |
| **DRPC with recipe template** | `ocs_ci/templates/DR/drpc_with_recipe.yaml` |
| **Multicluster templates** (MirrorPeer, DRPolicy, S3 profile, …) | `ocs_ci/templates/ocs-deployment/multicluster/` |
| **RDR deployment docs** | `docs/usecases/ocs-ci-rdr-deployment.md` |
| **Multicluster test example** | `docs/multicluster_test_example.md` |

---

## Supported DR Mode

| Mode | Constant | Cluster Roles |
|---|---|---|
| **Regional DR (RDR)** | `RDR_MODE = "regional-dr"` | `ActiveACM`, `PrimaryODF`, `SecondaryODF` |

---

## Supported Workload Types

All workload classes are in `ocs_ci/ocs/dr/dr_workload.py`:

| Class | Workload Type Constant | Storage |
|---|---|---|
| `BusyBox` | `SUBSCRIPTION` | RBD, CephFS |
| `BusyBox_AppSet` | `APPLICATION_SET` | RBD, CephFS (push / pull model) |
| `CnvWorkload` | `SUBSCRIPTION` / `APPLICATION_SET` | PVC (`VM_VOLUME_PVC`), DV (`VM_VOLUME_DV`), DVT (`VM_VOLUME_DVT`) |
| `BusyboxDiscoveredApps` | `DISCOVERED_APPS` | RBD, CephFS |
| `CnvWorkloadDiscoveredApps` | `DISCOVERED_APPS` | PVC, custom SC |

Workload fixtures in `tests/conftest.py`:
- `dr_workload` — BusyBox (Subscription / AppSet)
- `cnv_dr_workload` — CNV VMs
- `discovered_apps_dr_workload` — DiscoveredApps (kubeobject / recipe / multi-ns)
- `discovered_apps_dr_workload_cnv` — CNV DiscoveredApps (shared DRPC protection)
- `dr_workloads_on_managed_clusters` — Subscription apps on both clusters
- `all_dr_workloads` — combines `dr_workload`, `discovered_apps_dr_workload` and `discovered_apps_dr_workload_cnv`

Workload definitions (names, dirs, pod/PVC counts) live under `ENV_DATA` in `conf/ocsci/dr_workload.yaml`
(also holds `deploy_via_cli`, `cg_enabled`, `dr_workload_repo_url`, `dr_workload_repo_branch`):

| Config key | Description |
|---|---|
| `dr_workload_subscription_rbd` | Subscription BusyBox RBD workloads |
| `dr_workload_subscription_cephfs` | Subscription BusyBox CephFS workloads |
| `dr_workload_subscription_placement_rbd` / `_rbd_statefulsets` | Subscription with Placement, RBD (Deployments / StatefulSets) |
| `dr_workload_subscription_placement_cephfs` / `_cephfs_statefulsets` | Subscription with Placement, CephFS (Deployments / StatefulSets) |
| `dr_workload_appset_rbd` | AppSet BusyBox RBD (Deployments) |
| `dr_workload_appset_rbd_statefulsets` | AppSet BusyBox RBD (StatefulSets) |
| `dr_workload_appset_cephfs` | AppSet BusyBox CephFS (Deployments) |
| `dr_workload_appset_cephfs_statefulsets` | AppSet BusyBox CephFS (StatefulSets) |
| `dr_workload_discovered_apps_rbd` | DiscoveredApps RBD kubeobject |
| `dr_workload_discovered_apps_cephfs` | DiscoveredApps CephFS kubeobject |
| `dr_cnv_workload_appset_push` / `_pull` / `dr_cnv_workload_sub` | CNV (PVC) AppSet push / pull, Subscription |
| `dr_cnv_dv_workload_appset_push` / `_pull` / `dr_cnv_dv_workload_sub` | CNV DataVolume (DV) AppSet push / pull, Subscription |
| `dr_cnv_dvt_workload_appset_push` / `_pull` / `dr_cnv_dvt_workload_sub` | CNV DataVolumeTemplate (DVT) AppSet push / pull, Subscription |
| `dr_cnv_discovered_apps` | CNV DiscoveredApps (standalone) |
| `dr_cnv_discovered_apps_shared` | CNV DiscoveredApps (shared DRPC) |
| `dr_cnv_discovered_apps_using_custom_pool_and_sc` | CNV DiscoveredApps on a custom pool / StorageClass |
| `dr_cnv_discovered_apps_dvt_standalone` / `dr_cnv_discovered_apps_dvt_shared` | CNV DiscoveredApps with DataVolumeTemplate |
| `dr_workload_discovered_apps_filebrowser_rbd` / `_cephfs` | FileBrowser DiscoveredApps |
| `dr_workload_discovered_apps_mongodb_rbd` / `_cephfs` | MongoDB DiscoveredApps |

---

## Key Helper Functions (dr_helpers.py)

The most-used functions:

| Function | Purpose |
|---|---|
| `failover(failover_cluster, namespace, workload_type, ...)` | Patches DRPC to trigger failover; validates ODF CLI |
| `relocate(preferred_cluster, namespace, workload_type, ...)` | Patches DRPC to trigger relocate; validates ODF CLI |
| `get_current_primary_cluster_name(namespace, workload_type)` | Returns current primary cluster name |
| `get_current_secondary_cluster_name(namespace, workload_type)` | Returns current secondary cluster name |
| `set_current_primary_cluster_context(...)` | Switches `config` context to the active primary |
| `get_scheduling_interval(namespace, workload_type)` | Returns DR policy replication interval (minutes) |
| `wait_for_all_resources_creation(pvc_count, pod_count, ...)` | Waits for all PVCs, pods, VRGs, VRCs to be ready |
| `wait_for_all_resources_deletion(...)` | Waits for cleanup on old primary |
| `wait_for_cnv_workload(vm_name, namespace, drpc_phase)` | Waits for VM to reach expected DR phase |
| `check_rbd_mirror_running(namespace)` | Confirms RBD mirror daemon is running |
| `wait_for_mirroring_status_ok(...)` | Waits for mirror health OK; supports CG |
| `is_cg_enabled()` | Returns True if Consistency Group is enabled (RDR only) |
| `is_cg_cephfs_enabled()` | Returns True if CephFS CG is enabled |
| `do_discovered_apps_cleanup(drpc_name, old_primary, ...)` | Cleans up DRPC/VRG on old primary for DiscoveredApps |
| `do_discovered_apps_cleanup_multi_ns(old_primary, ...)` | Multi-namespace DiscoveredApps cleanup |
| `validate_drpolicy_grouping(drpolicy_name)` | Validates DRPolicy CG grouping config (ODF >= 4.21) |
| `verify_last_group_sync_time(...)` | Verifies replication sync time |
| `verify_cluster_data_protected_status(...)` | Checks `ClusterDataProtected` condition |
| `get_dr_topology_clusters()` | Returns cluster data from DR topology |
| `get_dr_topology_protected_apps(workloads)` | Returns protected app details |
| `configure_rdr_hub_recovery(...)` | Hub recovery configuration |
| `disable_dr_from_app(...)` / `disable_dr_rdr(...)` | Removes DR protection from workload |
| `apply_drpolicy_to_workload(...)` | Applies DRPolicy to a workload |
| `replace_cluster(...)` | Cluster replace operations |

---

## DRPC Resource Class (ocs_ci/ocs/resources/drpc.py)

```python
class DRPC(OCP):
    def get_peer_ready_status()          # Returns PeerReady condition status
    def wait_for_peer_ready_status()     # Waits until PeerReady == True
    def get_clusterdataprotected_status()
    def wait_for_clusterdataprotected_status()
    def get_progression_status(status_to_check=None)
    def wait_for_progression_status(expected_status, timeout, ...)
    def get_last_group_sync_time()
    def get_last_kubeobject_protection_time()

# Namespaces per workload type:
# Subscription   → workload's own namespace
# ApplicationSet → openshift-gitops
# DiscoveredApps → openshift-dr-ops  (constants.DR_OPS_NAMESPACE)
```

---

## UI Functions (dr_helpers_ui.py)

| Function | Purpose |
|---|---|
| `failover_relocate_ui(acm_obj, ..., action, workload_type)` | Triggers Failover or Relocate from ACM Web Console |
| `verify_failover_relocate_status_ui(...)` | Verifies post-action UI status |
| `navigate_to_dr_topology_tab(acm_obj)` | Navigates to DR Topology view |
| `is_dr_cluster_displayed_on_topology(...)` | Checks cluster visibility on topology |
| `is_dr_policy_displayed_on_topology(...)` | Checks policy on topology |
| `open_dr_topology_cluster_sidebar(...)` | Opens cluster detail sidebar |
| `open_dr_topology_policy_sidebar(...)` | Opens policy sidebar |
| `open_dr_topology_drpc_sidebar(...)` | Opens DRPC/app sidebar |
| `verify_dr_topology_applications_sidebar(...)` | Verifies apps in sidebar |
| `verify_drpolicy_ui(...)` | Checks DRPolicy presence on ACM UI |
| `check_cluster_status_on_acm_console(...)` | Checks cluster availability on ACM |
| `dr_submariner_validation_from_ui(...)` | Submariner connectivity check via UI |
| `check_or_assign_drpolicy_for_discovered_vms_via_ui(...)` | Assign DRPolicy to discovered VMs from UI |
| `navigate_using_fleet_virtualization(...)` | Virtualization view navigation |
| `health_and_peer_connection_check_on_ui(...)` | Dashboard health check |
| `protected_volume_count_per_cluster(...)` | Volume count per cluster on dashboard |
| `application_count_on_ui(...)` | Application count verification |
| `verify_pending_cleanup_alert_firing(...)` | Checks pending cleanup alert |
| `verify_pending_cleanup_alert_resolved(...)` | Checks alert resolution |

To use UI helpers, a `setup_acm_ui` fixture must be active (defined in `tests/conftest.py`). RDR UI tests live in `tests/functional/disaster-recovery/regional-dr/` (`test_dr_topology_view_ui.py`, `test_rdr_monitoring_dashboard_ui.py`, `test_negative_failover_relocate_ui.py`).

---

## Capabilities

1. **Cluster Discovery**: Inspects Hub cluster, DRClusters, DRPolicies, DRPCs, managed applications, and DR topology.
2. **Workload Failover**: Unplanned or forced failover — Subscription, ApplicationSet, DiscoveredApps (kubeobject / recipe / multi-ns), CNV VMs.
3. **Workload Relocate**: Planned relocation with optional multi-namespace, shared DRPC, and VM auto-cleanup support.
4. **Deploy DR Workload**: Deploy any supported workload type (BusyBox, CNV PVC/DV/DVT, FileBrowser, MongoDB) via Subscription, AppSet (push/pull), or DiscoveredApps.
5. **Disable DR**: Remove DR protection (`disable_dr_from_app`, `disable_dr_rdr`).
6. **Consistency Groups (CG)**: Validate CG grouping, VGRC count, VolumeGroupReplicationContent resources (ODF >= 4.18).
7. **DR Topology & UI Inspection**: Inspects DR Topology sidebar, ACM Web Console applications view, monitoring dashboard.
8. **Monitoring & Alerting**: `pending_cleanup` alerts, replication dashboard, MCO operator rebranding.
9. **VolSync / CephFS Replication**: VolSync-based async replication for CephFS workloads (`verify_volsync`).
10. **Hub Failure & Recovery**: Neutral hub failure + recovery (`configure_rdr_hub_recovery`).
11. **Node Failures**: Managed cluster node failure and node operations during failover/relocate.
12. **Real-time Status Tracking**: Tracks `PeerReady`, `ClusterDataProtected`, `lastGroupSyncTime`, `progression`, and `phase` until completion.

---

## Phase A: Cluster Collection and Role Detection

### 1. Kubeconfig Collection Prompt

Ask the user in a single prompt for 3 to 5 kubeconfig file paths with optional role tags (`hub`, `primary`, `secondary`, `passive-hub`, `extra`).

**Prompt template**:
> Please provide 3 to 5 kubeconfig file paths for the DR environment, along with optional declared roles (`hub`, `primary`, `secondary`, `passive-hub`, `extra`).
>
> Format: `<kubeconfig_path> [declared_role]` (one per line)
>
> Example:
> ```
> /path/to/hub.kubeconfig hub
> /path/to/cluster1.kubeconfig primary
> /path/to/cluster2.kubeconfig secondary
> ```

**Minimum Cluster Requirement**:
- Minimum: **3 clusters** (1 ACM Hub cluster + 2 Managed ODF clusters).
- If fewer than 3 kubeconfig paths are provided, stop execution immediately with:
  `Error: Regional DR validation requires a minimum of 3 clusters (1 ACM Hub and 2 Managed Clusters). Only <count> provided. Stopping execution.`

**Execution Rule**:
- Never rely on ambient shell `KUBECONFIG` environment variables or hardcoded paths.
- Always invoke `oc` with the explicit cluster context per command:
  `KUBECONFIG=<kubeconfig_path> oc ...`

---

### 2. Cluster Probing and Role Detection

Run probe commands against each provided kubeconfig to identify cluster identity and active components.

#### A. Basic Identity and API Server
```bash
KUBECONFIG=<kubeconfig_path> oc whoami --show-server
```

#### B. Hub Probe
Check for ACM / DR Hub resources and the Ramen Hub operator pod:
```bash
# Check for DR CRDs and managed cluster objects
KUBECONFIG=<kubeconfig_path> oc get drpolicies,drclusters,managedclusters -A --no-headers

# Check for Ramen Hub operator pod (by label across all namespaces)
KUBECONFIG=<kubeconfig_path> oc get pods -A -l app=ramen-hub --no-headers
```
- If `drpolicies`, `drclusters`, or `managedclusters` CRs exist and a pod with `app=ramen-hub` is found, classify the cluster as **Hub** (`ActiveACM`).

#### C. Managed Cluster Probe
Check for ODF / Ceph resources and the Ramen DR Cluster operator pod:
```bash
# Check for CephCluster in openshift-storage
KUBECONFIG=<kubeconfig_path> oc get cephcluster -n openshift-storage --no-headers

# Check for Ramen DR Cluster operator pod (by label across all namespaces)
KUBECONFIG=<kubeconfig_path> oc get pods -A -l app=ramen-dr-cluster --no-headers
```
- If `CephCluster` and a pod with `app=ramen-dr-cluster` are found, classify the cluster as a **Managed Cluster** (`ODF Managed`).
- Cross-reference the cluster name with DRCluster resources listed on the Hub:
  ```bash
  KUBECONFIG=<hub_kubeconfig_path> oc get drclusters -o jsonpath='{.items[*].metadata.name}'
  ```

---

### 3. Primary and Secondary Role Derivation

Determine which managed cluster is currently acting as `Primary` and which is `Secondary` using the following rule:

1. **Check for Existing DRPlacementControl (DRPC) on Hub**:
   ```bash
   KUBECONFIG=<hub_kubeconfig_path> oc get drpc -A -o json
   ```
2. **Derivation Rule**:
   - **If one or more DRPCs exist**:
     - Check `spec.action` on the DRPC:
       - If `spec.action == "Failover"`: **Primary** = `spec.failoverCluster`.
       - Otherwise (e.g. `spec.action == "Relocate"` or unspecified): **Primary** = `spec.preferredCluster`.
     - **Secondary** = the other cluster defined in the associated DRPolicy (`spec.drClusters`):
       ```bash
       KUBECONFIG=<hub_kubeconfig_path> oc get drpolicy <policy_name> -o jsonpath='{.spec.drClusters}'
       ```
   - **If no DRPC exists on the Hub**:
     - Prompt the user to select which managed cluster will serve as the **Primary** cluster for initial workload deployment.
     - The remaining managed cluster in the DRPolicy is automatically assigned as **Secondary**.

---

### 4. Cluster Summary Table and Mismatch Confirmation

Display the gathered cluster metadata in a summary table comparing declared vs detected roles:

| Cluster Name | API Server URL | Detected Role | Declared Role | Status |
|---|---|---|---|---|
| `<hub_cluster_name>` | `<hub_api_url>` | `hub` | `<declared_or_none>` | MATCH / MISMATCH |
| `<primary_cluster_name>` | `<primary_api_url>` | `primary` | `<declared_or_none>` | MATCH / MISMATCH |
| `<secondary_cluster_name>` | `<secondary_api_url>` | `secondary` | `<declared_or_none>` | MATCH / MISMATCH |

**Mismatch Confirmation Gate**:
- If any declared role differs from the detected role (or if roles were not declared), display the discrepancy and prompt the user:
  > **Role Confirmation**: Detected cluster roles differ from declared roles (or roles were not specified).
  > Please confirm: Proceed with detected topology? `[Yes / No]` (or provide role corrections).
- Wait for user confirmation before proceeding to Phase B.

---

### 5. Topology and Model Report

Query and report the DR environment's configuration:

#### A. DR Mode (RDR check)
```bash
# Query DRPolicy scheduling interval
KUBECONFIG=<hub_kubeconfig_path> oc get drpolicies -o custom-columns=NAME:.metadata.name,INTERVAL:.spec.schedulingInterval,CLUSTERS:.spec.drClusters
```
- This skill validates **Regional DR (RDR)** only. An RDR DRPolicy has a non-empty `spec.schedulingInterval` such as `5m` (async replication); the minute value is used later for the sync waits.
- If the DRPolicy has no `schedulingInterval`, or its value is `0m`/`0`, treat the environment as not RDR: report it and stop. (This is a heuristic; if unsure, ask the user which DR mode the environment was deployed in.)

#### B. Consistency Groups (CG) Support
```bash
# grouping is reported per storage class in status.async.peerClasses (ODF >= 4.21)
KUBECONFIG=<hub_kubeconfig_path> oc get drpolicies -o jsonpath='{range .items[*]}{.metadata.name}{": "}{range .status.async.peerClasses[*]}{.storageClassName}{"="}{.grouping}{" "}{end}{"\n"}{end}'
```
- CG is **Enabled** when every storage class listed has `grouping=true`; **Disabled** if any shows `false` or empty; **Unknown** if `status.async.peerClasses` is absent (older ODF or not yet reconciled). Report Unknown as-is; do not guess.

#### C. ODF Version
```bash
# Check ODF/OCS CSV version on managed clusters
KUBECONFIG=<managed_kubeconfig_path> oc get csv -n openshift-storage -o custom-columns=NAME:.metadata.name,VERSION:.spec.version
```

#### D. Container Native Virtualization (CNV / OpenShift Virtualization)
```bash
# Check for HyperConverged CR on managed clusters
KUBECONFIG=<managed_kubeconfig_path> oc get hyperconverged -A --no-headers
```
- If `HyperConverged` CR (or CRD `hyperconvergeds.hco.kubevirt.io`) is present, CNV workloads are supported.

#### E. Component Version Collection

Collect the following on every run as part of the topology report. Run all commands with explicit `KUBECONFIG=<path>`; discover namespaces from the output rather than assuming them.

```bash
# OCP version (all clusters)
KUBECONFIG=<kubeconfig_path> oc get clusterversion version -o jsonpath='{.status.desired.version}'

# ACM (MultiClusterHub) version — hub only
KUBECONFIG=<hub_kubeconfig_path> oc get multiclusterhub -A -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.currentVersion}{"\n"}{end}'

# MCE (MultiClusterEngine) version — hub always; also check managed clusters (present in some deployments)
KUBECONFIG=<kubeconfig_path> oc get multiclusterengine -A \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.currentVersion}{"\n"}{end}' 2>/dev/null || echo "not found"

# Submariner operator version — managed clusters (CSV in submariner-operator namespace)
KUBECONFIG=<managed_kubeconfig_path> oc get csv -n submariner-operator submariner.v* \
  -o jsonpath='{.metadata.name}{" "}{.spec.version}' 2>/dev/null || echo "not found"

# OADP operator version — hub and managed clusters
KUBECONFIG=<kubeconfig_path> oc get csv -A --no-headers 2>/dev/null | grep oadp-operator | \
  awk '{print $1, $3}'

# GitOps (openshift-gitops-operator) version — hub and managed clusters
KUBECONFIG=<kubeconfig_path> oc get csv -n openshift-operators \
  -o jsonpath='{range .items[?(@.spec.displayName=="Red Hat OpenShift GitOps")]}{.spec.version}{"\n"}{end}' 2>/dev/null | \
  head -1
```

#### F. Print Topology Summary
```
============================================================
DR Topology Report
============================================================
DR Mode               : Regional DR (RDR)
DRPolicy Name         : <drpolicy_name>
Scheduling Interval   : <scheduling_interval>
Paired DRClusters     : <drcluster_1>, <drcluster_2>
Consistency Groups    : Enabled / Disabled / Unknown
OCP Version           : <version> (hub) / <version> (primary) / <version> (secondary)
ODF Version           : <version_primary> (Primary) / <version_secondary> (Secondary)
ACM Version           : <version>
MCE Version           : <version> (hub) / <version or N/A> (primary) / <version or N/A> (secondary)
Submariner Version    : <version> (or N/A if not installed)
OADP Version          : <version_hub> (hub) / <version_managed> (managed)
GitOps Version        : <version>
CNV (HyperConverged)  : Present / Absent
============================================================
```

---

## Phase B: Prerequisite and Health Validation

Run after Phase A is confirmed and **before every DR action** (deploy, failover, relocate). It mirrors the `rdr_health_check` fixture (`tests/functional/disaster-recovery/regional-dr/conftest.py`) and related helpers using `oc` only. Reuse the hub/managed kubeconfig paths and DRPolicy name from Phase A; do not ask again.

Each check yields **PASS / WARN / FAIL**. Any **FAIL** blocks execution: print the failing check, the evidence and the remediation hint, then stop and ask the user. Never trigger an action on an unhealthy environment.

Run every command as `KUBECONFIG=<path> oc ...` (no global export).

| # | Check | Where | Command | PASS / WARN / FAIL |
|---|---|---|---|---|
| 1 | API reachable | every cluster | `oc whoami --show-server` | PASS if it returns; FAIL otherwise |
| 2 | Ceph health | each managed cluster | `oc -n openshift-storage get cephcluster ocs-storagecluster-cephcluster -o jsonpath='{.status.ceph.health}'` | `HEALTH_OK` PASS; `HEALTH_WARN` WARN; anything else FAIL |
| 3 | rbd-mirror running | each managed cluster | `oc -n openshift-storage get deploy rook-ceph-rbd-mirror-a -o jsonpath='{.spec.replicas} {.status.readyReplicas}'` | PASS if both >= 1; FAIL if missing or either is 0/empty |
| 4 | Mirroring health | each managed cluster | see below | PASS if all keys are `OK`; FAIL otherwise |
| 5 | DRClusters validated | hub | `oc get drcluster <name> -o jsonpath='{range .status.conditions[*]}{.type}{"/"}{.reason}{"/"}{.status}{"\n"}{end}'` | PASS if a condition has `status=True` and reason `Validated`/`Succeeded` (or type `Validated`); FAIL otherwise. Check every DRCluster |
| 6 | DRPolicy validated | hub | `oc get drpolicy -o jsonpath='{range .items[*].status.conditions[*]}{.type}{"/"}{.reason}{"/"}{.status}{"\n"}{end}'` | Same rule as check 5; FAIL if no DRPolicy exists |
| 7 | Ramen operators running | hub + managed | `oc get pods -A -l app=ramen-hub` (hub), `oc get pods -A -l app=ramen-dr-cluster` (managed) | PASS if at least one pod is found and all are `Running`; FAIL otherwise. Discover the namespace from `-A`; do not assume it |
| 8 | Submariner (optional) | hub or a managed cluster | `subctl show connections` | PASS if all connections `connected`; WARN if `subctl` is not installed; FAIL only if connections are reported as not connected |
| 9 | Existing DRPC health | hub, per selected DRPC | `oc -n <ns> get drpc <name> -o jsonpath='{range .status.conditions[?(@.type=="PeerReady")]}{.status}{end} {.status.progression}'` | PASS if `PeerReady` is `True` and progression is `Completed`; FAIL if `PeerReady` is not `True` or progression shows an action in flight |

Check 8 notes: the repo does not ship `subctl` for macOS (see `conftest.py`). Treat a missing binary as WARN, not FAIL.

### Check 4 detail: mirroring health (run on each managed ODF cluster)

The resource depends on the ODF version (from the Phase A topology report):

```bash
# ODF >= 4.19: built-in implicit CephBlockPoolRadosNamespace
RNS=$(KUBECONFIG=<managed_kubeconfig_path> oc -n openshift-storage get cephblockpoolradosnamespaces -o name | grep -- '-builtin-implicit')
KUBECONFIG=<managed_kubeconfig_path> oc -n openshift-storage get $RNS -o jsonpath='{.status.mirroringStatus.summary}'

# ODF < 4.19: default CephBlockPool
KUBECONFIG=<managed_kubeconfig_path> oc -n openshift-storage get cephblockpool ocs-storagecluster-cephblockpool -o jsonpath='{.status.mirroringStatus.summary}'
```

- Required keys, each must equal `OK`: `daemon_health`, `health`, `image_health`. On ODF >= 4.19 also `group_health`.
- If a workload is already deployed, `states.replaying` must equal the number of its RBD PVCs. A higher count means leftover DRPCs/namespaces from earlier runs; report it and ask the user to clean up rather than continuing.
- Provider/client (HCI) setups use a per-client radosnamespace. This skill does not cover them; if `.status.mirroringStatus` is absent, report it and ask the user.
- CephFS-only environments have no RBD mirroring; skip check 4 for CephFS workloads and verify VolSync objects in Phase E instead.

### Remediation hints (suggest only; run a fix only after user approval)

| Failing check | Hint |
|---|---|
| 1 API reachable | Confirm the kubeconfig path and that the cluster is up (`oc whoami`, `oc get nodes`) |
| 2 Ceph health | `HEALTH_WARN`: wait for recovery and re-run; check `oc -n openshift-storage get cephcluster -o yaml` and the toolbox `ceph status`. Do not proceed on `HEALTH_ERR` |
| 3 rbd-mirror | Inspect `oc -n openshift-storage get pods -l app=rook-ceph-rbd-mirror` and its logs. If the deployment is missing, a `CephRBDMirror` resource was not created: ODF/MirrorPeer setup is incomplete |
| 4 Mirroring health | Wait up to 2 minutes and re-check (the fixture waits 120s); persistent non-`OK` means a replication problem: check rbd-mirror logs and the peer secret |
| 5 / 6 DRCluster / DRPolicy | `oc describe drcluster <name>` / `oc describe drpolicy <name>`; check the S3 profile secret and the hub operator pods (check 7) |
| 7 Ramen operators | `oc -n <ns> describe pod <pod>` and logs; confirm the MCO/ODF operator is installed on the hub |
| 8 Submariner | Only relevant if Submariner is the connectivity layer; follow the Submariner troubleshooting guide |
| 9 Existing DRPC | Wait for the current action to complete; if `PeerReady` stays `False`, run `oc describe drpc` and use Phase F failure analysis |

### Output format

Print one table and an overall verdict:

```
Phase B: Health Validation
------------------------------------------------------------
Check                      Cluster        Result  Detail
Ceph health                <managed_1>    PASS    HEALTH_OK
rbd-mirror                 <managed_2>    PASS    replicas=1 ready=1
Mirroring health           <managed_1>    PASS    daemon/health/image OK
...
------------------------------------------------------------
Verdict: PASS (0 FAIL, 1 WARN) -> safe to proceed
```

Verdict is **BLOCKED** if any check is FAIL. WARN items are shown and need the user's acknowledgement before proceeding.

### Re-run rule

Re-run checks 2, 3, 4 and 9 (cluster and DRPC health) immediately before every failover and every relocate, not just once at the start. Checks 1, 5, 6, 7 only need re-running if something failed or a cluster was restarted.

---

## Phase C: Scenario Menu

Run after Phase B returns PASS (or WARN acknowledged). Show the menu once, let the user pick **one** scenario, and run only that scenario. Do not suggest running several.

```
RDR Validation: choose a scenario
------------------------------------------------------------
 1. Inspect only                   topology, DRPCs, health; no writes
 2. Deploy BusyBox + full cycle    new BusyBox Subscription workload (RBD or CephFS),
                                   then failover -> relocate
 3. Full cycle on existing DRPC(s) failover -> relocate on DRPCs you pick (multi-select)
 4. Failover only                  existing DRPC
 5. Relocate only                  existing DRPC
------------------------------------------------------------
Enter a number (1-5):
```

### Availability rules

Build the menu from what Phase A/B found. Show an unavailable option as `(unavailable: <reason>)` and do not accept it.

| Option | Needs | Unavailable when |
|---|---|---|
| 1 | Hub access | never |
| 2 | Phase B PASS, a chosen primary cluster (Phase A step 3 asks if no DRPC exists), the workload repo reachable from the cluster | Phase B is BLOCKED |
| 3 / 4 / 5 | At least one existing DRPC (`oc get drpc -A`) | no DRPC exists, or Phase B is BLOCKED |

- Hide CNV DRPCs from the pick list when the `HyperConverged` CR is absent (Phase A topology report).
- Hide any DRPC whose Phase B check 9 failed (`PeerReady` not `True`, or an action already in flight) and say why.
- Option 2 supports BusyBox **Subscription** only (RBD or CephFS). Other workload types (ApplicationSet, DiscoveredApps, CNV) must already exist as a DRPC and use option 3, 4 or 5.
- Primary-cluster-down scenarios are out of scope (they need platform-specific node stop/start).

### Plan summary (print before any write)

For options 2-5 print this, then ask for confirmation using the **Step 5: Safety Confirmation** block. Do not run `oc patch`, `oc apply` or `oc create` before the user answers yes.

```
Scenario        : <number and name>
Workload(s)     : <namespace>/<drpc-name> (<type>, <RBD|CephFS>) ...
Current primary : <cluster>
Target cluster  : <secondary for failover, original primary for relocate>
DRPolicy        : <name>   Interval: <N>m   CG: Enabled|Disabled|Unknown
Steps           : <e.g. wait sync -> failover -> verify -> wait sync -> relocate -> verify>
Timeouts        : FailedOver <= 360s, Relocated <= 1200s
Writes          : <list of DRPC patches / resources that will be created>
Report          : .bob/artifacts/rdr-validation-<UTC timestamp>.md
Proceed? [Yes / No]
```

- Option 1 prints no plan summary and needs no confirmation (read-only).
- Option 3: show the picked DRPCs as a numbered list; the run continues with the remaining workloads if one fails (Phase E failure rule).
- Option 4: failover target is the cluster that is not the current primary. Option 5: relocate target is the cluster that is not the current primary (normally the original preferred cluster); ask the user to confirm the target cluster.
- If the user answers No, return to the menu.

---

## Phase D: Workload Selection or Fresh Deployment

Applies to scenarios 2-5. Scenario 1 only lists DRPCs (step D1) and stops. Reuse the hub/managed kubeconfigs, DRPolicy and `schedulingInterval` from Phase A. Run every command as `KUBECONFIG=<path> oc ...`.

### D1. List existing DRPCs (scenarios 1, 3, 4, 5)

```bash
KUBECONFIG=<hub_kubeconfig_path> oc get drpc -A -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,POLICY:.spec.drPolicyRef.name,PREFERRED:.spec.preferredCluster,FAILOVER:.spec.failoverCluster,ACTION:.spec.action,PHASE:.status.phase,PROGRESSION:.status.progression
```

Derive the workload **type** and **workload namespace** for each row (this is how `dr_helpers.failover` / `relocate` address them):

| DRPC namespace | Type | Workload namespace on the managed clusters |
|---|---|---|
| `openshift-gitops` | ApplicationSet. DRPC name is `<placement-name>-drpc` | `spec.template.spec.destination.namespace` of the ApplicationSet whose placement label equals `<placement-name>` (command below) |
| `openshift-dr-ops` | DiscoveredApps | `oc -n openshift-dr-ops get drpc <name> -o jsonpath='{.spec.protectedNamespaces}'` (may be several) |
| anything else | Subscription | the DRPC namespace itself |

```bash
# ApplicationSet -> placement name -> workload namespace
KUBECONFIG=<hub_kubeconfig_path> oc -n openshift-gitops get applicationset -o jsonpath='{range .items[*]}{.metadata.name}{"/"}{.spec.generators[0].clusterDecisionResource.labelSelector.matchLabels.cluster\.open-cluster-management\.io/placement}{"/"}{.spec.template.spec.destination.namespace}{"\n"}{end}'
```

- **CNV**: only when the `HyperConverged` CR exists. A DRPC is CNV if `oc get vm -n <workload namespace>` on the current primary returns a VirtualMachine. Mark it `CNV` in the table; Phase E adds the VMI check.
- **Storage type**: RBD or CephFS cannot be read from the DRPC alone. Ask the user, or check which StorageClass the workload PVCs use on the primary (`oc -n <ns> get pvc -o jsonpath='{.items[*].spec.storageClassName}'`; `ocs-storagecluster-ceph-rbd` = RBD, `ocs-storagecluster-cephfs` = CephFS).
- **PVC / pod counts**: take the live counts from the primary (`oc -n <ns> get pvc --no-headers | wc -l`, pods in `Running`), not from `dr_workload.yaml`.
- Show the table (namespace, name, type, storage, policy, current primary, phase, progression) and apply the Phase C hiding rules. Scenario 3 accepts several rows; 4 and 5 exactly one.
- Current primary = `spec.failoverCluster` if `spec.action` is `Failover`, else `spec.preferredCluster` (same rule as Phase A).

### D2. Fresh BusyBox Subscription workload (scenario 2 only)

Mirrors `BusyBox.deploy_workload` (`ocs_ci/ocs/dr/dr_workload.py`) with `deploy_via_cli: True` and the `dr_workload_subscription_placement_*` config, which is the RDR path in `create_workload_factory` (`tests/conftest.py`). Everything is created **on the hub**. Nothing is applied before the Phase C confirmation.

**Inputs**

| Input | RBD | CephFS |
|---|---|---|
| Config key | `dr_workload_subscription_placement_rbd[0]` | `dr_workload_subscription_placement_cephfs[0]` |
| `workload_path` (git-path) | `rdr/busybox/rbd/workloads/deployments/app-busybox-1` | `rdr/busybox/cephfs/workloads/deployments/app-busybox-1` |
| PVC selector (matchLabels) | `appname: busybox_app1` | `appname: busybox_app1_cephfs` |
| Expected PVCs / pods | 10 / 10 | 10 / 10 |
| Namespace suffix | `-rbd` | `-cephfs` |

- Repo and branch: `dr_workload_repo_url` / `dr_workload_repo_branch` from `conf/ocsci/dr_workload.yaml` (`https://github.com/red-hat-storage/ocs-workloads.git`, `master`). The skill does not handle private repos (`dr_workload_repo_login`); if the user needs one, stop.
- **Namespace** `<ns>`: a unique name ending in `-rbd` or `-cephfs`. The suffix matters: the replication checks in `dr_helpers` branch on `"cephfs" in namespace`.
- **Placement name** `<placement>`: unique, 25 characters or fewer.
- **Primary** `<primary>`: the cluster chosen in Phase A step 3. **DRPolicy** `<policy>`: from Phase A.
- **ClusterSet** `<clusterset>`: the label `cluster.open-cluster-management.io/clusterset` on the two managed clusters. They must share one value; if not, stop.
  ```bash
  KUBECONFIG=<hub_kubeconfig_path> oc get managedcluster <managed_1> <managed_2> -o jsonpath='{range .items[*]}{.metadata.labels.cluster\.open-cluster-management\.io/clusterset}{"\n"}{end}'
  ```

**Objects** (write them with the file tool to `<run_dir>/manifests/`, where `<run_dir>` is `.bob/artifacts/rdr-validation-<UTC timestamp>/`; start from the templates in `ocs_ci/templates/DR/` and replace the `PLACEHOLDER` values as listed). The code creates objects 1-6 in one `oc create -f`, then annotates, then creates the DRPC.

| # | Kind | Template | Values to set |
|---|---|---|---|
| 1 | Namespace | `namespace.yaml` | `metadata.name: <ns>` |
| 2 | Channel | `channel.yaml` | name stays `ramen-gitops`; `metadata.namespace: <ns>`; `spec.pathname: <repo url>` |
| 3 | Application | `app.yaml` | `metadata.name` and `metadata.namespace: <ns>`; `spec.selector.matchExpressions[0].values[0]: <ns>` |
| 4 | Subscription | `subscription.yaml` | `metadata.name` and `metadata.namespace: <ns>`; label `app: <ns>`; annotations `git-path: <workload_path>`, `git-branch: <branch>`; `spec.channel: <ns>/ramen-gitops`; `spec.placement.placementRef.name: <placement>` |
| 5 | Placement | `placement_subscription.yaml` | `metadata.name: <placement>`; `metadata.namespace: <ns>`; `spec.clusterSets[0]: <clusterset>`; `spec.predicates[0].requiredClusterSelector.labelSelector.matchExpressions[0].values[0]: <primary>` |
| 6 | ManagedClusterSetBinding | `managedcluster_setbinding.yaml` | `metadata.name: <clusterset>`; `metadata.namespace: <ns>`; `spec.clusterSet: <clusterset>` |
| 7 | DRPlacementControl | `drpc.yaml` | `metadata.name: <placement>-drpc`; `metadata.namespace: <ns>`; `spec.preferredCluster: <primary>`; `spec.drPolicyRef.name: <policy>`; `spec.placementRef.name: <placement>`; `spec.placementRef.namespace: <ns>`; `spec.pvcSelector.matchLabels: {appname: <selector>}`; **delete** `spec.pvcSelector.matchExpressions` and `spec.kubeObjectProtection` |

```bash
# objects 1-6 in a single file (multi-document YAML), applied on the hub
KUBECONFIG=<hub_kubeconfig_path> oc create -f <run_dir>/manifests/subscription-workload.yaml

# Placement annotation (done by add_annotation_to_placement before the DRPC is created)
KUBECONFIG=<hub_kubeconfig_path> oc -n <ns> annotate placement <placement> cluster.open-cluster-management.io/experimental-scheduling-disable='true'

# object 7
KUBECONFIG=<hub_kubeconfig_path> oc create -f <run_dir>/manifests/drpc.yaml
```

**Post-deploy verification** (stop and go to Phase F failure analysis on any timeout; 900s is the helper default):

| Check | Where | Pass |
|---|---|---|
| PVCs | primary | `oc -n <ns> get pvc` shows 10 `Bound` |
| Pods | primary | `oc -n <ns> get pods` shows 10 `Running` |
| VRG | primary | `oc -n <ns> get vrg` exists and `.spec.replicationState` and `.status.state` are `primary` |
| DRPC | hub | `PeerReady` `True`; `.status.phase` set |

Replication resources on the primary (from `wait_for_replication_resources_creation`; use the CG result from Phase A):

- **RBD**: with CG enabled, one `VolumeGroupReplication` exists and exactly **1** `VolumeReplication`; with CG disabled, **10** `VolumeReplication`. Each VR (and the VGR when present) must have `.spec.replicationState` and `.status.state` equal to `primary`. If Phase A reported CG `Unknown`, accept 1 or 10 and record which one was seen.
- **CephFS**: **10** `ReplicationSource`. If `VolumeGroupSnapshotClass ocs-storagecluster-cephfs-groupsnapclass` exists, also a `ReplicationGroupSource` and a `VolumeGroupSnapshot`, and **10** `VolumeSnapshot`.
  ```bash
  KUBECONFIG=<primary_kubeconfig_path> oc get volumegroupsnapshotclass ocs-storagecluster-cephfs-groupsnapclass
  ```
- The CG rule in the code (`is_cg_enabled`) is "ODF >= 4.21 and `cg_enabled` in config", which can differ from the live DRPolicy grouping that Phase A reads. Trust the live objects; if they disagree with Phase A, report it as a finding.

**Initial sync wait**

1. **RBD**: run Phase B check 4 on both managed clusters until it passes (timeout 900s), with `states.replaying` equal to the **PVC count** (10 for one workload; the sum if several workloads share the pool). A higher value means leftover resources: stop and ask the user to clean up.
2. **CephFS**: on the secondary, `ReplicationDestination` count equals the PVC count (10); with CG also a `ReplicationGroupDestination` and `VolumeSnapshot` count equal to the PVC count.
   ```bash
   KUBECONFIG=<secondary_kubeconfig_path> oc -n <ns> get replicationdestination --no-headers | wc -l
   ```
3. Wait `2 x schedulingInterval` minutes (the tests sleep this long "to run IOs"), then run the **sync-time gate**:
   ```bash
   SYNC=$(KUBECONFIG=<hub_kubeconfig_path> oc -n <ns> get drpc <drpc> -o jsonpath='{.status.lastGroupSyncTime}')
   python3 -c "import sys,time,calendar;print(round((time.time()-calendar.timegm(time.strptime(sys.argv[1],'%Y-%m-%dT%H:%M:%SZ')))/60,1))" "$SYNC"
   ```
   - If `lastGroupSyncTime` is empty, poll every 15s for up to `3 x schedulingInterval` minutes.
   - PASS when the printed age in minutes is **less than `3 x schedulingInterval`**; otherwise FAIL (volumes are not syncing).

### D3. Cleanup of a fresh workload (ask the user at the end)

Only for workloads Bob deployed in D2, never for pre-existing DRPCs. Ask "Delete the test workload `<ns>`? [Yes / No]" after the report is written. If yes:

```bash
KUBECONFIG=<hub_kubeconfig_path> oc delete --wait=false -f <run_dir>/manifests/subscription-workload.yaml
```

This matches `BusyBox.delete_workload`: only the combined workload file (objects 1-6) is deleted. The DRPC is not deleted separately; it lives in `<ns>`, which this file deletes (the test waits for its progression to pass through `Deleting`). Then confirm on the hub that the DRPC is gone and on **both** managed clusters that the namespace has no PVCs, pods, VRG or VolumeReplication / ReplicationSource left (the test teardown waits up to 1500s for this). If anything remains, report it instead of force-deleting.

---


## Phase E: Scenario Execution

Run after Phase C confirmation (user answered Yes). Re-run Phase B checks 2, 3, 4 and 9 before the first failover and again before relocate. If any check fails, stop — do not patch the DRPC.

All hub operations: `KUBECONFIG=<hub_kubeconfig_path> oc ...`
Managed cluster operations: `KUBECONFIG=<primary_kubeconfig_path> oc ...` or `<secondary_kubeconfig_path>`.
Never mix contexts in one pipeline.

---

### E1. Scenario 1 — Inspect only

No writes. Report the output of Phase A (topology) + Phase B (health) and stop. Scenario 1 never reaches Phase E.

---

### E2. Core cycle (scenarios 2 and 3)

Scenario 2 runs this cycle on the fresh workload deployed in Phase D2.
Scenario 3 iterates over every DRPC the user selected; per-workload failures are caught and recorded (see failure rule at the end) but the cycle continues with the remaining DRPCs.

For each workload `<ns>` / `<drpc>` / `<type>` / `<storage>` (`RBD` or `CephFS`):

**Step E2-1. Record current state**
```bash
# Record primary and secondary before touching anything
PRIMARY=$(KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> -o jsonpath='{.spec.preferredCluster}')
SECONDARY=<the other DRCluster from Phase A>
SCHEDULING_INTERVAL=<integer minutes from Phase A>
```
Phase A already has these. Do not re-query DRPolicy here; reuse the values.

**Step E2-2. (CephFS only) Pre-failover ReplicationDestination check on secondary**
```bash
KUBECONFIG=<secondary_kubeconfig_path> oc -n <ns> get replicationdestination --no-headers | wc -l
# Expected: equal to PVC count (from Phase D); timeout 900s
```
With CG (VolumeGroupSnapshotClass `ocs-storagecluster-cephfs-groupsnapclass` exists on primary):
```bash
KUBECONFIG=<secondary_kubeconfig_path> oc -n <ns> get replicationgroupdestination --no-headers | wc -l
# Expected: 1
KUBECONFIG=<secondary_kubeconfig_path> oc -n <ns> get volumesnapshot --no-headers | wc -l
# Expected: equal to PVC count
```

**Step E2-3. Wait and verify sync gate before failover**

1. Wait `2 × SCHEDULING_INTERVAL` minutes.
2. Record `lastGroupSyncTime` *before* failover:
   ```bash
   BEFORE_FAILOVER_SYNC=$(KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> \
     -o jsonpath='{.status.lastGroupSyncTime}')
   ```
3. Verify the age of `BEFORE_FAILOVER_SYNC` is less than `3 × SCHEDULING_INTERVAL` minutes (use the sync-age one-liner from Phase D2). If it exceeds that, FAIL — volumes are not syncing.
4. For DiscoveredApps also check `lastKubeObjectProtectionTime`:
   ```bash
   KUBECONFIG=<hub_kubeconfig_path> oc -n openshift-dr-ops get drpc <drpc> \
     -o jsonpath='{.status.lastKubeObjectProtectionTime}'
   # age must be < 2 × SCHEDULING_INTERVAL minutes
   ```

**Step E2-4. Patch DRPC to Failover**

Re-run Phase B checks 2, 3, 4, 9 first. Then patch:

```bash
# Subscription
KUBECONFIG=<hub_kubeconfig_path> oc -n <ns> patch drpc <drpc> --type merge \
  -p '{"spec":{"action":"Failover","failoverCluster":"<SECONDARY>"}}'

# ApplicationSet (DRPC is in openshift-gitops)
KUBECONFIG=<hub_kubeconfig_path> oc -n openshift-gitops patch drpc <placement>-drpc --type merge \
  -p '{"spec":{"action":"Failover","failoverCluster":"<SECONDARY>"}}'

# DiscoveredApps — must also set preferredCluster = current primary (old_primary)
KUBECONFIG=<hub_kubeconfig_path> oc -n openshift-dr-ops patch drpc <drpc> --type merge \
  -p '{"spec":{"action":"Failover","failoverCluster":"<SECONDARY>","preferredCluster":"<PRIMARY>"}}'
```

**Step E2-5. Wait for FailedOver phase (≤ 360s)**

Poll every 10s:
```bash
KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> \
  -o jsonpath='{.status.phase} {.status.progression}'
```
- PASS when `.status.phase` is `FailedOver` AND `.status.progression` is `Completed`.
- If not reached within 360s: FAIL. Capture DRPC YAML and go to Phase F.

**Step E2-6. Verify resources on secondary (now active primary)**

On secondary:
```bash
KUBECONFIG=<secondary_kubeconfig_path> oc -n <ns> get pvc --no-headers | grep -c Bound
KUBECONFIG=<secondary_kubeconfig_path> oc -n <ns> get pods --no-headers | grep -c Running
```
- Expected counts: PVC count and pod count from Phase D1 / D2.
- CNV workloads also:
  ```bash
  KUBECONFIG=<secondary_kubeconfig_path> oc -n <ns> get vmi -o jsonpath='{.items[*].status.phase}'
  # Expected: all items are "Running" (timeout 600s, poll every 10s)
  ```
- VRG on secondary must be in state `primary` (`.spec.replicationState` and `.status.state`):
  ```bash
  KUBECONFIG=<secondary_kubeconfig_path> oc -n <vrg_ns> get vrg <vrg> -o jsonpath='{.spec.replicationState} {.status.state}'
  # Subscription / AppSet: vrg_ns = <ns>
  # DiscoveredApps: vrg_ns = openshift-dr-ops
  ```

**Step E2-7. DiscoveredApps cleanup on old primary (only for DiscoveredApps)**

After `FailedOver + Completed`, the DRPC progression transitions to `WaitOnUserToCleanUp`. The cleanup deletes the workload from old primary (now secondary).

```bash
# 1. Confirm progression is WaitOnUserToCleanUp (poll up to 300s)
KUBECONFIG=<hub_kubeconfig_path> oc -n openshift-dr-ops get drpc <drpc> \
  -o jsonpath='{.status.progression}'

# 2. Delete workload from old primary (kustomize directory, from Phase D1 workload type or Phase D2 workload_path)
KUBECONFIG=<primary_kubeconfig_path> oc delete -k ocs-workloads/<workload_dir> -n <ns> \
  --wait=false --ignore-not-found --force

# 3. Wait for deletion on old primary (VRG, PVCs, pods)
# Poll every 30s for up to 1500s:
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get pvc,pods,vrg --no-headers 2>/dev/null | wc -l
# Pass when count is 0

# 4. Confirm DRPC progression returns to Completed (poll up to 300s)
KUBECONFIG=<hub_kubeconfig_path> oc -n openshift-dr-ops get drpc <drpc> \
  -o jsonpath='{.status.progression}'
```

Subscription and ApplicationSet: skip this step — `wait_for_all_resources_deletion` verifies deletion without the cleanup step.

**Step E2-8. Verify deletion from old primary (Subscription / ApplicationSet)**

```bash
# Poll until namespace is empty of PVCs and pods (1500s timeout)
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get pvc --no-headers 2>/dev/null | wc -l
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get pods --no-headers 2>/dev/null | wc -l
```
Also VRG state on old primary transitions to `secondary`:
```bash
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get vrg -o jsonpath='{.items[*].status.state}'
# Expected: "secondary" (or resource gone on ODF < 4.18)
```

**Step E2-9. Post-failover storage check**

**RBD** (both managed clusters):
- Re-run Phase B check 4 with `states.replaying` = PVC count. This confirms mirroring health is OK after the role swap.

**CephFS** (on the new secondary, which was the old primary):
```bash
# ReplicationGroupDestination must be gone from old secondary (previous run of this check showed it existed there)
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get replicationgroupdestination 2>/dev/null | wc -l
# Expected: 0

# ReplicationDestination must appear on old primary (new secondary)
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get replicationdestination --no-headers | wc -l
# Expected: PVC count
```
With CG (VolumeGroupSnapshotClass exists):
```bash
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get replicationgroupdestination 2>/dev/null | wc -l
# Expected: 1
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get volumesnapshot --no-headers | wc -l
# Expected: PVC count
```

**Step E2-10. Wait and verify sync gate before relocate**

1. Wait `2 × SCHEDULING_INTERVAL` minutes again.
2. Record and verify `lastGroupSyncTime` again — this must be a **new** value different from `BEFORE_FAILOVER_SYNC`:
   ```bash
   BEFORE_RELOCATE_SYNC=$(KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> \
     -o jsonpath='{.status.lastGroupSyncTime}')
   ```
   Age must be < `3 × SCHEDULING_INTERVAL` minutes. This is the gate the test explicitly calls "do NOT skip" (`.claude/skills/rdr-writer/SKILL.md` step 12).

**Step E2-11. Patch DRPC to Relocate**

Re-run Phase B checks 2, 3, 4, 9 first. Then patch:

```bash
# Subscription
KUBECONFIG=<hub_kubeconfig_path> oc -n <ns> patch drpc <drpc> --type merge \
  -p '{"spec":{"action":"Relocate","preferredCluster":"<PRIMARY>"}}'

# ApplicationSet
KUBECONFIG=<hub_kubeconfig_path> oc -n openshift-gitops patch drpc <placement>-drpc --type merge \
  -p '{"spec":{"action":"Relocate","preferredCluster":"<PRIMARY>"}}'

# DiscoveredApps — must set both preferredCluster (new) AND failoverCluster (old primary = now secondary)
KUBECONFIG=<hub_kubeconfig_path> oc -n openshift-dr-ops patch drpc <drpc> --type merge \
  -p '{"spec":{"action":"Relocate","failoverCluster":"<PRIMARY>","preferredCluster":"<PRIMARY>"}}'
```

Note: for DiscoveredApps, `relocate()` waits for `.status.phase == Relocating` (not `Relocated`), then triggers cleanup. The skill follows the same sequence.

**Step E2-12. Wait for Relocate phase (≤ 1200s)**

Poll every 15s:
```bash
KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> \
  -o jsonpath='{.status.phase} {.status.progression}'
```
- **Subscription / ApplicationSet:** PASS when `.status.phase` is `Relocated` AND `.status.progression` is `Completed`.
- **DiscoveredApps:** PASS when `.status.phase` is `Relocating` (then run cleanup E2-7 again with roles swapped: old_primary = secondary, cleanup on secondary).
- If not reached within 1200s: FAIL, go to Phase F.

**Step E2-13. Verify resources back on original primary**

Same checks as E2-6 but on primary (which is now running the workload again). CNV: verify VMI `Running`.

**Step E2-14. Post-relocate storage check**

Same as E2-9, with roles swapped: the new secondary is the cluster that was the failover target.

**Step E2-15. Final sync-time check**

```bash
AFTER_RELOCATE_SYNC=$(KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> \
  -o jsonpath='{.status.lastGroupSyncTime}')
```
Must be different from `BEFORE_RELOCATE_SYNC` and age < `3 × SCHEDULING_INTERVAL`.

---

### E3. Scenario 4 — Failover only

Run Phase B checks (2, 3, 4, 9), then run steps E2-1 through E2-9. Stop after E2-9. Report phase, progression and sync time. Do not patch to Relocate.

---

### E4. Scenario 5 — Relocate only

Run Phase B checks (2, 3, 4, 9), then run E2-1, E2-3 (sync gate before relocate), E2-11, E2-12, E2-13, E2-14, E2-15. Skip E2-4 through E2-10.

Before patching, confirm the DRPC's current state allows relocate: `spec.action` should be `Failover` (or empty if a previous failover is reflected by the cluster running a non-preferred workload). If `spec.action` is already `Relocate`, tell the user the workload appears to already be on the preferred cluster and ask whether to proceed.

---

### Continue-on-failure rule (scenario 3 only)

For each DRPC in the selected list, catch any FAIL from steps E2-5, E2-6, E2-8, E2-12, E2-13:

1. Record `{ drpc: <name>, stage: <step id>, error: <message>, primary_before: <PRIMARY>, secondary_before: <SECONDARY> }`.
2. **Skip the relocate steps (E2-10 through E2-15)** for that DRPC to avoid leaving it in an inconsistent state. Leave it in its current phase; do not attempt to undo.
3. Continue to the next DRPC in the list.
4. Include all failures in the Phase G report under "Failures".

---

### DRPC namespace and name summary

| Type | DRPC namespace (`<drpc_ns>`) | DRPC name (`<drpc>`) | VRG namespace for managed-cluster checks |
|---|---|---|---|
| Subscription | workload namespace | DRPC found in that namespace | workload namespace |
| ApplicationSet | `openshift-gitops` | `<placement>-drpc` | workload namespace |
| DiscoveredApps | `openshift-dr-ops` | placement name | `openshift-dr-ops` |

---


## Phase F: Monitoring and Failure Analysis

Phase E calls Phase F whenever a step returns FAIL. Phase F is also used proactively: during each polling loop (E2-5, E2-12) Bob already shows the phase/progression; if it stops changing for 10 minutes, classify it as a stall before the timeout fires.

Run every command as `KUBECONFIG=<path> oc ...`. Save all command output to `<run_dir>/evidence/<drpc>-<stage>/` using the file tool.

---

### F1. Polling during a DR action

While waiting for phase / progression to change, print a one-line status every 30s:

```
[HH:MM:SS] <drpc>: phase=<phase>  progression=<progression>  PeerReady=<T/F>  lastGroupSyncTime=<age>m ago
```

Commands:
```bash
KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> \
  -o jsonpath='phase={.status.phase}  progression={.status.progression}  PeerReady={range .status.conditions[?(@.type=="PeerReady")]}{.status}{end}'
```

**Stall rule**: if `progression` has not changed for 10 consecutive minutes (≥ 20 polls), flag it as `STALL` and proceed to evidence collection without waiting for the timeout.

---

### F2. Evidence collection

Run this command set when a step FAILs or stalls. Capture each output to a file.

```bash
# 1. Full DRPC YAML (hub)
KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> -o yaml \
  > <run_dir>/evidence/<drpc>-<stage>/drpc.yaml

# 2. DRPC conditions (hub)
KUBECONFIG=<hub_kubeconfig_path> oc -n <drpc_ns> get drpc <drpc> \
  -o jsonpath='{range .status.conditions[*]}{.type}{"|"}{.reason}{"|"}{.status}{"|"}{.message}{"\n"}{end}'

# 3. VRG state on primary
KUBECONFIG=<primary_kubeconfig_path> oc -n <vrg_ns> get vrg -o yaml \
  > <run_dir>/evidence/<drpc>-<stage>/vrg-primary.yaml

# 4. VRG state on secondary
KUBECONFIG=<secondary_kubeconfig_path> oc -n <vrg_ns> get vrg -o yaml \
  > <run_dir>/evidence/<drpc>-<stage>/vrg-secondary.yaml

# 5. Ramen hub operator logs (discover namespace from label)
RAMEN_HUB_POD=$(KUBECONFIG=<hub_kubeconfig_path> oc get pods -A -l app=ramen-hub -o jsonpath='{.items[0].metadata.namespace}/{.items[0].metadata.name}')
KUBECONFIG=<hub_kubeconfig_path> oc logs -n ${RAMEN_HUB_POD%%/*} ${RAMEN_HUB_POD##*/} --tail=200 \
  > <run_dir>/evidence/<drpc>-<stage>/ramen-hub.log

# 6. Ramen dr-cluster operator logs on primary
RAMEN_DR_POD=$(KUBECONFIG=<primary_kubeconfig_path> oc get pods -A -l app=ramen-dr-cluster -o jsonpath='{.items[0].metadata.namespace}/{.items[0].metadata.name}')
KUBECONFIG=<primary_kubeconfig_path> oc logs -n ${RAMEN_DR_POD%%/*} ${RAMEN_DR_POD##*/} --tail=200 \
  > <run_dir>/evidence/<drpc>-<stage>/ramen-dr-primary.log

# 7. rbd-mirror pod logs on primary (RBD workloads only)
KUBECONFIG=<primary_kubeconfig_path> oc -n openshift-storage logs \
  -l app=rook-ceph-rbd-mirror --tail=100 \
  > <run_dir>/evidence/<drpc>-<stage>/rbd-mirror-primary.log

# 8. VolSync pod logs on both clusters (CephFS workloads only)
KUBECONFIG=<primary_kubeconfig_path> oc -n volsync-system logs \
  -l app.kubernetes.io/name=volsync --tail=100 \
  > <run_dir>/evidence/<drpc>-<stage>/volsync-primary.log
KUBECONFIG=<secondary_kubeconfig_path> oc -n volsync-system logs \
  -l app.kubernetes.io/name=volsync --tail=100 \
  > <run_dir>/evidence/<drpc>-<stage>/volsync-secondary.log

# 9. ReplicationSource / ReplicationDestination on both clusters (CephFS only)
KUBECONFIG=<primary_kubeconfig_path> oc -n <ns> get replicationsource,replicationdestination -o yaml \
  > <run_dir>/evidence/<drpc>-<stage>/repobjects-primary.yaml
KUBECONFIG=<secondary_kubeconfig_path> oc -n <ns> get replicationsource,replicationdestination -o yaml \
  > <run_dir>/evidence/<drpc>-<stage>/repobjects-secondary.yaml

# 10. Mirroring summary on both clusters (RBD only)
KUBECONFIG=<primary_kubeconfig_path> oc -n openshift-storage get cephblockpool \
  ocs-storagecluster-cephblockpool -o jsonpath='{.status.mirroringStatus.summary}' \
  > <run_dir>/evidence/<drpc>-<stage>/mirroring-primary.txt
KUBECONFIG=<secondary_kubeconfig_path> oc -n openshift-storage get cephblockpool \
  ocs-storagecluster-cephblockpool -o jsonpath='{.status.mirroringStatus.summary}' \
  > <run_dir>/evidence/<drpc>-<stage>/mirroring-secondary.txt
```

---

### F3. Failure classification and recommendations

Examine the evidence and classify using this table. A single failure may match more than one class; report all that apply.

| Class | Primary signal | Diagnosis command | Recommendation |
|---|---|---|---|
| **Replication lag** | `lastGroupSyncTime` age ≥ `3 × schedulingInterval` minutes | Check mirroring summary (file 10) for `replaying` count mismatch or non-`OK` keys | Wait one more interval; if it persists check rbd-mirror pod (file 7) for errors; confirm `states.replaying` equals PVC count |
| **DRPC stuck** | `progression` unchanged for ≥ 10 minutes | Check DRPC conditions (file 2) for a `False` condition with a message; check ramen-hub logs (file 5) for errors referencing the DRPC name | Describe the DRPC (`oc -n <drpc_ns> describe drpc <drpc>`) and look for `message` fields; consider restarting the ramen-hub pod after consulting support |
| **FailedOver not reached** | Phase E2-5 timeout; `.status.phase` is not `FailedOver` | Check DRPC conditions for `PeerReady: False`; check VRG on failover target (file 4) for `state: secondary` stuck | Confirm `PeerReady` is `True` before retrying; if VRG is stuck on secondary, check ramen-dr-cluster logs (file 6) on the failover target |
| **Relocated not reached** | Phase E2-12 timeout; `.status.phase` is not `Relocated` | Check `WaitOnUserToCleanUp` progression on hub; if so, E2-7 cleanup was not run | Run the DiscoveredApps cleanup steps manually (E2-7) and verify progression returns to `Completed` |
| **Storage replication failure (RBD)** | `daemon_health` / `health` / `image_health` not `OK` in mirroring summary | See mirroring file (10); check rbd-mirror pod (file 7) for auth or network errors | Verify peer secret is valid (`oc -n openshift-storage get secret`); check connectivity between clusters; restart the rbd-mirror deployment if the pod is crash-looping |
| **Storage replication failure (CephFS)** | `ReplicationSource` count mismatch; no `ReplicationDestination` on secondary | Check volsync logs (files 8, 9) for `error` lines | Verify VolSync is running on both clusters (`oc -n volsync-system get pods`); check `ReplicationSource` `.status.lastSyncTime` and `.status.conditions` |
| **Network / Submariner** | Stall on phase without a clear DRPC condition message; `subctl show connections` shows non-connected peers | `subctl show connections` (if installed); check managedcluster status on hub | Re-run Submariner diagnostics; this is outside the skill's scope — escalate |
| **WaitOnUserToCleanUp not clearing** | DiscoveredApps progression stuck at `WaitOnUserToCleanUp` after cleanup was run | Check old-primary namespace for leftover PVCs or VRG (`oc -n <ns> get pvc,vrg`) | Retry `oc delete -k ... --force --ignore-not-found`; if VRG remains, check ramen-dr-cluster logs for errors |
| **Other** | None of the above patterns match | Print the full DRPC `conditions` array (file 2) and ramen-hub logs (file 5) in the report | Include all evidence files in the report and note "classification unknown" |

**After classification:**
- Print the class, the primary signal, and the recommendation inline.
- If the class is **DRPC stuck** or **Network / Submariner**, stop the scenario and go to report generation (Phase G) immediately. These require investigation outside automated `oc` commands.
- For all other classes, after reporting, ask the user: "Attempt to continue from this step? [Yes / No / Abort]". "Abort" goes straight to Phase G.

---


## Phase G: Report Generation

Run at the end of every session — whether the scenario completed successfully, failed or was aborted. Report before asking the user about workload cleanup (Phase D3).

---

### G1. Artifact folder layout

All artifacts for one session live in:
```
.bob/artifacts/rdr-validation-<UTC-timestamp>/
  manifests/                     ← Phase D2 generated YAMLs (only when a fresh workload was deployed)
  evidence/
    <drpc>-<stage>/              ← Phase F evidence files per DRPC + step
  rdr-validation-<UTC-timestamp>.md
  rdr-validation-<UTC-timestamp>.html
```

`<UTC-timestamp>` format: `YYYYMMDD-HHMMSS`. Bob generates this once at Phase A and reuses it everywhere.

**Redaction rule**: Never write any of the following to a report file — kubeconfig file contents, tokens, passwords, `AUTH` fields, `dc_rdr.yaml` values, or any value whose key contains `secret`, `token`, `password`, `credential` or `key`.

---

### G2. Markdown report template

Write `.bob/artifacts/rdr-validation-<UTC-timestamp>/rdr-validation-<UTC-timestamp>.md` using the file tool.

```markdown
# RDR Validation Report — <UTC timestamp>

## Environment

| Item | Value |
|---|---|
| Hub cluster | <API server URL> |
| Primary cluster | <name> |
| Secondary cluster | <name> |
| DRPolicy | <name> |
| Scheduling interval | <N> minutes |
| CG | Enabled / Disabled / Unknown |
| ODF version (primary) | <version> |
| ODF version (secondary) | <version> |
| CNV present | Yes / No |

## Pre-checks (Phase B)

| # | Check | Cluster | Result | Detail |
|---|---|---|---|---|
| 1 | API reachable | all | PASS / FAIL | … |
...
**Verdict:** PASS / WARN / BLOCKED

## Scenario

**Selected:** <number and name>
**Workloads:** <list of namespace/drpc/type/storage>

## Execution Summary

| Step | Workload | Result | Duration | Notes |
|---|---|---|---|---|
| E2-1 | <ns/drpc> | PASS | <s> | |
| E2-4 | <ns/drpc> | PASS | <s> | patched to Failover |
| E2-5 | <ns/drpc> | PASS | <s> | FailedOver+Completed in <Ns> |
...

**Overall result:** PASS / FAIL (N workloads passed, M failed)
**Total duration:** <minutes>

## Failures

_None._ or:

### <drpc> — Stage <step id>

- **Class:** <class from F3>
- **Signal:** <primary signal>
- **Evidence:** `.bob/artifacts/.../evidence/<drpc>-<stage>/`
- **Recommendation:** <recommendation from F3>

## Post-cycle state

| Workload | DRPC namespace | Phase | Progression | Current primary |
|---|---|---|---|---|
| <ns/drpc> | <ns> | Relocated | Completed | <cluster> |
...

## Artifacts

- Evidence files: `.bob/artifacts/rdr-validation-<UTC-timestamp>/evidence/`
- Manifests (if fresh deploy): `.bob/artifacts/rdr-validation-<UTC-timestamp>/manifests/`
```

---

### G3. HTML report

After writing the Markdown file, write the HTML version using the same data. Bob writes it with the file tool.

**Structure:** single self-contained HTML file. Inline all CSS. No external resources. Style rules:
- Font: `system-ui, -apple-system, sans-serif`, 14px, line-height 1.6.
- Background `#ffffff`, surface `#f7f8fa`, border `#e5e7eb`, text `#1f2328`, muted `#57606a`.
- PASS cells: `background: #d1fae5` (green-tint). FAIL/BLOCKED cells: `background: #fee2e2` (red-tint). WARN: `background: #fef9c3`.
- Tables: `border-collapse: collapse`, 1px solid border on each cell, padding 6px 10px.
- Max-width 900px, centred with `margin: 0 auto`.

Write a `<footer>` at the end: `Generated by Bob · RDR Validation Skill · <UTC timestamp>`.

---

### G4. Print paths and finish

After writing both files, print:
```
Report: .bob/artifacts/rdr-validation-<UTC-timestamp>/rdr-validation-<UTC-timestamp>.md
Report: .bob/artifacts/rdr-validation-<UTC-timestamp>/rdr-validation-<UTC-timestamp>.html
Evidence: .bob/artifacts/rdr-validation-<UTC-timestamp>/evidence/
```

Then (if a fresh workload was deployed in Phase D2) ask the cleanup question from Phase D3. Then stop.

---


## Relevant Tests to Reference

| Scenario | Test File |
|---|---|
| Standard failover + relocate (RDR) | `tests/functional/disaster-recovery/regional-dr/test_failover_and_relocate.py` |
| Relocate only | `tests/functional/disaster-recovery/regional-dr/test_relocate.py` |
| Sequential failover | `tests/functional/disaster-recovery/regional-dr/test_sequential_failover.py` |
| DiscoveredApps failover + relocate | `tests/functional/disaster-recovery/regional-dr/test_failover_and_relocate_discovered_apps.py` |
| DiscoveredApps multi-namespace | `tests/functional/disaster-recovery/regional-dr/test_failover_and_relocate_discovered_apps_multi_ns.py` |
| CNV app failover + relocate (RDR) | `tests/functional/disaster-recovery/regional-dr/test_cnv_app_failover_and_relocate.py` |
| CNV DiscoveredApps | `tests/functional/disaster-recovery/regional-dr/test_cnv_failover_and_relocate_discovered_apps.py` |
| Hub failure + recovery | `tests/functional/disaster-recovery/regional-dr/test_neutral_hub_failure_and_recovery.py` |
| Node operations during DR | `tests/functional/disaster-recovery/regional-dr/test_node_operations_during_failover_relocate.py` |
| Managed cluster node failure | `tests/functional/disaster-recovery/regional-dr/test_managed_cluster_node_failure.py` |
| Disable DR | `tests/functional/disaster-recovery/regional-dr/test_disable_dr.py` |
| DR topology UI | `tests/functional/disaster-recovery/regional-dr/test_dr_topology_view_ui.py` |
| Monitoring dashboard UI | `tests/functional/disaster-recovery/regional-dr/test_rdr_monitoring_dashboard_ui.py` |
| Negative UI scenarios | `tests/functional/disaster-recovery/regional-dr/test_negative_failover_relocate_ui.py` |
| Warning & alerting | `tests/functional/disaster-recovery/regional-dr/test_warning_and_alerting.py` |
| CG configuration | `tests/functional/disaster-recovery/regional-dr/test_cg_configuration.py` |
| Deploy RDR | `tests/functional/disaster-recovery/regional-dr/test_deploy_rdr.py` |
