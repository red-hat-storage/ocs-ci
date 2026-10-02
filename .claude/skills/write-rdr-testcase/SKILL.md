---
name: write-rdr-testcase
version: 2.1.0
description: Use when the user wants to write a new RDR (Regional Disaster Recovery) test case in ocs-ci. Guides through class structure, markers, fixtures, workload setup, DR actions (failover/relocate), and verification patterns consistent with the existing test suite. Covers standard appset/subscription, discovered apps, CNV, node-fault, hub-recovery, HCP, and disconnected-mode scenarios. Includes complete dr_helpers.py and DRPC class reference.
---

# Write an RDR Test Case

> **Audience:** New contributors learning patterns AND experienced engineers needing a fast reference.
> All code in this skill is grounded in real files under `tests/functional/disaster-recovery/regional-dr/`.

---

## ⚡ Quick-Pick (experienced engineers — jump directly)

| I need to write a test for… | Go to |
|---|---|
| Standard appset failover/relocate (primary focus) | [Step 3 — Core Skeleton](#step-3--core-test-skeleton) |
| Subscription-only or appset-only variant | [Step 3 — note on workload count](#step-3--core-test-skeleton) |
| Discovered-apps workload | [Addendum A — Discovered Apps](#addendum-a--discovered-apps-workload) |
| CNV / KubeVirt VM workload | [Addendum B — CNV Workload](#addendum-b--cnv-workload) |
| Node-fault / concurrent operations | [Addendum C — Node Operations](#addendum-c--node-fault--concurrent-operations) |
| CephFS-specific checks (ReplicationDestination) | [Step 5 — CephFS Checks](#step-5--cephfs-specific-checks) |
| UI-driven failover/relocate | [Addendum D — ACM UI Flow](#addendum-d--acm-ui-flow) |
| Structured step logging / DRPC progression wait | [Addendum E — Advanced Patterns](#addendum-e--advanced-patterns) |
| Mixed RBD + CephFS in one test | [Addendum F — Mixed PVC Interface](#addendum-f--mixed-pvc-interface-tests-rbd--cephfs-simultaneously) |
| Hub failure and recovery (active/passive hub) | [Addendum G — Hub Recovery](#addendum-g--hub-recovery-tests) |
| Per-cluster loops / custom teardown / TimeoutSampler | [Addendum H — Per-Cluster Loop](#addendum-h--per-cluster-loop-pattern) |
| Scale-down fault injection (`scale_deployments`) | [Addendum I — Scale-Down Fault](#addendum-i--scale-down-fault-injection-pattern) |
| Context helpers / retry / skip health check / disable DR | [Addendum J — Helper Utilities](#addendum-j--helper-utilities-quick-reference) |
| Marks reference (all optional marks incl. hcp_required, mdr, skip_rdr_health_check) | [Step 2 — Marks](#step-2--marks--class-layout) |
| All conftest fixtures | [Step 4 — Fixtures](#step-4--conftest-fixtures-reference) |
| UI test automation (Selenium, locators, PatternFly, CLI fallback, via_ui pattern) | [Appendix O — UI Test Automation](#appendix-o--ui-test-automation-for-rdr) |
| DRPC class methods (wait_for_peer_ready, wait_for_progression_status, drpolicy_obj, etc.) | [Appendix N — DRPC Class Methods](#appendix-n--drpc-class-methods-reference) |

---

## Step 1 — Gather Requirements

Use `ask_followup_question` to clarify these before writing a single line:

| # | Question | Why it matters |
|---|---|---|
| 1 | **Test scenario** — What DR failure mode does this cover? (node failure, pod failure, network partition, sequential failover, etc.) | Drives the scenario-specific fault injection block |
| 2 | **Workload type** — Subscription/AppSet (`dr_workload`), Discovered Apps (`discovered_apps_dr_workload`), or CNV (`cnv_dr_workload`)? | Each type has different DRPC lookup, cleanup, and verification paths |
| 3 | **PVC interface** — RBD (`constants.CEPHBLOCKPOOL`), CephFS (`constants.CEPHFILESYSTEM`), or both (parametrized)? | CephFS requires extra `ReplicationDestination` checks; RBD requires `wait_for_mirroring_status_ok` |
| 4 | **Primary cluster state during failover** — Always down, always up, or parametrize both? | Controls `primary_cluster_down` param and node stop/start code |
| 5 | **UI or CLI actions** — CLI only, or also cover ACM UI (`via_ui` param)? | UI path requires `setup_acm_ui`, `AcmAddClusters`, and `failover_relocate_ui` |
| 6 | **OCS version gate** — Does the scenario require `@skipif_ocs_version("<4.XX")`? | Some features (CephFS DR, CG, discovered apps) are version-gated |
| 7 | **Tier and squad** — Default is `@tier1` + `@turquoise_squad`. Node/fault tests are typically `@tier4b`. | Wrong tier fails CI filtering |
| 8 | **Polarion IDs** — One per `pytest.param`. Use placeholder `OCS-XXXX` if not yet assigned. | Required by CI; missing IDs cause failures |
| 9 | **File name** — Suggest `test_<scenario>.py`. Confirm with user. | Determines class and method names too |

---

## Step 2 — Marks & Class Layout

### Import block (copy exactly — add only what the test uses)

```python
import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1, skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)
```

> Add UI imports only when `via_ui` is needed:
> ```python
> from ocs_ci.helpers.dr_helpers_ui import (
>     dr_submariner_validation_from_ui,
>     check_cluster_status_on_acm_console,
>     failover_relocate_ui,
> )
> from ocs_ci.ocs.acm.acm import AcmAddClusters
> ```

### Class-level marks (MUST all appear — in this order)

```python
@rdr              # ALWAYS — filters test to RDR clusters only via conftest hook
@tier1            # or @tier2 / @tier4a / @tier4b — pick one
@turquoise_squad  # ALWAYS for RDR tests
class TestMyScenario:
```

**Optional class-level marks** — add above `@rdr` when applicable:

| Mark | Import path | When to use |
|---|---|---|
| `@skipif_ocs_version("<4.XX")` | `ocs_ci.framework.testlib` | Gate the entire class on an OCS version |
| `@dr_hub_recovery` | `ocs_ci.framework.pytest_customization.marks` | Hub-recovery tests (replaces `@rdr` — do NOT use both) |
| `@mdr` | `ocs_ci.framework.pytest_customization.marks` | Metro DR tests (separate topology from RDR) |
| `@hcp_required` | `ocs_ci.framework.pytest_customization.marks` | Skip-unless test targets a Hosted Control Plane environment |
| `@pytest.mark.skip_rdr_health_check` | built-in `pytest.mark` | Suppress the autouse `rdr_health_check` fixture for this class/method |
| `@pytest.mark.order("last")` | `pytest-ordering` | Force test to run after all others (required for hub-recovery) |

> ⚠️ `@dr_hub_recovery` is **not** combined with `@rdr`. Hub-recovery tests use `@dr_hub_recovery` **instead** of `@rdr`.
> `@hcp_required` is a `pytest.mark.skipif` wrapper — it gates the test to environments where at least one cluster has `is_hosted: true`.

**Per-`pytest.param` marks** (never class-level):
- `acceptance` — marks a case as an acceptance gate
- `pytest.mark.polarion_id("OCS-XXXX")` — required per param
- `skipif_ocs_version("<4.XX")` — gate a specific param variant

### Parametrization pattern

```python
@pytest.mark.parametrize(
    argnames=["primary_cluster_down", "pvc_interface"],
    argvalues=[
        pytest.param(
            False,
            constants.CEPHBLOCKPOOL,
            marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
            id="primary_up-rbd",
        ),
        pytest.param(
            True,
            constants.CEPHBLOCKPOOL,
            marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
            id="primary_down-rbd",
        ),
        pytest.param(
            False,
            constants.CEPHFILESYSTEM,
            marks=[skipif_ocs_version("<4.19"), acceptance, pytest.mark.polarion_id("OCS-XXXX")],
            id="primary_up-cephfs",
        ),
        pytest.param(
            True,
            constants.CEPHFILESYSTEM,
            marks=[skipif_ocs_version("<4.19"), pytest.mark.polarion_id("OCS-XXXX")],
            id="primary_down-cephfs",
        ),
    ],
)
def test_my_scenario(self, primary_cluster_down, pvc_interface, dr_workload, ...):
```

> **Rules:**
> - `marks=acceptance` (bare, no list) when it is the only mark.
> - `marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")]` when there are multiple.
> - `id` must be unique and descriptive — it appears in pytest output and CI reports.

---

## Step 3 — Core Test Skeleton

This skeleton covers the **standard subscription + appset failover-then-relocate** scenario. It is the pattern used in `test_failover_and_relocate.py` and `test_sequential_failover.py`. All other workload types derive from it.

```python
def test_failover_and_relocate(
    self,
    primary_cluster_down,
    pvc_interface,
    dr_workload,
    nodes_multicluster,
    node_restart_teardown,
):
    """
    Verify application failover when primary cluster is UP or DOWN, then relocate
    back to primary.

    Steps:
        1. Deploy appset (+ optional subscription) workloads on primary cluster.
        2. (CephFS only) Verify ReplicationDestination resources on secondary.
        3. Wait 2× scheduling interval for initial IOs.
        4. Record lastGroupSyncTime BEFORE failover (gates the action).
        5. (Optional) Stop primary cluster nodes.
        6. Trigger failover to secondary for all workloads.
        7. Verify all resources created on secondary.
        8. (If primary was down) Restore primary nodes; wait for health.
        9. Verify all resources deleted from primary.
        10. (CephFS) Verify ReplicationDestination lifecycle post-failover.
        11. (RBD) Wait for mirroring status OK.
        12. Wait 2× scheduling interval; verify lastGroupSyncTime post-failover.
        13. Verify lastGroupSyncTime BEFORE relocate (gates the action).
        14. Trigger relocate back to primary for all workloads.
        15. Verify resources deleted from secondary and created on primary.
        16. (CephFS/RBD) Repeat storage checks post-relocate.
        17. Verify final lastGroupSyncTime after relocate.
    """
    # ── 1. Deploy workloads ──────────────────────────────────────────────────
    # AppSet is the primary workload type; subscription is included when the
    # test needs to cover both types simultaneously.
    # Subscription-only: dr_workload(num_of_subscription=1, num_of_appset=0, ...)
    # AppSet-only:       dr_workload(num_of_subscription=0, num_of_appset=1, ...)
    workloads = dr_workload(
        num_of_subscription=1, num_of_appset=1, pvc_interface=pvc_interface
    )
    # Build DRPC objects — subscription uses workload_namespace; appset uses
    # GITOPS_CLUSTER_NAMESPACE + "{appset_placement_name}-drpc"
    drpc_subscription = DRPC(namespace=workloads[0].workload_namespace)
    drpc_appset = DRPC(
        namespace=constants.GITOPS_CLUSTER_NAMESPACE,
        resource_name=f"{workloads[1].appset_placement_name}-drpc",
    )
    drpc_objs = [drpc_subscription, drpc_appset]

    # ── 2. Identify clusters ─────────────────────────────────────────────────
    primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
        workloads[0].workload_namespace
    )
    config.switch_to_cluster_by_name(primary_cluster_name)
    primary_cluster_index = config.cur_index
    primary_cluster_nodes = get_node_objs()
    secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
        workloads[0].workload_namespace
    )

    # ── 3. CephFS pre-check: ReplicationDestination on secondary ────────────
    if pvc_interface == constants.CEPHFILESYSTEM:
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in workloads:
            if dr_helpers.is_cg_cephfs_enabled():
                dr_helpers.wait_for_resource_existence(
                    kind=constants.REPLICATION_GROUP_DESTINATION,
                    namespace=wl.workload_namespace,
                    should_exist=True,
                )
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=wl.workload_namespace,
                    expected_count=wl.workload_pvc_count,
                )
            dr_helpers.wait_for_replication_destinations_creation(
                wl.workload_pvc_count, wl.workload_namespace
            )

    # ── 4. Wait for initial IOs and record sync time ─────────────────────────
    scheduling_interval = dr_helpers.get_scheduling_interval(
        workloads[0].workload_namespace
    )
    wait_time = 2 * scheduling_interval  # minutes
    logger.info(f"Waiting for {wait_time} minutes to run IOs")
    sleep(wait_time * 60)

    before_failover_sync_times = [
        dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
        for obj in drpc_objs
    ]
    logger.info("Verified lastGroupSyncTime before failover.")

    # ── 5. [SCENARIO-SPECIFIC FAULT INJECTION GOES HERE] ────────────────────

    # ── 6. Stop primary if required ──────────────────────────────────────────
    if primary_cluster_down:
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
        nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

    # ── 7. Failover ──────────────────────────────────────────────────────────
    for wl in workloads:
        dr_helpers.failover(
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

    # ── 8. Verify resources created on secondary ─────────────────────────────
    config.switch_to_cluster_by_name(secondary_cluster_name)
    for wl in workloads:
        dr_helpers.wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            wl.workload_namespace,
            performed_dr_action=True,
        )

    # ── 9. Restore primary if it was stopped ─────────────────────────────────
    config.switch_to_cluster_by_name(primary_cluster_name)
    if primary_cluster_down:
        logger.info(
            f"Waiting for {wait_time} minutes before starting nodes of "
            f"primary cluster: {primary_cluster_name}"
        )
        sleep(wait_time * 60)
        nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
        wait_for_nodes_status([node.name for node in primary_cluster_nodes])
        logger.info("Wait for 180 seconds for pods to stabilize")
        sleep(180)
        logger.info("Wait for all pods in openshift-storage to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()

    # ── 10. Verify resources deleted from primary ────────────────────────────
    for wl in workloads:
        dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

    # ── 11. Post-failover storage checks ────────────────────────────────────
    # CephFS: verify ReplicationDestination lifecycle (see Step 5)
    # RBD:
    if pvc_interface == constants.CEPHBLOCKPOOL:
        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(wl.workload_pvc_count for wl in workloads)
        )

    # ── 12. Wait for IOs and verify lastGroupSyncTime post-failover ──────────
    logger.info(f"Waiting for {wait_time} minutes to run IOs")
    sleep(wait_time * 60)

    post_failover_sync_times = []
    for obj, t in zip(drpc_objs, before_failover_sync_times):
        post_failover_sync_times.append(
            dr_helpers.verify_last_group_sync_time(obj, scheduling_interval, t)
        )
    logger.info("Verified lastGroupSyncTime after failover.")

    # ── 13. Verify lastGroupSyncTime BEFORE relocate ─────────────────────────
    # This gates the relocate action — do not skip. Mirrors the pattern used
    # before failover. Uses the timestamps captured in step 12 as baseline.
    before_relocate_sync_times = []
    for obj in drpc_objs:
        before_relocate_sync_times.append(
            dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
        )
    logger.info("Verified lastGroupSyncTime before relocate.")

    # ── 14. Relocate ─────────────────────────────────────────────────────────
    for wl in workloads:
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=wl.workload_namespace,
            workload_type=wl.workload_type,
            workload_placement_name=(
                wl.appset_placement_name
                if wl.workload_type == constants.APPLICATION_SET
                else None
            ),
        )

    # ── 15. Verify resources deleted from secondary ──────────────────────────
    config.switch_to_cluster_by_name(secondary_cluster_name)
    for wl in workloads:
        dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

    # ── 16. Verify resources created on primary ──────────────────────────────
    config.switch_to_cluster_by_name(primary_cluster_name)
    for wl in workloads:
        dr_helpers.wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            wl.workload_namespace,
            performed_dr_action=True,
        )

    # Post-relocate storage checks (CephFS: see Step 5; RBD):
    if pvc_interface == constants.CEPHBLOCKPOOL:
        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(wl.workload_pvc_count for wl in workloads)
        )

    # ── 17. Final lastGroupSyncTime check ────────────────────────────────────
    # Uses before_relocate_sync_times as the baseline (not post_failover_sync_times)
    for obj, t in zip(drpc_objs, before_relocate_sync_times):
        dr_helpers.verify_last_group_sync_time(obj, scheduling_interval, t)
    logger.info("Verified lastGroupSyncTime after relocate.")
```

---

## Step 4 — Conftest Fixtures Reference

These fixtures are **auto-registered** by `conftest.py` in the `regional-dr/` directory. Never redefine them in the test file.

### Function-scoped (request per test)

| Fixture | Signature | Purpose |
|---|---|---|
| `dr_workload` | `dr_workload(num_of_subscription=N, num_of_appset=N, pvc_interface=...)` | Deploy subscription + appset workloads. Returns list of workload objects. |
| `discovered_apps_dr_workload` | `discovered_apps_dr_workload(pvc_interface=..., kubeobject=1, recipe=1)` | Deploy discovered-apps workloads. |
| `cnv_dr_workload` | `cnv_dr_workload(num_of_vm_subscription=1, num_of_vm_appset_push=1, num_of_vm_appset_pull=1, vm_type=...)` | Deploy CNV VM workloads. |
| `nodes_multicluster` | `nodes_multicluster[cluster_index]` | Returns node-management object per cluster index. Access via `nodes_multicluster[primary_cluster_index].stop_nodes(...)`. |
| `node_restart_teardown` | — | Auto-restarts nodes if the test fails mid-way. Always add for any test that stops nodes. |
| `node_drain_teardown` | — | Ensures drained nodes are re-scheduled after test. |
| `setup_acm_ui` | — | Initializes ACM UI session. Required for UI tests. |
| `scale_deployments` | `scale_deployments("down")` / `scale_deployments("up")` | Scale ODF/Submariner deployments up or down. Has teardown to restore. |
| `rdr_health_check` | autouse | Runs before every test: Ceph health, rbd-mirror status, mirroring health check. Skip with `@pytest.mark.skip_rdr_health_check`. |

### Session-scoped (auto-applied once per run)

| Fixture | Purpose |
|---|---|
| `setup_odf_cli_binary` | Downloads and sets up the `odf-cli` binary. |
| `update_odf_cli_dr_kubeconfigs` | Updates odf-cli kubeconfig for DR clusters. |
| `check_subctl_cli` | Ensures `subctl` binary is present (Submariner). |
| `check_gitops_secret_offline_mode` | Creates GitOps private repo secret if missing. |
| `cnv_hyperconverged_installed_on_dr_clusters` | Skips CNV tests if HyperConverged CR is absent. Autouse; active only for `CNV_TEST_FILES`. |
| `get_virtctl` | Downloads `virtctl` binary. |

---

## Step 5 — CephFS-Specific Checks

Insert these blocks **at the two verification points** (post-failover and post-relocate). They replace or supplement the RBD mirroring check when `pvc_interface == constants.CEPHFILESYSTEM`.

### After failover — on the now-current secondary (= old primary)

```python
if pvc_interface == constants.CEPHFILESYSTEM:
    for wl in workloads:
        # Old secondary (now active): verify RGD is gone
        config.switch_to_cluster_by_name(secondary_cluster_name)
        cg_enabled = dr_helpers.is_cg_cephfs_enabled()
        if cg_enabled:
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=wl.workload_namespace,
                should_exist=False,
            )
            dr_helpers.wait_for_replication_destinations_deletion(
                wl.workload_namespace
            )

        # New secondary (old primary): verify RGD + RepDest + VolumeSnapshot created
        config.switch_to_cluster_by_name(primary_cluster_name)
        if cg_enabled:
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=wl.workload_namespace,
                should_exist=True,
            )
            dr_helpers.wait_for_replication_destinations_creation(
                wl.workload_pvc_count, wl.workload_namespace
            )
            dr_helpers.wait_for_resource_count(
                kind=constants.VOLUMESNAPSHOT,
                namespace=wl.workload_namespace,
                expected_count=wl.workload_pvc_count,
            )
```

### After relocate — on the restored primary

```python
if pvc_interface == constants.CEPHFILESYSTEM:
    for wl in workloads:
        # Old secondary (now primary) should have RepDest deleted
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_replication_destinations_deletion(wl.workload_namespace)
        cg_enabled = dr_helpers.is_cg_cephfs_enabled()
        if cg_enabled:
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=wl.workload_namespace,
                should_exist=False,
            )

        # Current secondary should have RepDest + RGD recreated
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_replication_destinations_creation(
            wl.workload_pvc_count, wl.workload_namespace
        )
        if cg_enabled:
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=wl.workload_namespace,
                should_exist=True,
            )
            dr_helpers.wait_for_resource_count(
                kind=constants.VOLUMESNAPSHOT,
                namespace=wl.workload_namespace,
                expected_count=wl.workload_pvc_count,
            )
```

---

## Step 6 — File, Class, and Method Naming

| Item | Convention | Example |
|---|---|---|
| File | `test_<scenario_snake_case>.py` | `test_failover_after_network_partition.py` |
| Directory | Always `tests/functional/disaster-recovery/regional-dr/` | — |
| Class | `Test<ScenarioPascalCase>` | `TestFailoverAfterNetworkPartition` |
| Method | `test_<scenario_snake_case>` | `test_failover_after_network_partition` |
| Param ID | `<primary_state>-<pvc_type>[-<variant>]` | `primary_down-rbd-cli`, `primary_up-cephfs-ui` |

---

## Step 7 — Write and Validate

1. Use `write_file` to create the complete test file.
2. Confirm the checklist below before reporting done:

### Pre-submit checklist

- [ ] `@rdr`, `@turquoise_squad`, and a tier mark appear on the class
- [ ] Every `pytest.param` has a `polarion_id` mark (placeholder `OCS-XXXX` is acceptable)
- [ ] Every symbol imported is actually used
- [ ] `DRPC` objects are constructed correctly: subscription uses `namespace=wl.workload_namespace`; appset uses `namespace=constants.GITOPS_CLUSTER_NAMESPACE, resource_name=f"{wl.appset_placement_name}-drpc"`
- [ ] `primary_cluster_down=True` paths have `skip_odf_cli_validation=True` in `failover()`
- [ ] `node_restart_teardown` fixture is included whenever nodes are stopped
- [ ] CephFS and RBD storage checks are both present and conditional
- [ ] `verify_last_group_sync_time` is called **before failover** AND **before relocate** (not only after)
- [ ] Discovered-apps tests call `verify_last_kubeobject_protection_time` before failover AND before relocate
- [ ] Docstring lists numbered steps matching the actual code

3. Run syntax check:

```bash
python -m py_compile tests/functional/disaster-recovery/regional-dr/test_<name>.py
```

Use `execute_command` and confirm zero output (exit 0) before marking the file complete.

---

## Addendum A — Discovered Apps Workload

**Key differences from the core skeleton:**

### Workload deployment

```python
rdr_workloads = discovered_apps_dr_workload(
    pvc_interface=pvc_interface, kubeobject=1, recipe=1
)
first_workload = rdr_workloads[0]
```

### DRPC construction — different namespace and resource_name

```python
drpc_objs = [
    DRPC(
        namespace=constants.DR_OPS_NAMESPACE,
        resource_name=wl.discovered_apps_placement_name,
    )
    for wl in rdr_workloads
]
```

### Cluster identification — pass `discovered_apps=True`

```python
primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
    first_workload.workload_namespace,
    discovered_apps=True,
    resource_name=first_workload.discovered_apps_placement_name,
)
secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
    first_workload.workload_namespace,
    discovered_apps=True,
    resource_name=first_workload.discovered_apps_placement_name,
)
scheduling_interval = dr_helpers.get_scheduling_interval(
    first_workload.workload_namespace,
    discovered_apps=True,
    resource_name=first_workload.discovered_apps_placement_name,
)
```

### Sync checks BEFORE failover — both group sync AND kubeobject protection

> Discovered-apps tests check **both** time fields before triggering any DR action.
> Check them after the initial IO wait, before stopping nodes or triggering failover.

```python
wait_time = 2 * scheduling_interval  # minutes
logger.info(f"Waiting for {wait_time} minutes to run IOs")
sleep(wait_time * 60)

logger.info("Checking for lastKubeObjectProtectionTime before failover")
for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
    dr_helpers.verify_last_kubeobject_protection_time(
        drpc_obj, rdr_workload.kubeobject_capture_interval_int
    )

logger.info("Checking for lastGroupSyncTime before failover")
for drpc_obj in drpc_objs:
    dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
```

### Sync checks BEFORE relocate — both fields again

> After the post-failover IO wait, re-verify both fields before triggering relocate.
> The pattern from `test_failover_and_relocate_discovered_apps_workloads.py`:

```python
# After post-failover IO wait:
logger.info("Checking for lastKubeObjectProtectionTime before relocate")
for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
    dr_helpers.verify_last_kubeobject_protection_time(
        drpc_obj, rdr_workload.kubeobject_capture_interval_int
    )

logger.info("Checking for lastGroupSyncTime before relocate")
for drpc_obj in drpc_objs:
    dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
```

### Failover — pass `discovered_apps=True` and `old_primary`

```python
for rdr_workload in rdr_workloads:
    dr_helpers.failover(
        failover_cluster=secondary_cluster_name,
        namespace=rdr_workload.workload_namespace,
        discovered_apps=True,
        workload_placement_name=rdr_workload.discovered_apps_placement_name,
        old_primary=primary_cluster_name,
        skip_odf_cli_validation=primary_cluster_down,
    )
```

### Post-failover cleanup — mandatory for discovered apps

```python
for rdr_workload in rdr_workloads:
    dr_helpers.do_discovered_apps_cleanup(
        drpc_name=rdr_workload.discovered_apps_placement_name,
        old_primary=primary_cluster_name,
        workload_namespace=rdr_workload.workload_namespace,
        workload_dir=rdr_workload.workload_dir,
        vrg_name=rdr_workload.discovered_apps_placement_name,
    )
```

### Resource creation verification — extra params required

```python
config.switch_to_cluster_by_name(secondary_cluster_name)
for rdr_workload in rdr_workloads:
    dr_helpers.wait_for_all_resources_creation(
        rdr_workload.workload_pvc_count,
        rdr_workload.workload_pod_count,
        rdr_workload.workload_namespace,
        timeout=1200,                 # longer timeout for discovered apps
        discovered_apps=True,
        vrg_name=rdr_workload.discovered_apps_placement_name,
        performed_dr_action=True,
    )

if pvc_interface == constants.CEPHBLOCKPOOL:
    dr_helpers.wait_for_mirroring_status_ok(
        replaying_images=sum(wl.workload_pvc_count for wl in rdr_workloads)
    )
```

> Call `wait_for_mirroring_status_ok` **after** resource creation verification, **both** after failover and after relocate. Use the same `if pvc_interface == constants.CEPHBLOCKPOOL` guard — never call it for CephFS workloads.

### Relocate — pass `discovered_apps=True` and `old_primary`

```python
for rdr_workload in rdr_workloads:
    dr_helpers.relocate(
        preferred_cluster=primary_cluster_name,
        namespace=rdr_workload.workload_namespace,
        workload_placement_name=rdr_workload.discovered_apps_placement_name,
        discovered_apps=True,
        old_primary=secondary_cluster_name,
        workload_instance=rdr_workload,
    )
```

### Sync checks AFTER relocate — both fields one final time

```python
logger.info("Checking for lastKubeObjectProtectionTime post relocate")
for drpc_obj, rdr_workload in zip(drpc_objs, rdr_workloads):
    dr_helpers.verify_last_kubeobject_protection_time(
        drpc_obj, rdr_workload.kubeobject_capture_interval_int
    )
```

### OCS ≥ 4.22 — ApplicationCleanupPending alert verification

```python
from ocs_ci.utility.version import get_semantic_ocs_version_from_config
from semantic_version import Version
from ocs_ci.helpers.dr_helpers_ui import (
    verify_pending_cleanup_alert_firing,
    verify_pending_cleanup_alert_resolved,
)

if get_semantic_ocs_version_from_config() >= Version("4.22", partial=True):
    wait_time_for_alert = constants.ALERT_APPLICATION_CLEANUP_PENDING_THRESHOLD + 180
    sleep(wait_time_for_alert)
    acm_obj = AcmAddClusters()
    for rdr_workload in rdr_workloads:
        verify_pending_cleanup_alert_firing(
            acm_obj, "Failover",   # or "Relocate"
            drpc_name=rdr_workload.discovered_apps_placement_name,
        )
    # ... cleanup ...
    for rdr_workload in rdr_workloads:
        verify_pending_cleanup_alert_resolved(
            acm_obj, "Failover",
            drpc_name=rdr_workload.discovered_apps_placement_name,
        )
```

---

## Addendum B — CNV Workload

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

---

## Addendum C — Node Fault / Concurrent Operations

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

# Trigger failover while drain is in progress
config.switch_to_cluster_by_name(primary_cluster_name)
dr_helpers.failover(...)

# After verification, assert drain completed cleanly
drain_operation.result()
schedule_nodes([node_name])
```

### Tier for fault tests

Use `@tier4b` (not `@tier1`) — these are stress/disruptive scenarios.

---

## Addendum D — ACM UI Flow

**Used in `test_failover_and_relocate.py` `via_ui=True` params.**

### Required fixtures

Add `setup_acm_ui` to the method signature.

### UI setup

```python
if via_ui:
    acm_obj = AcmAddClusters()
```

### Before stopping cluster — validate submariner from UI

```python
if via_ui:
    config.switch_acm_ctx()
    dr_submariner_validation_from_ui(acm_obj)
```

### After cluster is down — verify cluster marked Unknown in ACM

```python
if via_ui and primary_cluster_down:
    config.switch_acm_ctx()
    check_cluster_status_on_acm_console(
        acm_obj,
        down_cluster_name=primary_cluster_name,
        expected_text="Unknown",
    )
```

### Failover via UI

```python
if via_ui:
    failover_relocate_ui(
        acm_obj,
        scheduling_interval=scheduling_interval,
        workload_to_move=f"{wl.workload_name}-1",
        policy_name=wl.dr_policy_name,
        failover_or_preferred_cluster=secondary_cluster_name,
        workload_type=wl.workload_type,
    )
```

### Relocate via UI

```python
if via_ui:
    check_cluster_status_on_acm_console(acm_obj)
    dr_submariner_validation_from_ui(acm_obj)
    failover_relocate_ui(
        acm_obj,
        scheduling_interval=scheduling_interval,
        workload_to_move=f"{wl.workload_name}-1",
        policy_name=wl.dr_policy_name,
        failover_or_preferred_cluster=primary_cluster_name,
        action=constants.ACTION_RELOCATE,
        workload_type=wl.workload_type,
    )
```

### Important: UI and CLI share the same verification steps

After the UI action triggers failover/relocate, the resource creation/deletion verification code is identical to the CLI path. Do not duplicate — use `if via_ui` only around the action trigger, not the verification.

---

## Addendum E — Advanced Patterns

### Structured step logging with `logger.test_step`

Some tests (e.g. `test_rdr_bug_verification.py`, `test_cnv_failover_and_relocate_discovered_apps.py`) use `logger.test_step` instead of plain `logger.info` for major test phases. This produces structured output in CI logs, making test steps trivially searchable in ReportPortal.

```python
logger.test_step("Deploy GitOps/ApplicationSet workload")
logger.test_step("Write initial data to VMs and record checksums")
logger.test_step(f"Failover to secondary cluster {secondary_cluster_name}")
logger.test_step("Validate data integrity after failover")
logger.test_step("Recover the primary managed cluster")
logger.test_step("Relocate workloads back to original primary cluster")
logger.test_step("Validate data integrity after relocate")
```

> Use `logger.test_step` for any test that has ≥6 major phases, especially disruptive or CNV tests. Use plain `logger.info` for minor intermediate status messages within a phase.

---

### Waiting for DRPC progression to complete

After a failover or relocate that goes through the ACM hub context (e.g. discovered-apps, CNV discovered tests), wait for the DRPC's progression status to reach `COMPLETED` before issuing the next action or checking sync times:

```python
config.switch_acm_ctx()
drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)
```

> Only needed when driving DR actions through the hub context. Standard CLI subscription/appset tests do not need this — the `failover()` and `relocate()` helpers already poll for completion internally.

---

### Class-level parametrize (single-method classes)

When a class has exactly one test method, `@pytest.mark.parametrize` can be placed on the class itself rather than the method. This is seen in `test_sequential_relocate.py`:

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

> Use this when ALL methods in the class share the same parametrize axes. For classes with multiple test methods with different axes, keep `@pytest.mark.parametrize` on each method.

---

### Parallel (concurrent) DR actions with `ThreadPoolExecutor`

For sequential failover/relocate tests that trigger operations on multiple workloads concurrently, use `ThreadPoolExecutor` with a short stagger between submissions:

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

---

## Addendum F — Mixed PVC Interface Tests (RBD + CephFS simultaneously)

Several tests deploy **two separate workload groups** in one test — one for RBD and one for CephFS — so that both storage types are exercised in a single failover/relocate cycle. Used in `test_failover_after_multiple_pods_failure.py` and `test_site_failure_recovery_and_failover.py`.

```python
# Deploy RBD workloads
rbd_workloads = dr_workload(
    num_of_subscription=1, num_of_appset=1,
    pvc_interface=constants.CEPHBLOCKPOOL,
)
# Deploy CephFS workloads separately (same dr_workload fixture, called twice)
cephfs_workloads = dr_workload(
    num_of_subscription=1, num_of_appset=1,
    pvc_interface=constants.CEPHFILESYSTEM,
)
all_workloads = rbd_workloads + cephfs_workloads
```

### Per-workload interface dispatch

Because workloads have a `pvc_interface` attribute, you can branch per-workload instead of using a top-level `if pvc_interface ==` check:

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

> Tier for these tests is usually `@tier4` + `@tier4c` (multiple simultaneous fault injection).

---

## Addendum G — Hub Recovery Tests

Used in `test_site_failure_recovery_and_failover.py` and `test_neutral_hub_failure_and_recovery.py`. These are the most complex tests in the suite. Key differences:

### Required class-level marks

```python
@tier4a    # or @tier2 for neutral-hub
@turquoise_squad
@dr_hub_recovery          # REQUIRED — gates test to hub-recovery environments
@pytest.mark.order("last")  # ALWAYS — hub recovery must run after all other tests
class TestHubRecovery:
```

> Note: hub-recovery tests do NOT use `@rdr` — they use `@dr_hub_recovery` instead.

### Workload deployment with `switch_ctx`

Hub-recovery tests deploy workloads on the **passive hub** context:

```python
from ocs_ci.helpers.dr_helpers import get_passive_acm_index

rdr_workload = dr_workload(
    num_of_subscription=1,
    num_of_appset=1,
    pvc_interface=constants.CEPHBLOCKPOOL,
    switch_ctx=get_passive_acm_index(),   # deploy via passive hub
)
```

### Context switches using `switch_ctx(index)` not `switch_to_cluster_by_name`

```python
from ocs_ci.ocs.utils import get_active_acm_index
from ocs_ci.helpers.dr_helpers import get_passive_acm_index

config.switch_ctx(get_active_acm_index())   # switch to active hub
config.switch_ctx(get_passive_acm_index())  # switch to passive hub
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

# 1. Configure hub recovery on the active hub before bringing it down
assert configure_rdr_hub_recovery()

# 2. Stop active hub nodes, then wait before switching to passive
nodes_multicluster[active_hub_index].stop_nodes(active_hub_nodes)
time.sleep(wait_time)

# 3. On passive hub: create KlusterletConfig, restore backup, verify
config.switch_ctx(get_passive_acm_index())
create_klusterlet_config()
restore_backup()
time.sleep(wait_time)
verify_restore_is_completed()

# 4. Wait for surviving managed cluster to be imported
for sample in TimeoutSampler(
    timeout=1800, sleep=15,
    func=validate_cluster_import,
    cluster_name=secondary_cluster_name,
    switch_ctx=get_passive_acm_index(),
):
    if sample:
        break
    raise UnexpectedBehaviour(f"import of {secondary_cluster_name} failed")

# 5. Verify DRPolicy is in Validated state on new hub
verify_drpolicy_cli(switch_ctx=get_passive_acm_index())

# 6. Pass switch_ctx to failover/relocate helpers
dr_helpers.failover(
    failover_cluster=secondary_cluster_name,
    namespace=wl.workload_namespace,
    workload_type=wl.workload_type,
    switch_ctx=get_passive_acm_index(),   # ← required for hub-recovery tests
)

# 7. Remove appliedManifestWorkEvictionGracePeriod after workloads are stable
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
# Write to temp file and apply
config.switch_ctx(get_passive_acm_index())
run_cmd(f"oc apply -f {auto_import_secret_yaml.name}")
```

---

## Addendum H — Per-Cluster Loop Pattern

Some tests iterate over **all non-ACM managed clusters** to perform the same action on each (e.g. node failure, health check, mirroring validation). Used in `test_managed_cluster_node_failure.py` and `test_rdr_bug_verification.py`.

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

When a test needs cleanup logic that is more specific than `node_restart_teardown`, define a `teardown` fixture directly in the class using `request.addfinalizer`:

```python
@pytest.fixture(autouse=True)
def teardown(self, request):
    def finalizer():
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            # e.g. archive ceph crashes, scale up deployments, etc.
            archive_ceph_crashes(get_ceph_tools_pod())
            ceph_health_check(tries=40, delay=60)
    request.addfinalizer(finalizer)
```

> Use `autouse=True` so the finalizer runs for every test method in the class without explicit fixture argument.

### `TimeoutSampler` for polling conditions

Use `TimeoutSampler` instead of `sleep + assert` when waiting for a condition that may take variable time:

```python
from ocs_ci.utility.utils import TimeoutSampler

for sample in TimeoutSampler(
    timeout=600,     # total seconds to wait
    sleep=15,        # poll interval in seconds
    func=some_check_function,
    arg1=value1,
):
    if sample:
        logger.info("Condition met")
        break
    logger.warning("Condition not yet met, retrying...")
```

---

## Addendum I — Scale-Down Fault Injection Pattern

Used in `test_failover_after_multiple_pods_failure.py`. Simulates partial cluster failure by scaling deployments to zero, triggers failover, then restores and verifies.

```python
# 1. Scale down ODF + submariner deployments on primary
config.switch_to_cluster_by_name(primary_cluster_name)
scale_deployments("down")   # fixture from conftest — has teardown
time.sleep(120)              # wait for failure to propagate

# 2. Failover while deployments are down
dr_helpers.failover(...)

# 3. After failover verified, restart nodes to clear sandbox failures
primary_cluster_index = config.cur_index
primary_worker_nodes = get_nodes()
nodes_multicluster[primary_cluster_index].restart_nodes(primary_worker_nodes)

# 4. Scale back up
scale_deployments("up")
wait_for_pods_to_be_running(
    namespace=constants.OPENSHIFT_STORAGE_NAMESPACE, timeout=420, sleep=30
)
wait_for_pods_to_be_running(
    namespace=constants.SUBMARINER_OPERATOR_NAMESPACE, timeout=420, sleep=30
)
ceph_health_check()
```

> Always pass a `timeout` to `wait_for_pods_to_be_running` in fault tests — the default may be too short after node restarts.

---

## Addendum J — Helper Utilities Quick Reference

These helpers are used across the suite but not yet shown in the core skeleton.

### Context helpers

| Helper | Import | Use |
|---|---|---|
| `dr_helpers.set_current_primary_cluster_context(namespace)` | `from ocs_ci.helpers import dr_helpers` | Switch context to the current primary cluster for a given workload namespace. Shorthand alternative to `get_current_primary_cluster_name` + `switch_to_cluster_by_name`. |
| `config.RunWithPrimaryConfigContext()` | `from ocs_ci.framework import config` | Context manager that temporarily switches to the primary cluster config. Used in 2AZ tests to read node zone labels. |
| `get_non_acm_cluster_config()` | `from ocs_ci.ocs.utils import get_non_acm_cluster_config` | Returns list of all managed cluster config objects (i.e. non-ACM/hub clusters). Use for per-cluster loops. |

### Retry decorator

```python
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed

@retry(CommandFailed, tries=5, delay=10, backoff=1)
def get_zone_nodes_with_retry():
    return get_nodes_having_label(label=zone_label)

zone_nodes = get_zone_nodes_with_retry()
```

> Use `@retry` for any `oc` command that may transiently fail during a node-restart or network-disruption phase.

### Marking a test to skip the RDR health check

The `rdr_health_check` autouse fixture runs before every test. To skip it for a specific test (e.g. a pure UI or non-cluster test):

```python
@pytest.mark.skip_rdr_health_check
def test_mco_operator_rebranding_ui(self, setup_acm_ui):
    ...
```

### Archiving Ceph crashes in teardown

After disruptive tests, Ceph daemon crash warnings may persist. Silence them in teardown to keep health checks clean:

```python
from ocs_ci.utility.utils import archive_ceph_crashes
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

archive_ceph_crashes(get_ceph_tools_pod())
ceph_health_check(tries=40, delay=60)
```

### Disable DR

```python
# Standard workloads
dr_helpers.disable_dr_rdr(discovered_apps=False)

# With discovered apps
dr_helpers.disable_dr_rdr(discovered_apps=True)

# Verify replication resources are deleted after disable
dr_helpers.wait_for_replication_resources_deletion(
    workload.workload_namespace,
    timeout=300,
    check_state=False,
)

# Verify workload pods still running after DR disabled (skip replication CRs)
dr_helpers.wait_for_all_resources_creation(
    workload.workload_pvc_count,
    workload.workload_pod_count,
    workload.workload_namespace,
    skip_replication_resources=True,
)
```

---

## Common Mistakes to Avoid

| Mistake | Correct pattern |
|---|---|
| Using `wl.workload_namespace` for appset DRPC | Appset DRPC uses `constants.GITOPS_CLUSTER_NAMESPACE` + `"{appset_placement_name}-drpc"` |
| Missing `skip_odf_cli_validation=primary_cluster_down` in `failover()` | Must be set when primary is down — otherwise odf-cli validation will fail trying to reach a stopped cluster |
| Calling `wait_for_mirroring_status_ok` for CephFS workloads | Only call it for `CEPHBLOCKPOOL`; CephFS uses `wait_for_replication_destinations_*` instead |
| Stopping nodes without `node_restart_teardown` | Always add this fixture — it is the safety net if the test fails before nodes are restarted |
| Verifying resources before switching cluster context | Always `config.switch_to_cluster_by_name(...)` before any `wait_for_all_resources_*` call |
| Defining fixtures in the test file | All DR fixtures live in `conftest.py` — never redefine them |
| Forgetting `config.switch_acm_ctx()` before UI operations | ACM UI calls must execute against the ACM/hub cluster context, not a managed cluster |
| Using `from ocs_ci.framework.testlib import rdr` | `rdr` is in `ocs_ci.framework.pytest_customization.marks`, not `testlib` |
| Calling `verify_last_group_sync_time` only AFTER failover/relocate | MUST also call it BEFORE each action — this is the gate check that confirms data was in sync before the DR event |
| Skipping `verify_last_kubeobject_protection_time` in discovered-apps tests | Discovered-apps tests MUST check both `lastGroupSyncTime` AND `lastKubeObjectProtectionTime` before failover and before relocate |
| Not calling `.result()` on ThreadPoolExecutor futures | Silently swallows exceptions from concurrent DR operations — always collect results |
| Using `@rdr` on hub-recovery test classes | Hub-recovery tests use `@dr_hub_recovery` instead of `@rdr`; missing `@pytest.mark.order("last")` will run hub recovery mid-suite |
| Using `@rdr` AND `@dr_hub_recovery` together | They are mutually exclusive — use `@dr_hub_recovery` alone for hub-recovery tests |
| Calling `switch_to_cluster_by_name` in hub-recovery tests | Hub-recovery tests use `config.switch_ctx(get_passive_acm_index())` — the passive hub has no name-based lookup |
| Calling `wait_for_mirroring_status_ok` without `timeout` in fault tests | Default timeout may expire before pods recover; always pass an explicit `timeout` (600–1800s) in disruptive tests |
| Missing `skip_replication_resources=True` after `disable_dr_rdr()` | After DR is disabled, replication CRs are gone — pass this flag or the creation check will fail looking for them |
| Putting `if via_ui` around verification code | `if via_ui` guards the action trigger ONLY — resource creation/deletion checks are identical for UI and CLI paths |
| Writing raw XPath/CSS strings in test code | All locators MUST live in `views.py` (`acm_page_nav` or a versioned override); retrieve via `locators_for_current_ocp_version()["acm_page"]` |
| Using `pf-c-` or `pf-v5-` class selectors in new locators | PatternFly class prefixes change between OCP minor versions; use `data-test`, `aria-label`, or `By.ID` instead |
| Calling `acm_obj.do_click()` on canvas-overlaid elements | ACM topology canvas intercepts pointer events; use `acm_obj.click_with_script(locator)` |
| Leaving `use_fallback=True` on negative element checks | AI fallback fires on every miss, wasting minutes; pass `use_fallback=False` for elements that should NOT exist |
| Forgetting `locator[::-1]` when calling `check_element_presence` | `check_element_presence` and `get_elements` expect Selenium-native `(By, value)` order; all other wrappers expect `(value, By)` |
| Omitting `setup_acm_ui` from the fixture signature | Even when some params have `via_ui=False`, `setup_acm_ui` must be declared — it's safe to declare and not use |
| Calling `wait_for_progression_status` without `success_if_deleted=True` in teardown | The DRPC may be deleted before teardown runs; always use the safe variant in `addfinalizer` blocks |
| Constructing DRPC without switching to ACM context first | DRPC constructor auto-calls `switch_acm_ctx()` — you do NOT need to switch manually before `DRPC(...)` |
| Using `drpolicy_obj.data["spec"]["schedulingInterval"]` as an `int` directly | `schedulingInterval` is a string like `"5m"` — use `dr_helpers.get_scheduling_interval()` which parses and returns an `int` |
| Calling `wait_for_peer_ready_status` or `wait_for_clusterdataprotected_status` in standard tests | Only needed after policy changes or cluster recovery — `failover()` / `relocate()` already poll for terminal state internally |

---

## Appendix K — Complete `dr_helpers.py` Function Reference

All functions live in [`ocs_ci/helpers/dr_helpers.py`](ocs_ci/helpers/dr_helpers.py). Import via `from ocs_ci.helpers import dr_helpers` or import individual functions directly.

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
| `failover` | `(failover_cluster, namespace, workload_type=SUBSCRIPTION, workload_placement_name=None, switch_ctx=None, discovered_apps=False, old_primary=None, skip_odf_cli_validation=False)` | `None` | Patches DRPC with `ACTION_FAILOVER`, waits for `STATUS_FAILEDOVER` phase (360s timeout). Skips odf-cli when `skip_odf_cli_validation=True`. Returns immediately if any cluster has `is_hosted=True`. |
| `relocate` | `(preferred_cluster, namespace, workload_type=SUBSCRIPTION, workload_placement_name=None, switch_ctx=None, discovered_apps=False, old_primary=None, workload_instance=None, multi_ns=False, workload_instances_shared=None, vm_auto_cleanup=False, skip_odf_cli_validation=False)` | `None` | Patches DRPC with `ACTION_RELOCATE`, waits for `STATUS_RELOCATED` (1200s). For discovered apps waits for `STATUS_RELOCATING` then auto-calls cleanup unless `vm_auto_cleanup=True`. |

### Mirroring / storage checks

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `check_rbd_mirror_running` | `(namespace=None)` | `bool` | Verifies rbd-mirror daemon deployment has ≥1 ready replica. Used by `rdr_health_check`. |
| `wait_for_mirroring_status_ok` | `(replaying_images=None, replaying_groups=None, timeout=900)` | `bool` | Polls `check_mirroring_status_ok` on every non-ACM cluster. Raises `TimeoutExpiredError` on failure. Skipped when `config.ENV_DATA["skip_mirroring_status_check"]` is set. |
| `check_mirroring_status_for_custom_pool` | `(pool_name, namespace=OPENSHIFT_STORAGE_NAMESPACE, min_replaying=1)` | `bool` | Validates custom `CephBlockPoolRadosNamespace` mirroring health and replaying count. |
| `verify_custom_pool_image_isolation` | `(pool_name)` | — | Asserts RBD images in a custom pool are not present in the default pool on both clusters. |

### Sync-time verification

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `verify_last_group_sync_time` | `(drpc_obj, scheduling_interval, initial_last_group_sync_time=None)` | `str` timestamp | When `initial` given: polls until value changes. Always asserts age `< 3 × scheduling_interval` minutes. Returns current timestamp as next baseline. |
| `verify_last_kubeobject_protection_time` | `(drpc_obj, kubeobject_sync_interval)` | `str` timestamp | Asserts `lastKubeObjectProtectionTime` age `< 2 × kubeobject_sync_interval` minutes. Fails with a Ramen config hint if field is missing (usually a cert issue). |

### Resource wait helpers

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `wait_for_all_resources_creation` | `(pvc_count, pod_count, namespace, timeout=900, skip_replication_resources=False, discovered_apps=False, vrg_name="", skip_vrg_check=False, performed_dr_action=False)` | `None` | Waits for PVCs→Bound, Pods→Running, then replication CRs. Pass `performed_dr_action=True` after DR to validate CG-CephFS VolumeSnapshot count. Pass `skip_replication_resources=True` after `disable_dr_rdr`. |
| `wait_for_all_resources_deletion` | `(namespace, timeout=1500, discovered_apps=False, workload_cleanup=False, vrg_name="", skip_vrg_check=False)` | `None` | Waits for pods, replication CRs, PVCs, PVs deleted. Pass `workload_cleanup=True` for final teardown. |
| `wait_for_cnv_workload` | `(vm_name, namespace, phase=STATUS_RUNNING, timeout=600)` | `None` | Waits for a `VirtualMachineInstance` to reach the specified phase. |
| `wait_for_replication_destinations_creation` | `(rep_dest_count, namespace, timeout=900)` | `None` | CephFS: waits for N `ReplicationDestination` objects to exist. |
| `wait_for_replication_destinations_deletion` | `(namespace, timeout=900)` | `None` | CephFS: waits for all `ReplicationDestination` objects in namespace to be absent. |
| `wait_for_replication_resources_deletion` | `(namespace, timeout, check_state, discovered_apps, vrg_name, skip_vrg_check, workload_cleanup)` | `None` | Lower-level deletion poller for VR/VRG and related CRs. |
| `wait_for_resource_existence` | `(kind, namespace, resource_name="", should_exist=True, timeout=900)` | `None` | Generic poll: waits for a named CR to exist or be absent. |
| `wait_for_resource_count` | `(kind, namespace, expected_count=1, timeout=900)` | `None` | Polls until resource count in namespace equals `expected_count`. |
| `wait_for_resource_state` | `(kind, state, namespace, resource_name="", timeout=900)` | `None` | Polls until resource reaches a target phase/status. |
| `wait_for_vrg_state` | `(vrg_state, vrg_namespace, resource_name, timeout=900)` | `None` | Waits for `VolumeReplicationGroup` to reach a target state. |

### Backend volume helpers

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `get_backend_volumes_for_pvcs` | `(namespace)` | `dict` | Returns PVC name → backend RBD image name mapping. |
| `verify_backend_volume_deletion` | `(backend_volumes, timeout=600)` | — | Asserts backend RBD images have been removed from Ceph on both clusters. |
| `wait_for_backend_volume_deletion` | `(backend_volumes, timeout=600)` | — | Polls until backend images are gone. |

### DR policy and cluster info

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `get_all_drpolicy` | `()` | `list` | Returns `DRPolicy` objects matching current cluster set. Handles HCP by prefixing with `HYPERSHIFT_ADDON_DISCOVERYPREFIX`. |
| `get_all_drclusters` | `()` | `list[str]` | Returns names of all `DRCluster` objects from the hub. |
| `get_dr_topology_clusters` | `()` | `list[str]` | DRCluster names for topology UI validation (excludes `local-cluster`). |
| `get_dr_topology_policy_details` | `()` | `dict` | Returns `{name, connected_clusters, scheduling_interval}` from the first DRPolicy. |
| `validate_drpolicy_replication_ids` | `(drpolicy_name, sc_names)` | — | Validates `groupreplicationID` in DRPolicy `peerClasses` matches SC labels. |
| `validate_vgrc_count` | `()` | — | Validates `VolumeGroupReplicationContent` count across clusters. |
| `verify_drpolicy_cli` | `(switch_ctx=None)` | — | Verifies DRPolicy is in `Validated` state via odf-cli. Used in hub-recovery tests. |
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
| `do_discovered_apps_cleanup` | `(drpc_name, old_primary, workload_namespace, workload_dir, vrg_name, skip_resource_deletion_verification=False)` | — | Removes DRPC, VRG, namespace, and manifests from the old primary after failover/relocate. |
| `do_discovered_apps_cleanup_multi_ns` | `(old_primary, workload_instance)` | — | Same for multi-namespace discovered-apps. |

### ODF CLI validation

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `validate_application_odf_cli` | `(drpc_name, namespace, action="validate", dr_action=None, retries=5, retry_interval=100)` | `str\|None` | Runs `odf dr validate application` or `gather`. Returns `None` immediately when **any** cluster has `is_hosted=True`. |
| `validate_cluster_odf_cli` | `(retries=5, retry_interval=60)` | — | Cluster-level DR config validation via odf-cli. Called by `rdr_health_check`. |
| `update_odf_cli_dr_config_kubeconfigs` | `()` | — | Updates odf-cli kubeconfig paths for all DR clusters. |

### Disconnected / mirror helpers

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `generate_rdr_mirror_images` | `()` | `list[str]` | Clones DR workload repo, extracts all container image refs from `rdr/` YAML. Returns `[]` if `disconnected=False`. |
| `apply_itms_to_managed_clusters` | `(itms_file_path)` | — | Applies `ImageTagMirrorSet` YAML to all managed clusters, waits for MCP rollout. |
| `get_cdi_registry_credentials` | `()` | `tuple[str,str]` | Extracts `(username, password)` for mirror registry from cluster pull-secret. |
| `create_cdi_pull_secret` | `(namespace, secret_name="quayadmin")` | — | Creates Opaque secret with `accessKeyId`/`secretKey` for CDI registry auth. |
| `fetch_mirror_registry_cert` | `()` | `str` | Fetches TLS cert from `config.DEPLOYMENT["mirror_registry"]` via openssl. |
| `create_cdi_cert_configmap` | `(namespace, configmap_name="user-ca-bundle")` | — | Creates ConfigMap containing mirror registry CA cert for CDI trust. |
| `create_ingress_cert_dr` | `()` | — | Builds combined ingress CA bundle from all non-HCP clusters, applies ConfigMap + proxy patch. Skips HCP clusters. Called during deployment, not in tests. |

### Miscellaneous

| Function | Signature | Returns | What it does |
|---|---|---|---|
| `verify_volsync` | `()` | — | Verifies volsync pod running in `volsync-system` on every managed cluster. |
| `verify_cluster_data_protected_status` | `(workload_type, namespace, workload_placement_name=None)` | — | Polls DRPC until `clusterDataProtected` is `True`. |
| `disable_dr_rdr` | `(discovered_apps=False)` | — | Removes DR protection. Follow with `wait_for_replication_resources_deletion` and `wait_for_all_resources_creation(skip_replication_resources=True)`. |
| `is_cg_cephfs_enabled` | `()` | `bool` | `True` if CG is enabled for CephFS on the current cluster. |
| `is_cg_enabled` | `()` | `bool` | `True` if CG is enabled for any storage class. |
| `validate_protection_label` | `(kind, namespace, protection_name=None)` | — | Validates workload has the correct DR protection label. |
| `add_label_to_appsub` | `(workloads, label="test", value="test1")` | — | Adds label to application subscription resources. |

---

## Appendix L — HCP (Hosted Control Plane) with RDR

HCP (Hosted Clusters / Hypershift) changes several RDR behaviours. The key config flag is `is_hosted: true` in the cluster's `MULTICLUSTER` block.

### How HCP clusters differ from standard managed clusters

| Behaviour | Standard cluster | HCP cluster |
|---|---|---|
| `validate_application_odf_cli` | Runs normally | **Auto-skipped** — returns `None` immediately when any `is_hosted=True` |
| `create_ingress_cert_dr` | Builds + applies trust bundle | **Skipped** — HCP shares hosting cluster's ingress CA |
| DRPolicy cluster name | Bare cluster name | `{HYPERSHIFT_ADDON_DISCOVERYPREFIX}-{cluster_name}` |
| `MachineConfigPool` rollout waits | Required after ITMS/proxy changes | **Skipped** — HCP has no independent MachineConfigPool |
| Node stop/start operations | Standard IaaS calls via `nodes_multicluster` | HCP workers are managed by the hosting cluster — verify platform support before using |

### Detecting HCP at test time

```python
# Check if ANY cluster in the current run is hosted
is_any_hosted = any(
    c.MULTICLUSTER.get("is_hosted", False)
    for c in config.clusters
)

# Check a specific cluster by name
idx = config.get_cluster_index_by_name("my-cluster")
is_hosted = config.clusters[idx].MULTICLUSTER.get("is_hosted", False)
```

### DRPolicy cluster name for HCP

```python
from ocs_ci.ocs import constants

# How dr_helpers.get_all_drpolicy handles it internally:
managed_cluster_name = (
    f"{constants.HYPERSHIFT_ADDON_DISCOVERYPREFIX}-{cluster_name}"
    if is_hosted
    else cluster_name
)
```

> You never need to apply this prefix in test code — `get_current_primary_cluster_name`, `get_current_secondary_cluster_name`, `failover`, and `relocate` handle it internally.

### ODF CLI auto-skip for HCP

`validate_application_odf_cli` detects `is_hosted=True` and returns `None` before running any command. This is called internally by `failover()` and `relocate()`. **Do not add an explicit HCP guard in test code** — it is already handled.

### Pre-submit checklist for HCP tests

- [ ] No explicit `validate_application_odf_cli` calls in test code (let `failover`/`relocate` handle it)
- [ ] `create_ingress_cert_dr` is NOT called in tests — deployment concern only
- [ ] Node operations verified to be compatible with HCP worker node management
- [ ] `@skipif_ocs_version` gate present if the feature requires a minimum OCS version

---

## Appendix M — Disconnected Mode

In disconnected (air-gapped) environments, workload and ODF images must be mirrored before tests run. The `mirror_rdr_images` session fixture in `conftest.py` handles this **automatically**. Tests themselves require no disconnected guards — **except** CNV tests that need CDI registry authentication.

### How disconnected mode is activated

```python
# These come from the cluster config / env file — not set in test code:
config.DEPLOYMENT["disconnected"] = True
config.DEPLOYMENT["mirror_registry"] = "registry.example.com:5000"
config.DEPLOYMENT["mirror_registry_path"] = "odf-mirror"
```

### What `mirror_rdr_images` (conftest session fixture) does

```
1. generate_rdr_mirror_images()
   → Clones DR workload repo branch
   → Walks rdr/ YAML files, extracts all container image: fields
   → Returns sorted list of unique image refs

2. oc mirror --config imageset-config.yaml docker://<mirror_registry> --v2
   → Mirrors all images to internal registry

3. Reads oc-mirror-workspace/.../itms-oc-mirror.yaml
   → Renames ITMS resource to "odf-generic-0"
   → Calls apply_itms_to_managed_clusters(itms_file_path)
      → oc apply -f <itms_file> on each managed cluster
      → Waits for MachineConfigPool rollout (up to 1800s per cluster)

Short-circuit: if ITMS "itms-generic-0" already exists on ALL managed clusters → skip mirroring entirely
```

### CNV tests in disconnected mode — CDI registry auth

CDI imports VM disk images from the mirror registry. It needs credentials and a CA cert trust:

```python
from ocs_ci.helpers.dr_helpers import (
    create_cdi_pull_secret,
    create_cdi_cert_configmap,
)

# Must exist in workload namespace BEFORE deploying CNV workload
create_cdi_pull_secret(
    namespace=cnv_workload.workload_namespace,
    secret_name="quayadmin",    # must match workload YAML spec.source.registry.secretRef
)
create_cdi_cert_configmap(
    namespace=cnv_workload.workload_namespace,
    configmap_name="user-ca-bundle",
)
```

> The `cnv_dr_workload` fixture already calls these internally when `disconnected=True`. Only call them manually when deploying CNV workloads outside the fixture.

### IDMS on managed clusters (ACM ≥ 2.14)

Applied automatically by `test_deploy_rdr.py` during deployment:
```bash
oc apply -f constants.ACM_BREW_IDMS_YAML
```
Tests never apply IDMS directly.

### Suppressing mirroring status checks

In some disconnected environments mirroring status may not be fully operational during early setup:
```python
config.ENV_DATA["skip_mirroring_status_check"] = True
# wait_for_mirroring_status_ok() returns True immediately
```

### Trust bundle (`create_ingress_cert_dr`)

Called during deployment, **never in tests**. It:
1. Collects ingress CA certs from every non-HCP cluster
2. Calls `fetch_mirror_registry_cert()` to add the registry TLS cert
3. Applies a combined `ConfigMap` + proxy `trustedCA` patch to every non-HCP cluster
4. Waits for MCP rollout

### Disconnected pre-submit checklist

- [ ] Test does NOT call `generate_rdr_mirror_images`, `apply_itms_to_managed_clusters`, or `create_ingress_cert_dr` — those are framework/deployment concerns
- [ ] CNV tests deploying outside `cnv_dr_workload` call `create_cdi_pull_secret` and `create_cdi_cert_configmap` before workload creation
- [ ] `wait_for_mirroring_status_ok` passes an explicit `timeout` (use 900–1800s; disconnected registries may be slower)
- [ ] Both RBD and CephFS workload images exist in the mirror registry when parametrizing on `pvc_interface`

---

## Appendix O — UI Test Automation for RDR

All UI-driven RDR tests in `tests/functional/disaster-recovery/regional-dr/` use the ACM console (PatternFly-based) via Selenium WebDriver. The framework provides a layered abstraction:

```
test code
  └── dr_helpers_ui.py          ← DR-specific UI helpers (failover_relocate_ui, etc.)
        └── AcmAddClusters      ← ACM page-object (base_ui.py BaseUI + acm_ui.py)
              └── BaseUI        ← Selenium driver wrapper (do_click, wait_until_*, etc.)
                    └── LocatorFallback  ← AI-based locator recovery on TimeoutException
```

---

### Locator system

**Never write raw XPath or CSS strings in test code.** All locators live in [`ocs_ci/ocs/ui/views.py`](ocs_ci/ocs/ui/views.py) and are retrieved at runtime via:

```python
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version
from ocs_ci.ocs.ui.helpers_ui import format_locator

acm_loc = locators_for_current_ocp_version()["acm_page"]
```

`locators_for_current_ocp_version()` returns the correct locator dict for the running OCP version — it merges base dicts with version-specific overrides automatically. **Do not hardcode version checks in test code.**

#### Locator tuple format

Every locator is a `(selector_string, By.TYPE)` tuple — note the **reversed order** from Selenium's native `(By.TYPE, selector_string)`:

```python
# In views.py (definition):
"cluster_name": ("//a[normalize-space()='{}']", By.XPATH)

# In test/helper code (usage):
acm_obj.do_click(format_locator(acm_loc["cluster_name"], cluster_name))
# format_locator fills {} placeholders:
# → ("//a[normalize-space()='my-cluster']", By.XPATH)
```

`BaseUI.do_click` and `WebDriverWait` both internally call `(locator[1], locator[0])` — i.e., they un-reverse the tuple. Never pass `(By.XPATH, "...")` directly to these wrappers.

#### Presence checks use `locator[::-1]`

When checking element presence (not clicking), the framework uses the **reversed** tuple:

```python
# check_element_presence expects (By, value) — the Selenium-native order
acm_obj.check_element_presence(acm_loc["some-key"][::-1], timeout=5, use_fallback=False)
```

This reversal is a project-wide convention — any time you call `check_element_presence` or `get_elements` directly, reverse the locator.

---

### Version-compatible selectors — rules

| Rule | Rationale |
|---|---|
| Use `data-test` or `data-test-id` CSS selectors first | These are stable OCS-CI test IDs — team controls them across upgrades |
| Use `aria-label` as second choice | ARIA attributes are PatternFly-mandated and survive component refactors |
| Use `By.ID` when the element has a stable `id` attribute | IDs are the most stable selector type |
| Use `By.XPATH` with `normalize-space()` for text matching | Trims invisible whitespace that breaks `text()=` in PatternFly components |
| Avoid class-based selectors (`pf-c-`, `pf-v5-`) | PatternFly versions class prefixes between OCP minor releases |
| Avoid positional XPath (`//div[3]`, `td[2]`) | DOM structure changes across PF4→PF5→PF6 migrations |

#### Good vs bad selector examples

```python
# GOOD — data-test attribute (stable, version-independent)
"kebab-action": ('button[data-test="kebab-button"]', By.CSS_SELECTOR)

# GOOD — aria-label (PatternFly-required attribute)
"modal_dialog_close_button": ("//button[@aria-label='Close']", By.XPATH)

# GOOD — normalize-space() for text matching
"Infrastructure": (
    "//button[normalize-space()='Infrastructure' and contains(@class, 'c-nav__link')]",
    By.XPATH,
)

# GOOD — data-test-id (project-owned, stable)
"search_operator_installed": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR)

# BAD — class-based selector (breaks PF4→PF5)
"Import_mode": ('button[class*="c-select__toggle"]', By.CSS_SELECTOR)  # pf-c- prefix

# BAD — positional XPath (breaks on DOM changes)
"second_dropdown_item": (
    '//a[@data-test="dropdown-menu-item-link"]/../../li[2]',
    By.XPATH
)
```

#### Adding new locators to views.py

Always add to the **base dict** (`acm_page_nav`) if the element exists across all supported OCP versions. Add to a versioned override dict (e.g., `acm_page_nav_419`) **only** if the element was introduced or changed in that version:

```python
# views.py — base dict (exists in all versions)
acm_page_nav = {
    ...
    "my-new-button": ('button[data-test="my-new-action"]', By.CSS_SELECTOR),
}

# views.py — version-specific override (only differs in 4.20+)
acm_page_nav_420 = {
    ...
    "my-new-button": ('button[data-test="my-new-action-v2"]', By.CSS_SELECTOR),
}
```

The `locators_for_current_ocp_version()` merger picks up the override automatically.

---

### AI-based locator fallback (`use_fallback`)

`BaseUI` has a built-in `LocatorFallback` that fires when a locator times out — it asks an LLM to suggest a corrected locator from the live page DOM. This is **transparent to test code** but important to understand:

```python
# Default: use_fallback=True — fallback fires on TimeoutException
acm_obj.do_click(acm_loc["some-key"])

# Disable fallback explicitly when:
# 1. Checking an element that SHOULD NOT exist (negative assertion)
# 2. The element is on a canvas/SVG (fallback can't help)
# 3. Inside a fast poll loop (fallback wastes minutes on expected misses)
acm_obj.check_element_presence(locator[::-1], timeout=3, use_fallback=False)
acm_obj.wait_until_expected_text_is_found(locator, expected_text="...", use_fallback=False)
```

The fallback costs real time and money. Only leave it enabled (`use_fallback=True`) on paths where a locator may genuinely be stale from an OCP upgrade.

---

### CLI fallback for UI failures

The `via_ui` parameter pattern is the standard mechanism for CLI fallback. The test triggers the DR action one way or the other — but **verification code is never duplicated**:

```python
@pytest.mark.parametrize(
    argnames=["primary_cluster_down", "pvc_interface", "via_ui"],
    argvalues=[
        pytest.param(True, constants.CEPHBLOCKPOOL, False,
                     marks=[acceptance, pytest.mark.polarion_id("OCS-4427")],
                     id="primary_down-rbd-cli"),
        pytest.param(True, constants.CEPHBLOCKPOOL, True,
                     marks=pytest.mark.polarion_id("OCS-4743"),
                     id="primary_down-rbd-ui"),
    ],
)
def test_failover_and_relocate(self, primary_cluster_down, pvc_interface, via_ui,
                                setup_acm_ui, dr_workload, ...):
    if via_ui:
        acm_obj = AcmAddClusters()

    # ... deploy, identify clusters, wait for sync ...

    # FAILOVER — action differs; verification is identical for both paths
    if via_ui:
        config.switch_acm_ctx()
        failover_relocate_ui(
            acm_obj,
            scheduling_interval=scheduling_interval,
            workload_to_move=f"{wl.workload_name}-1",
            policy_name=wl.dr_policy_name,
            failover_or_preferred_cluster=secondary_cluster_name,
            workload_type=wl.workload_type,
        )
    else:
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=wl.workload_namespace,
            workload_type=wl.workload_type,
            workload_placement_name=wl.appset_placement_name
            if wl.workload_type == constants.APPLICATION_SET else None,
            skip_odf_cli_validation=primary_cluster_down,
        )

    # VERIFICATION — identical for both paths, no if via_ui here
    config.switch_to_cluster_by_name(secondary_cluster_name)
    for wl in workloads:
        dr_helpers.wait_for_all_resources_creation(...)
```

> **Rule:** `if via_ui` guards the **action trigger only**. Never put `if via_ui` around resource verification, sync-time checks, or storage checks.

---

### Key UI functions in `dr_helpers_ui.py`

| Function | When to call | Notes |
|---|---|---|
| `dr_submariner_validation_from_ui(acm_obj)` | Before stopping any cluster; before UI failover | Only acts in `RDR_MODE`; no-ops in MDR. Always preceded by `config.switch_acm_ctx()`. |
| `check_cluster_status_on_acm_console(acm_obj, down_cluster_name=..., expected_text="Unknown")` | After primary nodes stopped | Pass `down_cluster_name` to check a specific cluster; omit for all-clusters ready check. |
| `failover_relocate_ui(acm_obj, ..., action=constants.ACTION_FAILOVER)` | In place of `dr_helpers.failover()` | Navigates ACM Applications page, searches workload, opens kebab, selects action, picks policy + cluster, checks `operation-readiness`, clicks Initiate. |
| `failover_relocate_ui(acm_obj, ..., action=constants.ACTION_RELOCATE)` | In place of `dr_helpers.relocate()` | Same flow; `action=constants.ACTION_RELOCATE`. |
| `verify_drpolicy_ui(acm_obj, scheduling_interval)` | Called internally by `failover_relocate_ui` | Validates DRPolicy is `Validated` on ACM topology. Do not call separately. |
| `verify_pending_cleanup_alert_firing(acm_obj, action, drpc_name)` | After discovered-apps failover (OCS ≥ 4.22) | See Addendum A. |
| `verify_pending_cleanup_alert_resolved(acm_obj, action, drpc_name)` | After discovered-apps cleanup (OCS ≥ 4.22) | See Addendum A. |

---

### Required fixture and context setup

```python
# test method signature — setup_acm_ui is always required for UI tests
def test_failover_and_relocate(
    self,
    primary_cluster_down,
    pvc_interface,
    via_ui,
    setup_acm_ui,          # ← always include, even when via_ui=False params exist
    dr_workload,
    nodes_multicluster,
    node_restart_teardown,
):
    if via_ui:
        acm_obj = AcmAddClusters()

    # ALWAYS switch to ACM context before any UI call
    if via_ui:
        config.switch_acm_ctx()
        dr_submariner_validation_from_ui(acm_obj)
```

> `setup_acm_ui` must appear in the fixture signature even for parametrized tests where some params have `via_ui=False`. The fixture is safe to declare and not use — it only initialises the browser session when the UI path actually runs.

---

### `failover_relocate_ui` internal flow (what it does)

Understanding the internal steps helps write correct assertions around it:

```
1. verify_drpolicy_ui()          → checks DRPolicy is Validated on ACM topology
2. navigate_applications_page()  → clicks ACM → Applications nav item
3. clear_filter / apply_filter   → clears existing filters, applies Subscription or AppSet filter
4. do_send_keys(search-bar)      → types workload name into search box
5. wait_for_element_to_be_clickable(kebab-action) → opens ⋮ menu (retry loop, max 10)
6. execute_script click          → clicks Failover / Relocate menu item (JS click for canvas safety)
7. do_click(policy-dropdown)     → selects DR policy (subscription only; AppSet auto-selects)
8. do_click(target-cluster)      → selects failover/preferred cluster
9. wait_until_expected_text(operation-readiness, "Ready") → asserts readiness before initiating
10. find_an_element_by_xpath(#modal-intiate-action) → checks aria-disabled
11. execute_script click Initiate → fires the DR action
```

The kebab menu click uses `driver.execute_script("arguments[0].click()")` instead of a native Selenium click — this is intentional because the ACM topology canvas can intercept pointer events. Use `click_with_script` for any canvas-overlaid element.

---

### PatternFly-specific gotchas

| Gotcha | Cause | Fix |
|---|---|---|
| `ElementClickInterceptedException` on canvas elements | ACM topology canvas intercepts pointer events | Use `acm_obj.click_with_script(locator)` instead of `do_click` |
| Kebab menu closes before item is clicked | `StaleElementReferenceException` on re-render | Use the `for _ in range(10): try/except` retry loop; already in `failover_relocate_ui` |
| `class*="pf-c-"` selectors break on OCP 4.16+ | PF4 → PF5 migration changes prefix to `pf-v5-` | Never use class-based selectors; use `data-test` or `aria-label` |
| Text matching with `text()=` fails | PatternFly wraps text in `<span>` children | Use `normalize-space()` or `contains(text(), ...)` in XPath |
| `wait_until_expected_text_is_found` fires AI fallback on expected-absent elements | `use_fallback=True` default | Pass `use_fallback=False` for negative checks |
| Version-specific nav structure (ACM 4.19 adds `all-clusters-view`) | `acm_page_nav_419` overlay | Never hardcode navigation steps — use `acm_obj.navigate_*` methods |

---

### UI test pre-submit checklist

- [ ] `setup_acm_ui` is in the method signature
- [ ] `if via_ui: acm_obj = AcmAddClusters()` is the ONLY place `AcmAddClusters` is constructed
- [ ] `config.switch_acm_ctx()` precedes every `acm_obj.*` call
- [ ] `if via_ui` wraps **only** the action trigger — never the verification steps
- [ ] All new locators are added to `views.py` (`acm_page_nav` or a versioned override) — never inline in test/helper code
- [ ] New locators use `data-test`, `aria-label`, or `By.ID` — not `pf-c-` / `pf-v5-` class selectors
- [ ] `use_fallback=False` is set on all negative element checks and fast-poll loops
- [ ] Canvas-overlaid elements use `click_with_script` not `do_click`

---

## Appendix N — DRPC Class Methods Reference

The [`DRPC`](ocs_ci/ocs/resources/drpc.py) class wraps a `DRPlacementControl` CR and is the primary object used to gate and verify DR state from test code. It is imported from `ocs_ci.ocs.resources.drpc`.

### Construction

```python
from ocs_ci.ocs.resources.drpc import DRPC

# Subscription workload
drpc_sub = DRPC(namespace=wl.workload_namespace)

# AppSet workload
drpc_appset = DRPC(
    namespace=constants.GITOPS_CLUSTER_NAMESPACE,
    resource_name=f"{wl.appset_placement_name}-drpc",
)

# Discovered-apps workload
drpc_discovered = DRPC(
    namespace=constants.DR_OPS_NAMESPACE,
    resource_name=wl.discovered_apps_placement_name,
)
```

> The `DRPC` constructor auto-calls `config.switch_acm_ctx()` to ensure the CR is read from the ACM/hub cluster context. No manual context switch needed before construction.

### Properties

| Property | Type | What it returns |
|---|---|---|
| `drpolicy` | `str` | Name of the `DRPolicy` referenced by this DRPC |
| `drpolicy_obj` | `OCS` | Full `DRPolicy` CR object (lazy-loaded from hub). Use to read `schedulingInterval`, `drClusters`, `peerClasses`. |

```python
# Read scheduling interval directly from the policy
interval_str = drpc_obj.drpolicy_obj.data["spec"]["schedulingInterval"]
# e.g. "5m" → strip suffix + convert yourself, or use dr_helpers.get_scheduling_interval()
```

### Sync-time methods

These are lower-level methods called internally by `dr_helpers.verify_last_group_sync_time` and `dr_helpers.verify_last_kubeobject_protection_time`. You typically use the `dr_helpers` wrappers — but these are useful when you need the raw timestamp without the assertion logic.

```python
# Returns the raw lastGroupSyncTime string from DRPC status, or None if absent
last_sync = drpc_obj.get_last_group_sync_time()

# Returns the raw lastKubeObjectProtectionTime string from DRPC status, or None if absent
last_kubeobj = drpc_obj.get_last_kubeobject_protection_time()
```

### Status wait methods

| Method | Signature | Timeout | When to use |
|---|---|---|---|
| `wait_for_peer_ready_status` | `(timeout=300, sleep=10)` | 300s | Wait for DRPC `peerReady` condition to be `True`. Call after a DR policy change or cluster recovery before initiating a new DR action. |
| `wait_for_clusterdataprotected_status` | `(timeout=300, sleep=10)` | 300s | Wait for DRPC `clusterDataProtected` condition to be `True`. Confirms a full data-protection cycle completed. |
| `wait_for_progression_status` | `(status, timeout=300, sleep=10, success_if_deleted=False)` | 300s (default) | Poll `DRPC.status.progression` until it matches `status`. |

#### `wait_for_progression_status` — status values and use cases

```python
from ocs_ci.ocs import constants

# After failover via hub context — wait for DRPC to report action complete
config.switch_acm_ctx()
drpc_obj.wait_for_progression_status(status=constants.STATUS_COMPLETED)

# After triggering relocate for discovered apps — wait for Relocating phase before cleanup
drpc_obj.wait_for_progression_status(status=constants.STATUS_RELOCATING, timeout=600)

# In teardown — DRPC may have been deleted; pass success_if_deleted=True to avoid
# a KeyError/NotFound exception if the CR was cleaned up before teardown ran
drpc_obj.wait_for_progression_status(
    status=constants.STATUS_COMPLETED,
    timeout=300,
    success_if_deleted=True,    # return success immediately if CR no longer exists
)
```

> `success_if_deleted=True` is the **teardown-safe** variant. Use it in `addfinalizer` blocks or `autouse` teardown fixtures to avoid spurious errors when cleanup races against test failure.

#### When to call DRPC wait methods vs dr_helpers wrappers

| Task | Use this |
|---|---|
| Verify `lastGroupSyncTime` fresh + assert age | `dr_helpers.verify_last_group_sync_time(drpc_obj, interval)` |
| Verify `lastKubeObjectProtectionTime` + assert age | `dr_helpers.verify_last_kubeobject_protection_time(drpc_obj, interval)` |
| Wait for DRPC progression = COMPLETED after hub-context action | `drpc_obj.wait_for_progression_status(constants.STATUS_COMPLETED)` |
| Confirm data protection cycle completed | `drpc_obj.wait_for_clusterdataprotected_status()` |
| Confirm DRPC peer is ready before next DR action | `drpc_obj.wait_for_peer_ready_status()` |
| Teardown: DRPC may or may not exist | `drpc_obj.wait_for_progression_status(..., success_if_deleted=True)` |

---
