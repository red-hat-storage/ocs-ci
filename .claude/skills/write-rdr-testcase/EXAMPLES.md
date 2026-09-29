# RDR Test Writer — Example Prompts

Copy any of these into the **RDR Test Writer** mode (Bob or Claude Code) to generate a complete,
validated test file. The more detail you provide up front, the fewer clarifying questions the
agent asks before writing.

---

## How to use this agent

**In Bob:** Switch to **RDR Test Writer** from the mode picker, then paste a prompt below.
**In Claude Code:** Start a new task and paste the prompt as the first message. Both environments
run the same 4-phase workflow: GATHER → PLAN → WRITE → VALIDATE.

Reply `"go"` after the agent shows its PLAN summary to trigger immediate file generation.

---

## Prompt templates

### 1 — Minimal (fastest start — agent asks all 9 questions)

```
Write an RDR test for failover after draining a node on the secondary cluster.
```

---

### 2 — Standard (most common — agent asks only for missing details)

```
Write a tier1 RDR test for appset + subscription failover/relocate.
Parametrize: RBD and CephFS, primary cluster up or down.
CLI only. OCS >= 4.18. No Polarion IDs yet.
File: test_failover_and_relocate_node_drain.py
```

---

### 3 — Detailed (fastest path — agent skips most questions, plans immediately)

```
Scenario: standard appset + subscription failover/relocate.
Workload: dr_workload(num_of_subscription=1, num_of_appset=1).
Parametrize axes:
  - primary_cluster_down: True / False
  - pvc_interface: CEPHBLOCKPOOL / CEPHFILESYSTEM
CephFS requires ReplicationDestination lifecycle checks (Step 5 of SKILL.md).
RBD requires wait_for_mirroring_status_ok.
UI/CLI: CLI only.
OCS version gate: >= 4.18 for CephFS params.
Tier: tier1. Squad: turquoise_squad.
Polarion IDs: OCS-5100 (up/rbd), OCS-5101 (down/rbd), OCS-5102 (up/cephfs), OCS-5103 (down/cephfs).
File: test_failover_and_relocate_rbd_cephfs.py
```

---

### 4 — CNV / KubeVirt VM workload

```
Scenario: CNV VM failover with MD5 data-integrity check after failover and after relocate.
Workload: cnv_dr_workload with one subscription VM.
VM type parametrized: VM_VOLUME_PVC and VM_VOLUME_DV.
Primary always up. CLI only. OCS >= 4.16. Tier1.
Polarion IDs: OCS-5501 (PVC), OCS-5502 (DV).
File: test_cnv_vm_data_integrity.py
```

---

### 5 — Discovered apps (with both sync-time gates)

```
Discovered-apps test: failover + relocate.
Both lastGroupSyncTime AND lastKubeObjectProtectionTime must be verified before failover
and before relocate (not only after).
One kubeobject workload, one recipe workload. RBD only. Primary up only.
Tier1. OCS >= 4.15. Polarion OCS-4900.
File: test_discovered_apps_sync_gate.py
```

---

### 6 — Node-fault / concurrent drain during failover

```
Scenario: drain a node on the secondary cluster concurrently with failover.
Subscription-only workload, RBD, primary always up.
Use ThreadPoolExecutor: start drain, sleep 2s, then trigger failover.
Assert drain_operation.result() after failover is verified.
Tier4b. No Polarion IDs. File: test_node_drain_during_failover.py
```

---

### 7 — Hub recovery (active hub failure)

```
Write a hub-recovery test for active hub failure with appset workloads on RBD.
Must use @dr_hub_recovery (NOT @rdr) and @pytest.mark.order("last").
Deploy workloads via passive hub using switch_ctx=get_passive_acm_index().
Tier4a. OCS >= 4.17. No Polarion IDs.
File: test_hub_failure_appset_rbd.py
```

---

### 8 — Mixed RBD + CephFS in one test

```
Scenario: deploy both RBD and CephFS workload sets in a single test and fail over both.
Use two dr_workload() calls: one for CEPHBLOCKPOOL, one for CEPHFILESYSTEM.
Per-workload dispatch: mirroring check filtered to RBD PVCs only; RepDest checks for CephFS only.
No node operations. Primary always up. Tier4c. No Polarion IDs.
File: test_mixed_pvc_interface_failover.py
```

---

### 9 — Scale-down fault injection

```
Scenario: scale ODF + submariner deployments to zero on primary, trigger failover, restart
nodes, then scale back up and relocate.
AppSet-only workload. RBD. Primary technically up but deployments down.
Use scale_deployments fixture from conftest.
Tier4b. OCS >= 4.16. No Polarion IDs.
File: test_scale_down_fault_failover.py
```

---

### 10 — UI-driven failover + CLI relocate (via_ui parametrized)

```
Standard appset + subscription failover/relocate with via_ui parametrized (True/False).
RBD only. Primary up only.
UI path: dr_submariner_validation_from_ui before failover, failover_relocate_ui for action,
check_cluster_status_on_acm_console after. CLI path: standard dr_helpers.failover.
Verification steps are identical for both paths.
Tier1. OCS >= 4.14. Polarion OCS-4200 (cli), OCS-4201 (ui).
File: test_failover_relocate_ui_cli.py
```

---

## Tips for fastest results

| Tip | Effect |
|---|---|
| Include the file name | Skips 1 question |
| Include Polarion IDs (or `OCS-XXXX`) | Skips 1 question |
| Specify tier + OCS version | Skips 2 questions |
| Name the workload type explicitly | Skips 1 question and removes all workload-type disambiguation |
| After the PLAN, reply `go` | Agent writes the file immediately — no second confirmation |

---

## What the agent will NOT do

- Add imports that are not used in the generated test
- Define fixtures in the test file (all fixtures live in `conftest.py`)
- Skip `verify_last_group_sync_time` before failover or before relocate
- Omit `polarion_id` from any `pytest.param`
- Use `@rdr` on a hub-recovery class (those use `@dr_hub_recovery` instead)
- Report done before `python -m py_compile` exits 0
