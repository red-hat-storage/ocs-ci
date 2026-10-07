# RDR Test Writer — Example Prompts

Copy any of these into a Bob session with the **write-rdr-testcase** skill active.
The workflow runs two phases automatically: GATHER → WRITE.

---

## How the two-phase workflow runs

1. **GATHER phase** (`rdr-gatherer` skill) — asks only the questions not answered by your prompt, batches them in one call. Outputs a structured SPEC.
2. **WRITE phase** (`rdr-writer` skill) — reads only the addendum files needed, writes the complete file, runs `py_compile`, reports the checklist.

Reply `"go"` after seeing the SPEC if you want to skip review and write immediately.

---

## Prompt templates

### 1 — Minimal (gatherer asks all questions)

```
Write an RDR test for failover after draining a node on the secondary cluster.
```

---

### 2 — Standard (gatherer asks only for missing details)

```
Write a tier1 RDR test for appset + subscription failover/relocate.
Parametrize: RBD and CephFS, primary cluster up or down.
CLI only. OCS >= 4.18. No Polarion IDs yet.
File: test_failover_and_relocate_node_drain.py
```

---

### 3 — Detailed (gatherer skips most questions, writes immediately)

```
Scenario: standard appset + subscription failover/relocate.
Workload: dr_workload(num_of_subscription=1, num_of_appset=1).
Parametrize axes:
  - primary_cluster_down: True / False
  - pvc_interface: CEPHBLOCKPOOL / CEPHFILESYSTEM
CephFS requires ReplicationDestination lifecycle checks.
RBD requires wait_for_mirroring_status_ok.
CLI only. OCS >= 4.18. Tier1. Squad: turquoise_squad.
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
and before relocate.
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
UI path: dr_submariner_validation_from_ui before failover, failover_relocate_ui for action.
CLI path: standard dr_helpers.failover. Verification steps are identical for both paths.
Tier1. OCS >= 4.14. Polarion OCS-4200 (cli), OCS-4201 (ui).
File: test_failover_relocate_ui_cli.py
```

> **What to expect for UI tests (Phase 1b):**
> Because this prompt includes `via_ui`, the gatherer will run a sequential evidence loop
> before emitting the SPEC. It will ask you to capture a screenshot + DOM dump from each
> ACM page (Applications list, Failover dialog, etc.) one at a time — you need a live cluster
> with a running test to do this. For each page it gives you the exact lines to insert into
> an existing test, then asks you to attach the `.png` and `.html` output files.
> Reply `"skip page_N"` for any page you cannot capture — placeholder locators will be used.

---

### 11 — UI-only failover (full Phase 1b walkthrough, no CLI path)

```
Write a UI-only RDR failover+relocate test. No CLI path parametrize.
AppSet workload, RBD. Primary up.
All actions via ACM UI: submariner validation, failover, relocate.
I have a live cluster — I can provide screenshots and DOM dumps for each page.
Tier1. OCS >= 4.14. No Polarion IDs.
File: test_failover_relocate_acm_ui_only.py
```

> **This prompt explicitly signals you have a live cluster.** The gatherer will walk you through
> pages 1→7 in sequence — one ask per page, confirm locators before moving on.
> Each round takes ~2 minutes (insert debug lines, run test stub, attach files, Ctrl+C).
> Total: ~15 minutes for all 7 pages. The writer then produces a test with real, verified
> locators in `views.py` — no guessed selectors.

---

## Tips for fastest results

| Tip | Effect |
|---|---|
| Include the file name | Gatherer skips 1 question |
| Include Polarion IDs (or `OCS-XXXX`) | Gatherer skips 1 question |
| Specify tier + OCS version | Gatherer skips 2 questions |
| Name the workload type explicitly | Gatherer skips 1 question and removes all workload-type disambiguation |
| After seeing the SPEC, reply `go` | Writer starts immediately — no second confirmation |
| Give a detailed prompt (template 3+) | Gatherer may skip all questions and go straight to SPEC |
