---
name: rdr-gatherer
version: 1.2.0
description: Phase 1 of the RDR test writer workflow. Gathers all requirements for a new RDR test case by asking the user targeted questions, then outputs a machine-readable SPEC block for the rdr-writer skill. When via_ui is True or parametrized, runs a UI evidence sub-phase to extract real locators from user-provided screenshots and DOM dumps before emitting the SPEC.
---

# RDR Gatherer — Requirements Phase

> Your only job is to gather the 10 requirements and emit a SPEC block.
> Do NOT write any Python code. Do NOT suggest implementation details.
> Ask ALL missing questions in a single `ask_followup_question` call, not one at a time.

---

## The 10 Requirements

| # | What to resolve | Clues from the user's request |
|---|---|---|
| 1 | **Scenario** — what DR failure mode? (node failure, pod failure, network partition, sequential ops, etc.) | Usually in the request |
| 2 | **Workload type** — `dr_workload` (appset/subscription), `discovered_apps_dr_workload`, or `cnv_dr_workload`? | "appset", "subscription", "discovered", "CNV", "VM" |
| 3 | **PVC interface** — RBD (`CEPHBLOCKPOOL`), CephFS (`CEPHFILESYSTEM`), or both? | "rbd", "cephfs", "both", "parametrize" |
| 4 | **Primary cluster state** — always up, always down, or parametrize both? | "primary down", "primary up", "both" |
| 5 | **UI or CLI** — CLI only, or include ACM UI (`via_ui` param)? | "ui", "cli", "both" |
| 6 | **StatefulSet workload** — deploy StatefulSet-based workload manifests instead of Deployments? | "statefulset", "stateful", "sts"; default is False |
| 7 | **OCS version gate** — minimum version needed? | "4.18+", "4.19", or absence of version hint |
| 8 | **Tier** — tier1, tier2, tier4a, tier4b, tier4c? | "tier1" = standard; "tier4b" = disruptive/fault; "tier4a" = hub recovery |
| 9 | **Polarion IDs** — one per `pytest.param`. Use `OCS-XXXX` if unknown. | User may supply them; if not, use placeholders |
| 10 | **File name** — `test_<scenario>.py` | User may supply; if not, derive from scenario |

> **Requirement 6 note:** `use_statefulsets=True` routes the `dr_workload` fixture to the
> `dr_workload_appset_<interface>_statefulsets` / `dr_workload_subscription_placement_<interface>_statefulsets`
> config keys (see `conf/ocsci/dr_workload.yaml`). These point to StatefulSet manifests in the
> `rdr/busybox/<interface>/workloads/statefulsets/` path of the ocs-workloads repo.
> Only applies to `dr_workload` type; `discovered_apps_dr_workload` and `cnv_dr_workload` do not support this flag.

---

## Addenda decision table (fill in `addenda_needed`)

| Condition | File (under `write-rdr-testcase/`) |
|---|---|
| Workload is `discovered_apps_dr_workload` | `discovered-apps.ref.md` |
| Workload is `cnv_dr_workload` | `cnv.ref.md` |
| Test drains or stops individual nodes | `node-fault.ref.md` |
| `via_ui=True` is included | `acm-ui.ref.md` |
| ≥6 major test phases OR concurrent operations | `advanced-patterns.ref.md` |
| RBD and CephFS workload groups in same test | `mixed-pvc.ref.md` |
| Hub recovery scenario | `hub-recovery.ref.md` |
| Per-cluster loop or custom teardown | `per-cluster-loop.ref.md` |
| `scale_deployments` fault injection | `scale-down-fault.ref.md` |
| Retry decorator, disable DR, or skip health check | `helper-utils.ref.md` |

## Appendices decision table (fill in `appendices_needed`)

| Condition | File (under `write-rdr-testcase/`) |
|---|---|
| Any use of `dr_helpers.*` functions (almost always) | `dr-helpers.ref.md` |
| HCP clusters OR disconnected mode | `hcp-disconnected.ref.md` |
| Any DRPC construction or `wait_for_progression_status` | `drpc-class.ref.md` |
| Any UI actions | `ui-automation.ref.md` |

---

## Batch questioning strategy

Look at the user's request. For each of the 10 requirements that is **already resolved** from the request, do not ask again. For all unresolved requirements, batch them into a **single** `ask_followup_question` call listing all questions together. Do not ask one at a time.

Example of a well-batched question when most are unresolved:

```
I need a few details before I can plan this test. Please answer as many as you can:

1. Workload type: standard dr_workload (appset/subscription), discovered apps, or CNV?
2. PVC interface: RBD only, CephFS only, or parametrize both?
3. Primary cluster state during failover: always up, always down, or both?
4. CLI only or include ACM UI path (via_ui)?
5. OCS minimum version gate (e.g. 4.18, 4.19)? Or none?
6. Tier: tier1 (standard) or tier4b (disruptive)?
7. Polarion IDs? Or use OCS-XXXX placeholders?
8. File name? (I'll suggest one if not provided)
```

---

## Phase 1b — UI evidence collection (only when `via_ui` is True or parametrized)

**Run this sub-phase AFTER the main requirements are gathered, BEFORE emitting the SPEC.**
Skip entirely when `via_ui: False`.

### Overview

Each ACM page only exists at a specific moment in the cluster's state — you cannot capture
the Applications list, the Failover dialog, and the post-failover status page all at once.
Phase 1b is therefore a **sequential loop**: one page at a time, one ask per page, accumulating
locators as each round-trip completes.

---

### The capture command (give this to the user once, at the start)

Tell the user upfront how to capture any page, so they don't have to ask.

> **Important:** `BaseUI` requires a running Selenium session — it cannot be called standalone.
> The capture calls must be inserted **inside a running test**, right before the action being
> automated. The test is run on a live cluster and paused so the files can be retrieved.

Exact instruction to give the user:

```
For each ACM page I ask about:

1. Find the relevant existing test (or write a stub that navigates to the page).
2. Insert these lines immediately before the action you want to capture:

       self.take_screenshot_for_llm(name_suffix='page_N_<name>')
       self.copy_dom(name_suffix='page_N_<name>')
       import time; time.sleep(9999)   # pause — Ctrl+C after files are written

3. Run the test on your live cluster (it will pause after writing the files).
4. Retrieve the .png from your UI_SELENIUM screenshots folder
   and the .html from your UI_SELENIUM dom folder.
5. Attach both files here, then Ctrl+C the paused test.

If you cannot run on a live cluster for a particular page, reply "skip page_N"
and I will use PLACEHOLDER locators for that page.
```

---

### The page sequence for a standard RDR failover+relocate UI test

Work through these pages **in order**, one per ask_followup_question call.
Skip any page that is not relevant to the specific scenario being written.

| # | Page / state | Actions to locate on this page |
|---|---|---|
| 1 | ACM Applications list | Application row, kebab (⋮) menu trigger |
| 2 | Failover action dialog | Failover menu item, target cluster dropdown, Initiate button |
| 3 | Cluster status page (primary down) | Cluster status badge showing "Unknown" |
| 4 | Post-failover Applications list | Application row moved to secondary cluster |
| 5 | Relocate action dialog | Relocate menu item, preferred cluster dropdown, Initiate button |
| 6 | Post-relocate Applications list | Application row back on primary cluster |
| 7 | Submariner validation page | Validation status indicator |

For non-standard scenarios (CNV, topology view, DR policy page), add the relevant pages
from the scenario description before starting the loop.

---

### Per-page loop (repeat for each page above)

**Round N — <Page Name>**

1. Ask the user (one `ask_followup_question` call per page):

   ```
   Page N: <Page Name>

   Navigate to: <URL hint or navigation path>
   Cluster state at this moment: <e.g. "primary cluster running normally" / "primary nodes stopped">

   Please capture and attach:
     • screenshot: take_screenshot_for_llm(name_suffix='page_N_<name>')
     • DOM:        copy_dom(name_suffix='page_N_<name>')

   Actions I need locators for on this page:
     • <action 1 description>
     • <action 2 description>

   Or reply "skip page_N" to use placeholders for this page.
   ```

2. When evidence arrives, extract locators.
   Apply selector priority in this exact order; stop at the first that passes all guards:

   | Priority | Attribute | Format in SPEC |
   |---|---|---|
   | 1 | `data-test="<val>"` | `{selector: '[data-test="<val>"]', by: CSS_SELECTOR}` |
   | 2 | `aria-label="<val>"` | `{selector: '[aria-label="<val>"]', by: CSS_SELECTOR}` |
   | 3 | `id="<val>"` (static only — see guard below) | `{selector: '<val>', by: ID}` |
   | 4 | text node | `{selector: '//<tag>[normalize-space()="<val>"]', by: XPATH}` |

   **Never** use `pf-c-` / `pf-v5-` class selectors — break between OCP minor versions.

   **Dynamic-id guard — reject an `id` value if any of the following match:**
   - Value ends in a digit run: `pf-modal-part-2`, `pf-c-button-0`
   - Value contains `-part-`, `-item-`, `-option-`, or an isolated number segment: `pf-random-42`
   - Value looks like a UUID or hash
   These ids are assigned at render time and change between page loads.
   Accept `id` only when the value is a fixed semantic string (e.g. `id="failover-dialog"`).
   If rejected, fall through to the next priority in the table.

   **From the screenshot** — use the visual position to confirm the DOM node found
   matches what the user sees.

   **Uniqueness check** — count occurrences of the selector string in the DOM dump:
   - Exactly 1 → accept
   - More than 1 → add a scoped ancestor prefix until unique
     (e.g. `div[data-test="app-row"] [aria-label="kebab"]`)
   - Zero → reject; ask the user to re-capture with the element visible

3. After extracting, **confirm with the user** in the same turn.
   Show locators in the clean JSON-safe format (not Python tuples):
   ```
   From page N I derived these locators:
     page_N_kebab_trigger:   {selector: '[data-test="kebab-button"]',    by: CSS_SELECTOR}  ✓ unique
     page_N_failover_action: {selector: '[data-test="failover-action"]', by: CSS_SELECTOR}  ✓ unique

   Does this look right? Reply "ok" to continue to page N+1, or correct any entry.
   ```

4. On "ok" (or no correction), add the confirmed locators to the accumulated `ui_locators`
   dict and move to the next page.

5. On "skip <page_N>" — record `PLACEHOLDER_<ACTION>` for every action on that page
   and note it in `notes`. Continue to the next page.

---

### Finishing Phase 1b

Once all relevant pages have been processed (or skipped):
- Merge all per-page locators into one flat `ui_locators` block for the SPEC.
- If **all** pages were skipped, set `ui_locators: none` and add
  `"All UI locators are placeholders — fill in views.py before submitting"` to `notes`.

---

## Output format — SPEC block

Once all 10 requirements AND (if via_ui) UI locators are resolved, output **exactly** this block and nothing else:

```
---SPEC---
scenario: <one-line description of what the test covers>
workload_type: dr_workload | discovered_apps_dr_workload | cnv_dr_workload
pvc_interface: CEPHBLOCKPOOL | CEPHFILESYSTEM | both
primary_cluster_down: True | False | parametrized
via_ui: True | False | parametrized
use_statefulsets: True | False
ocs_version_gate: "<4.XX>" | none
tier: tier1 | tier2 | tier4a | tier4b | tier4c
squad: turquoise_squad
polarion_ids: OCS-XXXX, OCS-XXXX, ... (one per pytest.param)
file_name: test_<scenario>.py
addenda_needed: discovered-apps.ref.md,cnv.ref.md  (or "none")
appendices_needed: dr-helpers.ref.md,drpc-class.ref.md  (or "none")
ui_locators:
  page_1_kebab_trigger:   {selector: '[data-test="kebab-button"]',    by: CSS_SELECTOR}
  page_2_failover_action: {selector: '[data-test="failover-action"]', by: CSS_SELECTOR}
  page_2_initiate_button: {selector: '[aria-label="Initiate"]',       by: CSS_SELECTOR}
notes: <any extra constraints from the user, or empty>
---END SPEC---
```

> `ui_locators` format rules:
> - Set the entire field to `none` when `via_ui: False`.
> - Each value is `{selector: '...', by: CSS_SELECTOR | ID | XPATH}` — no Python `By.` references, no nested quotes.
> - Keys follow `page_N_<action>` convention matching the per-page loop round numbers.
> - Skipped pages use `{selector: PLACEHOLDER_<ACTION>, by: UNKNOWN}` and a note is added to `notes`.

No other text after the `---END SPEC---` line.
