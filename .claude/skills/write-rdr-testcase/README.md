# RDR Test Writer — User Guide

This skill system lets you write a complete, validated RDR test case by describing what you
want in plain English. You don't need to know the fixture names, import paths, locator
conventions, or the 17-step failover/relocate skeleton — the agents handle all of that.

---

## How to start

In any Bob session, type:

```
use skill write-rdr-testcase
```

Then describe the test you want. That's it. The agents take over from there.

---

## What happens after you activate the skill

```
Your description
      │
      ▼
Phase 1 — Gatherer
  Asks only the questions your prompt didn't answer (batched in one call).
  For UI tests: walks you through each ACM page one at a time to capture
  real locators from your live cluster before writing any code.
  Outputs a structured SPEC block for you to review.
      │
      ▼
      (Reply "go" to write immediately, or review the SPEC first)
      │
      ▼
Phase 2 — Writer
  Reads only the reference files relevant to your scenario.
  For UI tests: adds new locator entries to views.py before writing the test.
  Writes the complete test file.
  Runs py_compile to verify syntax.
  Reports: file path, checklist status, any TODOs.
```

---

## The 3 workload types

### 1. Standard workload (`dr_workload`)

AppSet or Subscription based. The most common type.

```
Write a tier1 RDR test for appset failover/relocate.
Parametrize: RBD and CephFS, primary cluster up or down.
CLI only. OCS >= 4.18. No Polarion IDs yet.
File: test_failover_and_relocate.py
```

The gatherer will ask about anything not specified (tier, file name, etc.).

---

### 2. Discovered Apps (`discovered_apps_dr_workload`)

Uses the discovered-apps protection path. Both sync-time gates are mandatory.

```
Write a tier1 RDR discovered-apps test: failover + relocate.
One kubeobject workload, one recipe workload.
Parametrize: RBD and CephFS. Primary up only.
OCS >= 4.15. No Polarion IDs.
File: test_discovered_apps_failover_relocate.py
```

The writer automatically applies the discovered-apps differences:
- DRPC uses `DR_OPS_NAMESPACE` (not workload namespace)
- `verify_last_kubeobject_protection_time()` gating before failover and relocate
- `do_discovered_apps_cleanup()` after failover
- `wait_for_all_resources_creation()` with `discovered_apps=True`

---

### 3. ACM UI test (`via_ui`)

Triggers a live-cluster evidence loop before any code is written. The gatherer asks you
to capture a screenshot and DOM dump from each ACM page — one page at a time.

```
Write a UI-only RDR failover+relocate test.
AppSet workload, RBD. Primary up.
All actions via ACM UI: submariner validation, failover, relocate.
I have a live cluster — I can provide screenshots and DOM dumps.
Tier1. OCS >= 4.14. No Polarion IDs.
File: test_failover_relocate_acm_ui_only.py
```

**What the UI evidence loop looks like:**

The gatherer asks for one page at a time (up to 7 rounds):

```
Page 1: ACM Applications list
Navigate to: ACM > Applications
Cluster state: primary cluster running normally

Please capture and attach:
  • screenshot: self.take_screenshot_for_llm(name_suffix='page_1_apps_list')
  • DOM:        self.copy_dom(name_suffix='page_1_apps_list')

Actions I need locators for:
  • Application row identifier
  • Kebab (⋮) menu trigger

Or reply "skip page_1" to use placeholders.
```

**How to capture for each page:**

1. Find an existing test that navigates to that page (or write a 5-line stub).
2. Insert these lines right before the action you want to capture:
   ```python
   self.take_screenshot_for_llm(name_suffix='page_1_apps_list')
   self.copy_dom(name_suffix='page_1_apps_list')
   import time; time.sleep(9999)   # pause here — Ctrl+C after files are written
   ```
3. Run the test on your live cluster. It pauses after writing the files.
4. Retrieve `.png` from `UI_SELENIUM.screenshots_folder` and `.html` from `UI_SELENIUM.dom_folder`.
5. Attach both files to the chat, then Ctrl+C the paused test.
6. The gatherer extracts stable locators (`data-test`, `aria-label`) from the DOM,
   confirms them with you, then moves to the next page.

Each round takes ~2 minutes. All 7 pages takes ~15 minutes total.

**Can't access a live cluster?** Reply `"skip page_N"` for any page. The writer will still
produce the complete test with `# TODO: fill from DOM inspection` placeholders in `views.py`.

---

## What gets written

| Output | Location |
|---|---|
| Test file | `tests/functional/disaster-recovery/regional-dr/<file_name>.py` |
| New UI locators (UI tests only) | `ocs_ci/ocs/ui/views.py` — `acm_page` dict |

---

## All supported scenario types

| Scenario | Prompt hint |
|---|---|
| Standard failover/relocate | "appset", "subscription", "dr_workload" |
| Discovered apps | "discovered", "discovered apps" |
| CNV / KubeVirt VMs | "CNV", "VM", "cnv_dr_workload" |
| Node drain/fault during failover | "drain a node", "node failure" |
| Hub recovery | "hub failure", "hub recovery" |
| Mixed RBD + CephFS in one test | "both RBD and CephFS", "mixed PVC" |
| Scale-down fault injection | "scale deployments to zero", "ODF pods down" |
| ACM UI actions | "via UI", "ACM UI", "failover_relocate_ui" |
| StatefulSet workload variant | "statefulset", "stateful", "sts" |
| HCP / disconnected | "hosted control plane", "disconnected" |

---

## Tips for fastest results

| Tip | Effect |
|---|---|
| Include the file name | Gatherer skips 1 question |
| Include Polarion IDs (or `OCS-XXXX`) | Gatherer skips 1 question |
| Specify tier + OCS version | Gatherer skips 2 questions |
| Name the workload type explicitly | No workload-type disambiguation question |
| Say "I have a live cluster" (UI tests) | Gatherer proceeds with evidence loop immediately |
| Reply `go` after seeing the SPEC | Writer starts without a second confirmation round |
| Give a fully detailed prompt | Gatherer may skip all questions entirely |

---

## If you change code that affects these skills

The agents learn from the `.ref.md` files in this directory. If you add a function,
change a fixture parameter, or add a new ACM UI page, the skills go stale — the next
test the agent writes will use the old knowledge.

**Before merging a PR that touches any of these files, check `MAINTENANCE.md`:**

| Code file changed | Skill file(s) to update |
|---|---|
| `ocs_ci/helpers/dr_helpers.py` | `dr-helpers.ref.md` |
| `ocs_ci/helpers/dr_helpers_ui.py` | `ui-automation.ref.md` |
| `ocs_ci/ocs/ui/views.py` | `ui-automation.ref.md` |
| `tests/conftest.py` (dr_workload fixtures) | `rdr-writer/SKILL.md` Step 5 + relevant `.ref.md` |
| `conf/ocsci/dr_workload.yaml` | `rdr-gatherer/SKILL.md` Requirement 6 note |
| `ocs_ci/framework/pytest_customization/marks.py` | `rdr-gatherer/SKILL.md` + `rdr-writer/SKILL.md` |

Full dependency map, how-to steps, and the new-file checklist: → **[`MAINTENANCE.md`](MAINTENANCE.md)**

---

## Skill files in this directory

You don't need to open any of these — the agents load them automatically.
They are listed here so you know what exists if you want to understand or extend the system.

| File | Purpose |
|---|---|
| `SKILL.md` | Orchestrator — drives the two-phase workflow |
| `EXAMPLES.md` | Full prompt templates for all 11 scenario types |
| `README.md` | This file — user guide and entry point |
| `MAINTENANCE.md` | How to keep skills in sync when code changes |
| `discovered-apps.ref.md` | Discovered apps fixture patterns + StatefulSet variant |
| `cnv.ref.md` | CNV / KubeVirt VM workload patterns |
| `node-fault.ref.md` | Node drain / concurrent fault patterns |
| `acm-ui.ref.md` | ACM UI action patterns + locator wiring from SPEC |
| `advanced-patterns.ref.md` | Step logging, concurrent ops, complex phase structure |
| `mixed-pvc.ref.md` | Mixed RBD + CephFS in one test |
| `hub-recovery.ref.md` | Hub recovery marks, fixtures, and flow |
| `per-cluster-loop.ref.md` | Per-cluster loop and custom teardown patterns |
| `scale-down-fault.ref.md` | Scale-down fault injection pattern |
| `helper-utils.ref.md` | Retry decorator, disable DR, skip health check |
| `dr-helpers.ref.md` | Complete `dr_helpers.py` function reference |
| `hcp-disconnected.ref.md` | HCP (Hosted Control Plane) + Disconnected mode |
| `drpc-class.ref.md` | DRPC class construction and method reference |
| `ui-automation.ref.md` | Locator system, PatternFly gotchas, AI fallback rules |

---

## Two companion agent skills

The orchestrator spawns these automatically — you never activate them directly.

| Skill | Location | Role |
|---|---|---|
| `rdr-gatherer` | `.claude/skills/rdr-gatherer/SKILL.md` | Phase 1: gathers requirements + UI evidence loop |
| `rdr-writer` | `.claude/skills/rdr-writer/SKILL.md` | Phase 2: writes `views.py` + test file + validates |
