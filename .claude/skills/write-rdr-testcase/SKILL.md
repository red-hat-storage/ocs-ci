---
name: write-rdr-testcase
version: 3.2.0
description: Orchestrator for writing a new RDR (Regional Disaster Recovery) test case. Runs a two-phase workflow — GATHER then WRITE — using spawn_subagent. Phase 1 (rdr-gatherer) interviews the user and produces a structured spec; for UI tests it also runs a sequential page-by-page evidence loop to extract real locators. Phase 2 (rdr-writer) writes views.py locator entries when ui_locators is present, writes and validates the complete test file.
---

# RDR Test Writer — Orchestrator

> Activate this skill, then immediately run the two-phase workflow below.
> Do NOT write any test code yourself — delegate to the two subagents.

---

## Workflow

```
User request
    │
    ▼
Phase 1: spawn_subagent("general") with rdr-gatherer skill
    │  Gathers all 10 requirements
    │  If via_ui=True: runs Phase 1b — asks user for screenshots + DOM,
    │  extracts real locators, embeds them in SPEC as ui_locators
    │  Produces SPEC block
    │
    ▼
Phase 2: spawn_subagent("general") with rdr-writer skill + SPEC
    │  If ui_locators present: writes locator entries to views.py first
    │  Writes complete test file, runs py_compile, reports done
    │
    ▼
Report file path + pre-submit checklist status to user
```

---

## Phase 1 — Run the Gatherer

Call `spawn_subagent` with `fork_context=true` (user's original request is in context) and this description:

```
Activate the skill named "rdr-gatherer" from .claude/skills/rdr-gatherer/SKILL.md, then
gather all required information to write an RDR test case. Ask all missing questions
(batch them in a single ask_followup_question call where possible). If via_ui is True or
parametrized, run Phase 1b to collect UI screenshots and DOM dumps and extract locators.
Once all 10 requirements and any UI locators are resolved, output a SPEC block in exactly
this format and nothing else:

---SPEC---
scenario: <one-line description>
workload_type: dr_workload | discovered_apps_dr_workload | cnv_dr_workload
pvc_interface: CEPHBLOCKPOOL | CEPHFILESYSTEM | both
primary_cluster_down: True | False | parametrized
via_ui: True | False | parametrized
use_statefulsets: True | False
ocs_version_gate: <"<4.XX"> | none
tier: tier1 | tier2 | tier4a | tier4b | tier4c
squad: turquoise_squad
polarion_ids: <comma-separated list or "OCS-XXXX placeholders">
file_name: test_<scenario>.py
addenda_needed: <comma-separated list of discovered-apps,cnv,node-fault,acm-ui,advanced-patterns,mixed-pvc,hub-recovery,per-cluster-loop,scale-down-fault,helper-utils or "none">
appendices_needed: <comma-separated list of dr-helpers,hcp-disconnected,drpc-class,ui-automation or "none">
ui_locators:
  page_1_<action>: {selector: '<css-or-xpath>', by: CSS_SELECTOR|ID|XPATH}
  page_2_<action>: {selector: '<css-or-xpath>', by: CSS_SELECTOR|ID|XPATH}
  (set to "none" when via_ui is False)
notes: <any extra constraints or empty>
---END SPEC---
```

The original user request is in the conversation above.
```

---

## Phase 2 — Run the Writer

Once Phase 1 returns, extract the `---SPEC---` block and call `spawn_subagent` with `fork_context=false` and this description (fill in the SPEC):

```
Activate the skill named "rdr-writer" from .claude/skills/rdr-writer/SKILL.md, then
write a complete RDR test case from the following spec:

<INSERT SPEC BLOCK HERE>

The reference files are at:
  .claude/skills/write-rdr-testcase/discovered-apps.ref.md    (Discovered Apps)
  .claude/skills/write-rdr-testcase/cnv.ref.md                (CNV)
  .claude/skills/write-rdr-testcase/node-fault.ref.md         (Node Fault)
  .claude/skills/write-rdr-testcase/acm-ui.ref.md             (ACM UI)
  .claude/skills/write-rdr-testcase/advanced-patterns.ref.md  (Advanced Patterns)
  .claude/skills/write-rdr-testcase/mixed-pvc.ref.md          (Mixed PVC)
  .claude/skills/write-rdr-testcase/hub-recovery.ref.md       (Hub Recovery)
  .claude/skills/write-rdr-testcase/per-cluster-loop.ref.md   (Per-Cluster Loop)
  .claude/skills/write-rdr-testcase/scale-down-fault.ref.md   (Scale-Down Fault)
  .claude/skills/write-rdr-testcase/helper-utils.ref.md       (Helper Utilities)
  .claude/skills/write-rdr-testcase/dr-helpers.ref.md         (dr_helpers reference)
  .claude/skills/write-rdr-testcase/hcp-disconnected.ref.md   (HCP + Disconnected)
  .claude/skills/write-rdr-testcase/drpc-class.ref.md         (DRPC class)
  .claude/skills/write-rdr-testcase/ui-automation.ref.md      (UI automation)

Read only the files listed in addenda_needed and appendices_needed.
Write the file to tests/functional/disaster-recovery/regional-dr/<file_name>.
Run python -m py_compile on it.
Return: file path, py_compile exit code, and pre-submit checklist result.
```

---

## After Phase 2 completes

Report to the user:
- The file path created
- Whether `py_compile` passed (exit 0) or any errors
- Any pre-submit checklist items that need attention
- Remind them to add Polarion IDs if placeholders were used
