---
name: rdr-writer
version: 1.2.0
description: Phase 2 of the RDR test writer workflow. Receives a SPEC block from rdr-gatherer, reads only the relevant addendum/appendix files, writes views.py locator entries when ui_locators is present, writes a complete RDR test case, runs py_compile, and reports checklist status.
---

# RDR Writer — Code Generation Phase

> You receive a SPEC block. Your job: read only what you need, write the complete file, validate it.
> Do NOT ask the user any questions. Resolve everything from the SPEC.

---

## Step 0 — Handle `ui_locators` from the SPEC (only when present and not "none")

Before reading any reference file or writing any test code, process the `ui_locators` field:

### 0a — Locate `acm_page` in `views.py` efficiently

Do NOT read the whole file. Use `grep` to find the line number first:
```bash
grep -n '"acm_page"' ocs_ci/ocs/ui/views.py
```
Then use `read_file` with a targeted range (±50 lines around the match) to see just that dict.

### 0b — Convert each SPEC entry to a `views.py` tuple

Each SPEC `ui_locators` entry is in JSON-safe format:
```
page_1_kebab_trigger: {selector: '[data-test="kebab-button"]', by: CSS_SELECTOR}
```

Convert to the `views.py` tuple format (note: **reversed** from Selenium — selector first, `By.TYPE` second):
```python
"page_1_kebab_trigger": ('[data-test="kebab-button"]', By.CSS_SELECTOR),
```

`by` field mapping:
| SPEC `by` value | `views.py` value |
|---|---|
| `CSS_SELECTOR` | `By.CSS_SELECTOR` |
| `ID` | `By.ID` |
| `XPATH` | `By.XPATH` |
| `UNKNOWN` | `By.CSS_SELECTOR`  # placeholder — add `# TODO` comment |

### 0c — Write to `views.py`

For each converted entry:
- If the key does **not** already exist in `acm_page` → add it with `apply_diff`.
- If it already exists with the **same** selector → skip (already correct).
- If it already exists with a **different** selector → keep existing; add
  `# NOTE: SPEC suggested <new_selector>` comment — never silently overwrite.
- If the SPEC value was `PLACEHOLDER_<ACTION>` → add with `# TODO: fill from DOM inspection` comment
  and record in the pre-submit checklist (Step 8).

### 0d — Rule

**Never inline a selector string in the test file or helper.** All selectors go in `views.py`.

---

## Step 1 — Read required reference files

From the SPEC's `addenda_needed` and `appendices_needed` fields, read ONLY the relevant files.
Do not read files not listed in the SPEC.

File paths:
- `.claude/skills/write-rdr-testcase/discovered-apps.ref.md` — Discovered Apps
- `.claude/skills/write-rdr-testcase/cnv.ref.md` — CNV
- `.claude/skills/write-rdr-testcase/node-fault.ref.md` — Node Fault
- `.claude/skills/write-rdr-testcase/acm-ui.ref.md` — ACM UI
- `.claude/skills/write-rdr-testcase/advanced-patterns.ref.md` — Advanced Patterns
- `.claude/skills/write-rdr-testcase/mixed-pvc.ref.md` — Mixed PVC
- `.claude/skills/write-rdr-testcase/hub-recovery.ref.md` — Hub Recovery
- `.claude/skills/write-rdr-testcase/per-cluster-loop.ref.md` — Per-Cluster Loop
- `.claude/skills/write-rdr-testcase/scale-down-fault.ref.md` — Scale-Down Fault
- `.claude/skills/write-rdr-testcase/helper-utils.ref.md` — Helper Utilities
- `.claude/skills/write-rdr-testcase/dr-helpers.ref.md` — dr_helpers function reference
- `.claude/skills/write-rdr-testcase/hcp-disconnected.ref.md` — HCP + Disconnected
- `.claude/skills/write-rdr-testcase/drpc-class.ref.md` — DRPC class methods
- `.claude/skills/write-rdr-testcase/ui-automation.ref.md` — UI test automation

---

## Step 2 — Build the import block

Start with the base imports and add only what the test will use:

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

Add UI imports only when `via_ui` is in the SPEC:
```python
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
)
from ocs_ci.ocs.acm.acm import AcmAddClusters
```

**Rule:** Every imported symbol must be used in the generated code. Remove unused imports before writing.

---

## Step 3 — Class marks (from SPEC tier + scenario type)

```python
@rdr              # ALWAYS — except hub-recovery tests which use @dr_hub_recovery instead
@tier1            # from SPEC tier field
@turquoise_squad  # ALWAYS
class TestMyScenario:
```

Optional marks — add above `@rdr` when applicable:
- `@skipif_ocs_version("<4.XX")` — when `ocs_version_gate` is set in SPEC
- `@dr_hub_recovery` — for hub-recovery tests (replaces `@rdr`)
- `@pytest.mark.order("last")` — required for hub-recovery tests

---

## Step 4 — Parametrize block (from SPEC axes)

Build `pytest.param` entries from the SPEC's `pvc_interface`, `primary_cluster_down`, `via_ui`, and `polarion_ids` fields.

Standard 4-param pattern:
```python
@pytest.mark.parametrize(
    argnames=["primary_cluster_down", "pvc_interface"],
    argvalues=[
        pytest.param(False, constants.CEPHBLOCKPOOL,
                     marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
                     id="primary_up-rbd"),
        pytest.param(True, constants.CEPHBLOCKPOOL,
                     marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
                     id="primary_down-rbd"),
        pytest.param(False, constants.CEPHFILESYSTEM,
                     marks=[skipif_ocs_version("<4.19"), acceptance, pytest.mark.polarion_id("OCS-XXXX")],
                     id="primary_up-cephfs"),
        pytest.param(True, constants.CEPHFILESYSTEM,
                     marks=[skipif_ocs_version("<4.19"), pytest.mark.polarion_id("OCS-XXXX")],
                     id="primary_down-cephfs"),
    ],
)
```

Rules:
- `marks=acceptance` (bare, no list) when it is the only mark
- `marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")]` when multiple
- `id` must be unique and descriptive

---

## Step 5 — Core skeleton

Use the standard failover-then-relocate skeleton as the base for ALL workload types.
Apply modifications from the relevant ADDENDUM files over this base.

### `dr_workload` call — standard vs StatefulSet

When `use_statefulsets: False` in the SPEC (the default):
```python
workloads = dr_workload(
    num_of_subscription=0, num_of_appset=1, pvc_interface=pvc_interface
)
```

When `use_statefulsets: True` in the SPEC, add `use_statefulsets=True`:
```python
workloads = dr_workload(
    num_of_subscription=0,
    num_of_appset=1,
    pvc_interface=pvc_interface,
    use_statefulsets=True,
)
```

`use_statefulsets=True` routes the fixture to the `dr_workload_appset_<interface>_statefulsets`
config key (defined in `conf/ocsci/dr_workload.yaml`), which points to StatefulSet manifests
in the ocs-workloads repo under `rdr/busybox/<interface>/workloads/statefulsets/`.
The class docstring **must** state this and reference the config key so it is clear what is deployed.

Only applies to `dr_workload` (appset/subscription). For `discovered_apps_dr_workload`, see
`discovered-apps.ref.md` § StatefulSet variant. `cnv_dr_workload` has no StatefulSet variant.

### The 17-step flow:
1. Deploy workloads
2. (CephFS) verify ReplicationDestination on secondary
3. Wait 2× scheduling interval; record sync time BEFORE failover
4. Scenario-specific fault injection
5. Stop primary nodes if `primary_cluster_down=True`
6. Trigger failover for all workloads
7. Verify resources created on secondary
8. Restore primary nodes if they were stopped
9. Verify resources deleted from primary
10. Post-failover storage checks (CephFS: RepDest lifecycle; RBD: mirroring status)
11. Wait 2× scheduling interval; verify sync time AFTER failover
12. Record sync time BEFORE relocate (gate check — do NOT skip)
13. Trigger relocate for all workloads
14. Verify resources deleted from secondary
15. Verify resources created on primary
16. Post-relocate storage checks (same as step 10)
17. Verify final sync time AFTER relocate

DRPC construction:
- Subscription: `DRPC(namespace=wl.workload_namespace)`
- AppSet: `DRPC(namespace=constants.GITOPS_CLUSTER_NAMESPACE, resource_name=f"{wl.appset_placement_name}-drpc")`
- Discovered apps: `DRPC(namespace=constants.DR_OPS_NAMESPACE, resource_name=wl.discovered_apps_placement_name)`

---

## Step 6 — Naming conventions

| Item | Convention | Example |
|---|---|---|
| File | `test_<scenario_snake_case>.py` | `test_failover_after_node_drain.py` |
| Directory | Always `tests/functional/disaster-recovery/regional-dr/` | — |
| Class | `Test<ScenarioPascalCase>` | `TestFailoverAfterNodeDrain` |
| Method | `test_<scenario_snake_case>` | `test_failover_after_node_drain` |
| Param ID | `<primary_state>-<pvc_type>[-<variant>]` | `primary_down-rbd`, `primary_up-cephfs-ui` |

---

## Step 7 — Write the file

Use `write_file` to create the complete file at `tests/functional/disaster-recovery/regional-dr/<file_name>`.

The file must be complete — no `...`, no placeholders in the code body (Polarion `OCS-XXXX` placeholders in marks are acceptable).

---

## Step 8 — Pre-submit checklist (verify ALL before reporting done)

Go through each item. If any fails, fix the file and re-check before proceeding to Step 9.

- [ ] `@rdr` (or `@dr_hub_recovery` for hub-recovery), `@turquoise_squad`, and tier mark on class
- [ ] Every `pytest.param` has a `polarion_id` mark
- [ ] Every imported symbol is actually used in the generated code
- [ ] DRPC constructed correctly per workload type (see Step 5)
- [ ] `primary_cluster_down=True` paths have `skip_odf_cli_validation=True` in `failover()`
- [ ] `node_restart_teardown` fixture present whenever nodes are stopped
- [ ] CephFS and RBD storage checks present and conditional on `pvc_interface`
- [ ] `verify_last_group_sync_time` called BEFORE failover AND BEFORE relocate
- [ ] Discovered-apps: `verify_last_kubeobject_protection_time` before failover AND before relocate
- [ ] When `use_statefulsets=True`: `use_statefulsets=True` is passed to `dr_workload(...)` and class docstring references the `_statefulsets` config key and StatefulSet manifest path
- [ ] Docstring lists numbered steps matching the actual code
- [ ] When `ui_locators` was in the SPEC: all locators are in `views.py` (not inlined); any `PLACEHOLDER_*` entries have `# TODO` comments and are listed in the return message

---

## Step 9 — Syntax validation

Run:
```bash
python -m py_compile tests/functional/disaster-recovery/regional-dr/<file_name>
```

Use `execute_command`. Exit 0 = pass. Any output = syntax error — fix it and re-run.
Do NOT report completion until this exits 0.

---

## Step 10 — Return result

Report back:
1. The full file path written
2. `py_compile` result (exit 0 / error details)
3. Any pre-submit checklist items that were automatically resolved and any that need manual action (e.g. replace `OCS-XXXX` with real Polarion IDs)
