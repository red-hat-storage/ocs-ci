# RDR Skill System — Maintenance Guide

This file tells you **exactly which symbols to watch** in shared files that the RDR
test-writer agents depend on. Because these files are shared across the entire test suite,
you don't need to read the whole file — only the RDR-relevant symbols listed here.

---

## Quick reference — RDR-relevant symbols per shared file

### `tests/conftest.py`

Only these functions matter for RDR. Track changes to them in git diffs.

| Symbol | Current line | Skill file to update |
|---|---|---|
| `create_workload_factory()` | ~8019 | `rdr-writer/SKILL.md` Step 5 |
| `dr_workload` fixture | ~8204 | `rdr-writer/SKILL.md` Step 5 |
| `discovered_apps_dr_workload` fixture | ~8428 | `discovered-apps.ref.md` |
| `cnv_dr_workload` fixture | ~8305 | `cnv.ref.md` |
| `node_restart_teardown` fixture | ~5240 | `node-fault.ref.md` |
| `setup_acm_ui` fixture | ~6333 | `acm-ui.ref.md` |
| `all_dr_workloads` fixture | ~8752 | `rdr-writer/SKILL.md` Step 5 |

**How to scope a diff review:**
```bash
git diff HEAD~1 -- tests/conftest.py | grep "^[+-]" | grep -E \
  "create_workload_factory|dr_workload|discovered_apps_dr_workload|\
cnv_dr_workload|node_restart_teardown|setup_acm_ui|all_dr_workloads"
```
If that grep returns nothing, the RDR-relevant parts of `conftest.py` were not touched.

---

### `ocs_ci/ocs/ui/views.py`

Only these dicts matter for RDR UI tests.

| Symbol | Current line | Skill file to update |
|---|---|---|
| `acm_page_nav` (base ACM locators) | ~1128 | `ui-automation.ref.md` |
| `acm_page_nav_419` (OCP 4.19 overrides) | ~1153 | `ui-automation.ref.md` |
| `acm_page_nav_420` (OCP 4.20 overrides) | ~1174 | `ui-automation.ref.md` |
| `"acm_page"` dict blocks (per OCP version) | ~4309, 4390, 4469, 4537, 4601 | `ui-automation.ref.md` + `acm-ui.ref.md` |

**How to scope a diff review:**
```bash
git diff HEAD~1 -- ocs_ci/ocs/ui/views.py | grep "^[+-]" | grep -E \
  "acm_page_nav|acm_page"
```
If that grep returns nothing, the RDR-relevant parts of `views.py` were not touched.

**When a new locator key is added to `acm_page_nav` or `acm_page`:**
- If it's a new RDR UI action (failover, relocate, cluster status, submariner) →
  add a note in `ui-automation.ref.md` § Key UI functions table.
- If it replaces an existing locator → check `rdr-gatherer/SKILL.md` Phase 1b page
  sequence to see if the page description needs updating.

---

### `ocs_ci/framework/pytest_customization/marks.py`

Only these symbols matter for RDR.

| Symbol | Current line | Skill file to update |
|---|---|---|
| `rdr` | ~109 | `rdr-writer/SKILL.md` Step 3 |
| `dr_hub_recovery` | ~801 | `rdr-writer/SKILL.md` Step 3 + `hub-recovery.ref.md` |
| `turquoise_squad` | ~871 | `rdr-writer/SKILL.md` Step 3 |
| `tier1`, `tier2`, `tier4a`, `tier4b`, `tier4c` | ~62–68 | `rdr-gatherer/SKILL.md` Requirement 8 |
| `acceptance` | ~73 | `rdr-writer/SKILL.md` Step 4 |
| `skipif_ocs_version` | ~822 | `rdr-writer/SKILL.md` Step 3 |

**How to scope a diff review:**
```bash
git diff HEAD~1 -- ocs_ci/framework/pytest_customization/marks.py | grep "^[+-]" | grep -E \
  "^[+-]rdr|^[+-]dr_hub_recovery|^[+-]turquoise_squad|^[+-]tier[124]|^[+-]acceptance|^[+-]skipif_ocs_version"
```

---

### `conf/ocsci/dr_workload.yaml`

Every key in this file is potentially RDR-relevant. Track all additions.

| Key pattern | Skill file to update |
|---|---|
| `dr_workload_appset_*` | `rdr-gatherer/SKILL.md` Requirement 6 note |
| `dr_workload_subscription_placement_*` | `rdr-gatherer/SKILL.md` Requirement 6 note |
| `dr_workload_discovered_apps_*` | `discovered-apps.ref.md` |
| `dr_workload_cnv_*` | `cnv.ref.md` |
| Any new `_statefulsets` suffix key | `rdr-gatherer/SKILL.md` Requirement 6 note |

**How to scope a diff review:**
```bash
git diff HEAD~1 -- conf/ocsci/dr_workload.yaml
```
This file is small and RDR-specific — review the whole diff.

---

### `ocs_ci/helpers/dr_helpers.py`

This file is RDR-dedicated. Every public function is potentially relevant.

**How to verify after a change:**
```bash
# Functions in source but not in dr-helpers.ref.md:
comm -23 \
  <(grep -o "^def [a-z_]*" ocs_ci/helpers/dr_helpers.py | sed 's/^def //' | sort) \
  <(grep -o "\`[a-z_]*\`" .claude/skills/write-rdr-testcase/dr-helpers.ref.md | tr -d '`' | sort)
```
Any function that appears in existing RDR tests but is missing from `dr-helpers.ref.md`
should be added.

---

### `ocs_ci/helpers/dr_helpers_ui.py`

This file is RDR-dedicated. Every public function is potentially relevant.

**How to verify after a change:**
```bash
grep -n "^def " ocs_ci/helpers/dr_helpers_ui.py
```
Cross-check against the function table in `ui-automation.ref.md`.

---

## When adding a new RDR scenario type (new `.ref.md` file)

- [ ] Create `.claude/skills/write-rdr-testcase/<topic>.ref.md`
- [ ] First heading is `## <Topic Title>` — no "Addendum" or "Appendix" prefix
- [ ] Add condition → filename row to the addenda/appendices decision table in `rdr-gatherer/SKILL.md`
- [ ] Add `- '.claude/skills/write-rdr-testcase/<topic>.ref.md'` to `rdr-writer/SKILL.md` Step 1
- [ ] Add the file path + description to the orchestrator's Phase 2 reference block in `write-rdr-testcase/SKILL.md`
- [ ] Add a row to the skill files table in `README.md`
- [ ] Add an example prompt to `EXAMPLES.md`
- [ ] Add the new symbol/fixture to the relevant tracking section in this file

---

## Version bumping convention

When a skill file is meaningfully updated, bump its `version:` frontmatter field:

| Change type | Bump |
|---|---|
| New content added | Minor: `1.1.0` → `1.2.0` |
| Existing content corrected | Patch: `1.2.0` → `1.2.1` |
| Structural change (new phase, new step, new format) | Major: `1.2.0` → `2.0.0` |

`.ref.md` files use inline version comments instead of frontmatter:
```
<!-- v1.2.0 — added <what changed> -->
```

---

## Ownership

PRs that touch any symbol listed in this file should include a skill-update check
as part of the review. If the PR author doesn't update the skills, the reviewer should
flag it.

If you add a new reusable test pattern, also add an example prompt to `EXAMPLES.md` —
that is the main entry point for contributors who want to use the agent.
