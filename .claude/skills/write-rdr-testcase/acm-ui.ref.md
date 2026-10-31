## ACM UI Flow
<!-- v1.1.0 — added ui_locators section -->

**Used in `test_failover_and_relocate.py` `via_ui=True` params.**

### Required fixtures

Add `setup_acm_ui` to the method signature (always include even when `via_ui=False` params exist).

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

`if via_ui` wraps the **action trigger only** — never the resource creation/deletion verification.

---

### Using `ui_locators` from the SPEC

The `ui_locators` block in the SPEC is a flat dict accumulated across **multiple page rounds** —
each entry was captured from a different ACM page at a different cluster state.
The keys follow the naming convention `page_N_<action>` (e.g. `page_1_kebab_trigger`,
`page_2_failover_action`, `page_2_initiate_button`).

When the SPEC contains a `ui_locators` block, the writer **must**:

1. **Add each locator to `views.py`** under the `acm_page` section before writing any test code.

   SPEC entries arrive in JSON-safe format — convert to the `views.py` reversed-tuple format
   (selector first, `By.TYPE` second) before inserting:

   ```python
   # ocs_ci/ocs/ui/views.py  — inside the acm_page dict for the relevant OCP version
   # SPEC: {selector: '[data-test="kebab-button"]', by: CSS_SELECTOR}  →
   "page_1_kebab_trigger":   ('[data-test="kebab-button"]',    By.CSS_SELECTOR),
   # SPEC: {selector: '[data-test="failover-action"]', by: CSS_SELECTOR}  →
   "page_2_failover_action": ('[data-test="failover-action"]', By.CSS_SELECTOR),
   # SPEC: {selector: '[aria-label="Initiate"]', by: CSS_SELECTOR}  →
   "page_2_initiate_button": ('[aria-label="Initiate"]',       By.CSS_SELECTOR),
   ```

2. **Reference them via `locators_for_current_ocp_version()`** — never inline the selector
   string in the test or helper:

   ```python
   acm_loc = locators_for_current_ocp_version()["acm_page"]
   acm_obj.do_click(acm_loc["failover_kebab_menu"])
   acm_obj.do_click(format_locator(acm_loc["failover_action_item"], workload_name))
   ```

3. **When `ui_locators` contains `PLACEHOLDER_<ACTION>` values** — add a `# TODO` comment
   in `views.py` for each one, and add a checklist note to the SPEC output:

   ```python
   "failover_action_item": "PLACEHOLDER_FAILOVER_ACTION",  # TODO: fill from DOM inspection
   ```

   Do **not** block writing the test — write it with the placeholder keys and flag them.

4. **Uniqueness rule** — if the gatherer flagged a locator as appearing multiple times in the DOM,
   use the scoped variant it provided (ancestor prefix). Do not simplify it.
