## UI Test Automation for RDR

### Locator system

**Never write raw XPath or CSS strings in test code.** All locators live in `ocs_ci/ocs/ui/views.py`:

```python
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version
from ocs_ci.ocs.ui.helpers_ui import format_locator

acm_loc = locators_for_current_ocp_version()["acm_page"]
acm_obj.do_click(format_locator(acm_loc["cluster_name"], cluster_name))
```

Locator tuple format: `(selector_string, By.TYPE)` — **reversed** from Selenium's native order.

Presence checks use `locator[::-1]` (Selenium-native order):
```python
acm_obj.check_element_presence(acm_loc["some-key"][::-1], timeout=5, use_fallback=False)
```

### Version-compatible selector rules

| Rule | Rationale |
|---|---|
| Use `data-test` or `data-test-id` CSS selectors first | Stable OCS-CI test IDs |
| Use `aria-label` as second choice | PatternFly-mandated, survives refactors |
| Use `By.ID` when the element has a stable `id` attribute | Most stable selector type |
| Use `normalize-space()` in XPath for text matching | Trims invisible whitespace in PatternFly components |
| **Avoid** `pf-c-` / `pf-v5-` class selectors | Changes between OCP minor versions |
| **Avoid** positional XPath (`//div[3]`, `td[2]`) | DOM structure changes across PF versions |

### Key UI functions in `dr_helpers_ui.py`

| Function | When to call | Notes |
|---|---|---|
| `dr_submariner_validation_from_ui(acm_obj)` | Before stopping cluster; before UI failover | Always preceded by `config.switch_acm_ctx()`. |
| `check_cluster_status_on_acm_console(acm_obj, down_cluster_name=..., expected_text="Unknown")` | After primary nodes stopped | Omit `down_cluster_name` for all-clusters ready check. |
| `failover_relocate_ui(acm_obj, ..., action=constants.ACTION_FAILOVER)` | In place of `dr_helpers.failover()` | Navigates ACM Applications, opens kebab, selects action, checks readiness, clicks Initiate. |
| `failover_relocate_ui(acm_obj, ..., action=constants.ACTION_RELOCATE)` | In place of `dr_helpers.relocate()` | Same flow. |
| `verify_pending_cleanup_alert_firing(acm_obj, action, drpc_name)` | After discovered-apps failover (OCS ≥ 4.22) | See discovered-apps.ref.md. |
| `verify_pending_cleanup_alert_resolved(acm_obj, action, drpc_name)` | After discovered-apps cleanup (OCS ≥ 4.22) | See discovered-apps.ref.md. |

### PatternFly-specific gotchas

| Gotcha | Fix |
|---|---|
| `ElementClickInterceptedException` on canvas elements | Use `acm_obj.click_with_script(locator)` instead of `do_click` |
| Kebab menu closes before item is clicked | Use the `for _ in range(10): try/except` retry loop (already in `failover_relocate_ui`) |
| Text matching with `text()=` fails | Use `normalize-space()` or `contains(text(), ...)` in XPath |
| `wait_until_expected_text_is_found` fires AI fallback on expected-absent elements | Pass `use_fallback=False` for negative checks |

### AI-based locator fallback

`BaseUI` has a built-in `LocatorFallback` that fires on `TimeoutException`. Disable it explicitly on:
- Elements that SHOULD NOT exist (negative assertion)
- Canvas/SVG elements (fallback can't help)
- Fast poll loops (fallback wastes minutes on expected misses)

```python
acm_obj.check_element_presence(locator[::-1], timeout=3, use_fallback=False)
```

### UI test pre-submit checklist

- [ ] `setup_acm_ui` is in the method signature (always, even when some params have `via_ui=False`)
- [ ] `if via_ui: acm_obj = AcmAddClusters()` is the ONLY place `AcmAddClusters` is constructed
- [ ] `config.switch_acm_ctx()` precedes every `acm_obj.*` call
- [ ] `if via_ui` wraps **only** the action trigger — never the verification steps
- [ ] All new locators are in `views.py` — never inline in test or helper code
- [ ] New locators use `data-test`, `aria-label`, or `By.ID` — not `pf-c-` / `pf-v5-` class selectors
- [ ] `use_fallback=False` on all negative element checks and fast-poll loops
- [ ] Canvas-overlaid elements use `click_with_script` not `do_click`
