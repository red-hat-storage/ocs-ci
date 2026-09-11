# UI Test Guidelines — Black Squad

Applies to: `ocs_ci/ocs/ui/`, `tests/functional/ui/`, `tests/cross_functional/ui/`

---

## 1. SeleniumDriver — Singleton Access

**Rule:** Never reference `self.driver` directly. Always obtain the WebDriver instance through the singleton:

```python
# WRONG
element = self.driver.find_element(By.XPATH, "//button")

# CORRECT — get the singleton
driver = SeleniumDriver()
element = driver.find_element(By.XPATH, "//button")
```

`SeleniumDriver` is implemented with `__new__` in `ocs_ci/ocs/ui/base_ui.py` and returns the same `WebDriver` instance on every call. `BaseUI.__init__` stores it as `self.driver` for internal framework use; child classes and test files must call `SeleniumDriver()` instead of touching `self.driver`.

---

## 2. Page Object Model Architecture

### Inheritance chain

```
BaseUI                          # ocs_ci/ocs/ui/base_ui.py
└── PageNavigator               # page_objects/page_navigator.py
    ├── CreateResourceForm      # page_objects/data_foundation_tabs_common.py
    ├── DataFoundationTabBar
    │   └── InfraHealthModal
    └── BlockAndFile
        └── CephBlockPool

BaseUI
└── SearchBar                   # page_objects/searchbar.py
    └── ResourceList            # page_objects/resource_list.py

# Multiple-inheritance composition
StoragePools(CreateResourceForm, EditLabelForm, ResourceList)
StorageClusterPage(BlockAndFile, ObjectStorage, StoragePools, TopologyTab, EncryptionModule)
DataFoundationOverview(InfraHealthModal, PageNavigator)
```

### When to create a new POM class

A POM class is appropriate for any reusable UI surface — a full page, a wizard step, a modal dialog, or a UI card. The deciding question is: *will more than one test or POM method interact with this element set?* If yes, extract it into a class.

Examples: Storage Cluster wizard, Object Storage tab, Topology card, Block Pools list, Consumer management panel, Disaster Recovery card.

### Choosing the right parent

| Scenario | Inherit from |
|----------|-------------|
| Component always visible in the sidebar/nav | `PageNavigator` |
| Modal or form that creates a resource | `CreateResourceForm` |
| Page with a searchable list | `ResourceList` (-> `SearchBar`) |
| Label editing capability | `EditLabelForm` |
| Multiple capabilities combined | Multiple inheritance: `MyClass(CreateResourceForm, EditLabelForm, ResourceList)` |
| No closer fit | `BaseUI` |

### Navigation methods

Navigation methods must return the target page-object instance to enable fluent chaining:

```python
# CORRECT
def navigate_to_block_pools(self):
    self.do_click(self.page_nav["block_pools_link"])
    return StoragePools()
```

### POM methods must not assert

Assertions belong exclusively in test files. POM methods describe actions, not expectations.

```python
# WRONG — assertion in POM
def create_pool(self, name):
    ...
    assert "Pool created" in self.get_toast_text()

# CORRECT — POM returns data; test asserts
def get_toast_text(self):
    return self.get_element_text(self.generic_locators["toast_message"])

# in test file:
pools.create_pool("my-pool")
assert "Pool created" in pools.get_toast_text()
```

### Helper functions

New helper functions must be added to the relevant POM class, not to the test file. If a helper is generic enough to apply across many POM classes, add it to `BaseUI` or `helpers_ui.py`.

---

## 3. Locators

### Everything lives in `views.py`

All locators must be defined in `ocs_ci/ocs/ui/views.py`. No `By.*` calls or selector strings anywhere else.

```python
# WRONG — locator in POM file
element = self.do_click((By.XPATH, "//button[@data-test='submit']"))

# CORRECT — locator in views.py, accessed via BaseUI attribute
element = self.do_click(self.generic_locators["submit_btn"])
```

### Locator tuple order

Locators are stored as `(selector_string, By.TYPE)` — **selector first, By type second**. This is reversed from Selenium's native `(By.TYPE, selector)`. The framework's `do_click`, `WebDriverWait` wrappers handle the reversal internally.

```python
# views.py
generic_locators = {
    "submit_btn": ("//button[@data-test='submit']", By.XPATH),
}
```

### Accessing locators — BaseUI attributes only

Every locator category is exposed as an attribute in `BaseUI.__init__`. Always use these attributes; never access the raw `locators` dict.

| Attribute | Category |
|-----------|----------|
| `self.generic_locators` | Generic/shared elements |
| `self.page_nav` | Page navigation |
| `self.pvc_loc` | PVC management |
| `self.bp_loc` | Block pools |
| `self.sc_loc` | Storage classes |
| `self.obc_loc` | OBC |
| `self.topology_loc` | Topology tab |
| `self.alerting_loc` | Alerting |
| `self.vm_loc` | VirtualMachine |
| `self.data_foundation_overview` | DF overview tab |
| ... | (see `BaseUI.__init__` for full list) |

When adding a new locator category: register it in the `locators` master dict in `views.py` **and** add the corresponding attribute in `BaseUI.__init__`. Both changes are required.

### Dynamic locators

Use `format_locator()` from `helpers_ui.py` for locators with `{}` placeholders. Never use f-strings or string concatenation.

```python
# views.py
generic_locators = {
    "resource_link": ("//a[@data-test='resource-link-{}']", By.XPATH),
}

# POM file
from ocs_ci.ocs.ui.helpers_ui import format_locator

loc = format_locator(self.generic_locators["resource_link"], resource_name)
self.do_click(loc)
```

---

## 4. Locator Quality Rules

### HARD requirements for new locators

1. **No multi-alternative syntax in new locators.** The `|` / `or` / `,` pattern is reserved for upgrading existing locators across OCP versions. New locators must be single, unique selectors from the start.

2. **No PatternFly version-prefixed class names.** Class names like `pf-v5-c-button` or `pf-c-button` are versioned and break on upgrades. Use `contains()` or CSS substring matching instead.

   ```python
   # WRONG
   ("//button[contains(@class, 'pf-v5-c-button')]", By.XPATH)

   # CORRECT — version-agnostic
   ("//button[contains(@class, 'c-button')]", By.XPATH)
   ("button[class*='c-button']", By.CSS_SELECTOR)
   ```

3. **No index-based selectors.** Indexes shift with content changes.

   ```python
   # WRONG
   ("(//button[@class='c-button'])[2]", By.XPATH)
   ```

4. **No auto-generated locators** from AI tools, Blazemeter, or Selenium IDE recordings. These are long, brittle, and tied to DOM structure.

### Recommended strategies (best first)

| Priority | Strategy | Example |
|----------|----------|---------|
| 1 | `data-test` attribute | `//button[@data-test='create-storage-system']` |
| 2 | `id` | `By.ID, "pf-tab-2-odf-dashboard-tab"` |
| 3 | `name` | `By.NAME, "object-tab"` |
| 4 | XPath with `normalize-space()` text | `//button[normalize-space()='Create']` |
| 5 | XPath combining 2-3 stable attributes | `//button[@role='button' and @data-test='create']` |
| 6 | CSS with class substring | `button[class*='c-button--primary']` |
| 7 | XPath with `contains(@class, ...)` | `//div[contains(@class, 'c-menu-toggle')]` |

Prefer XPath over CSS selector. Prefer `data-test` attributes over generated class names.

### XPath axes for relative navigation

Use XPath axes when a unique anchor element exists but the target element has no distinguishing attributes:

```python
# Find label next to a sibling input
"//input[@data-test='pool-name']/preceding-sibling::label"

# Find a button in the same row as a known cell
"//td[normalize-space()='my-pool']/parent::tr//button[@aria-label='Actions']"
```

Available axes: `child::`, `parent::`, `ancestor::`, `descendant::`, `following::`, `preceding::`, `following-sibling::`, `preceding-sibling::`, `self::`.

### Resilient locators for OCP version upgrades

When updating an existing locator to support a new OCP version while keeping the old one for a release branch (one ODF version below OCP), use the `|` operator — but cap at 2 alternatives:

```python
# views.py — versioned override dict pattern
generic_locators_4_20 = {
    "nav_sidebar": (
        "//nav[@aria-label='Nav']//a[normalize-space()='Storage'] | "
        "//nav[@aria-label='Navigation']//a[normalize-space()='Storage']",
        By.XPATH,
    ),
}
```

Merge the override into the version dict: `{**generic_locators, **generic_locators_4_20}`.

---

## 5. Versioned Locators and OCP Version Support

Locators are keyed by **OCP version**, not ODF version, because the management console belongs to OpenShift.

```python
# views.py — master dict
locators = {
    "5.0": {
        "generic": {**generic_locators, **generic_locators_4_19},
        ...
    }
}
locators["4.23"] = locators["5.0"]
```

At runtime, `locators_for_current_ocp_version()` (in `helpers_ui.py`) selects the correct dict and falls back to the latest known version when OCP is newer.

**Rules:**
- Never hardcode OCP version strings in POM files.
- Version-specific override dicts follow the naming pattern `<area>_4_XX`.
- When ODF is one version behind OCP, the `|` locator syntax bridges the gap.

---

## 6. Wait Strategy

**No `time.sleep()`** anywhere in UI code or tests.

Use the helpers from `ocs_ci/ocs/ui/helpers_ui.py` and `BaseUI`:

| Situation | Helper |
|-----------|--------|
| Wait for element clickable | `wait_for_element_to_be_clickable(locator)` |
| Wait for page load | `page_has_loaded(timeout, sleep, locator)` |
| Wait for element visible | `WebDriverWait(driver, timeout).until(EC.visibility_of_element_located(loc))` |
| Wait for text in element | `WebDriverWait(driver, timeout).until(EC.text_to_be_present_in_element(loc, text))` |

Always prefer explicit waits over implicit waits.

---

## 7. Test File Conventions

- Tests interact with UI **exclusively through POM classes**. No `find_element`, `click`, `send_keys`, or `By.*` in test files.
- No `SeleniumDriver()` calls in test files; get the driver through POM methods if needed.
- Assertions are in the test file only — POM methods return data, not verdicts.
- Each test must be independent: create what it needs, yield for teardown, clean up on exit.
- Use descriptive assertion messages: `assert pool_exists, f"Pool '{name}' not found after creation"`.

---

## 8. Selenium Driver Integrity

Never modify the WebDriver instance for the purpose of a single test or POM method. The driver is a shared singleton managed by the framework and any mutation leaks across tests, producing non-deterministic failures.

**Forbidden modifications:**

```python
# WRONG — changes implicit wait globally for all subsequent tests
SeleniumDriver().implicitly_wait(10)

# WRONG — changes window size; may break viewport-dependent locators in other tests
SeleniumDriver().set_window_size(1024, 768)

# WRONG — closing the connection inside a test tears down the shared singleton
SeleniumDriver().quit()
```

If a test genuinely requires a different viewport, coordinate with the squad before changing framework-level defaults. Never apply such changes inline.

---

## 9. Locator Comments

Do not add inline comments to locator entries in `views.py`. The locator key name must fully describe the web element it targets — this is both a readability requirement and a functional one: the AI locator fallback uses the variable name at runtime to find the element when the selector no longer matches.

```python
# WRONG — comment is noise; key name should speak for itself
generic_locators = {
    "create_btn": ("//button[@data-test='create']", By.XPATH),  # Create button in toolbar
}

# CORRECT — key name is self-explanatory; no comment needed
generic_locators = {
    "toolbar_create_button": ("//button[@data-test='create']", By.XPATH),
}
```

Keep `views.py` lean. If a key name is ambiguous, rename the key — do not paper over it with a comment.

---

## 10. Logging in UI Code

UI tests produce no automatic per-command logs: Selenium clicks and waits are silent by default. To make test execution readable in CI output and during debugging, every meaningful action must be explicitly logged.

Follow `docs/logging_guide.md` for level selection. The rules below are the UI-specific application of those guidelines.

### Required log points

| Situation | Level | Example |
|-----------|-------|---------|
| Major test phase | `logger.test_step()` | `"Navigate to Block Pools page"` |
| Navigation to a new page | `logger.info()` | `"Navigating to Storage -> Block Pools"` |
| Form field filled | `logger.info()` | `f"Setting pool name to '{name}'"` |
| Button click that triggers state change | `logger.info()` | `"Clicking 'Create' to submit pool form"` |
| Wait started | `logger.info()` | `f"Waiting for pool '{name}' to appear in list"` |
| Assertion | `logger.assertion()` | `f"Pool visible: expected=True, actual={visible}"` |
| Element not found (expected absence) | `logger.info()` | `f"Confirmed element '{key}' is not present"` |

### Example — correct logging density

```python
def create_block_pool(self, name, replica_count=3):
    logger.info(f"Creating block pool: name='{name}', replicas={replica_count}")
    self.do_click(self.bp_loc["create_block_pool_btn"])

    logger.info(f"Filling pool name: '{name}'")
    self.do_send_keys(self.bp_loc["pool_name_input"], name)

    logger.info(f"Setting replica count to {replica_count}")
    self.select_replica_count(replica_count)

    logger.info("Submitting pool creation form")
    self.do_click(self.bp_loc["submit_btn"])
    return self
```

### Anti-patterns

```python
# WRONG — silent method; impossible to follow in logs
def create_block_pool(self, name):
    self.do_click(self.bp_loc["create_block_pool_btn"])
    self.do_send_keys(self.bp_loc["pool_name_input"], name)
    self.do_click(self.bp_loc["submit_btn"])

# WRONG — over-logging with duplicate information (see logging_guide.md)
logger.test_step("Fill pool name field")
logger.info("Filling the pool name field with value")  # restates the step
```

---

## 11. AI Fallback — Absence Checks

The AI locator fallback (`_locator_fallback` in `BaseUI`) is designed to locate elements that are **expected to be present** when the primary selector fails. It must not be used to verify that an element is absent.

**Never infer absence through an exception:**

```python
# WRONG — triggers AI fallback on TimeoutException, hides the real intent
try:
    self.wait_for_element_to_be_clickable(self.generic_locators["delete_btn"])
    assert False, "Delete button should not be visible"
except TimeoutException:
    pass  # incorrectly treats absence as success
```

**Use `get_elements()` instead:**

```python
# CORRECT — explicit absence check without triggering AI fallback
elements = self.get_elements(self.generic_locators["delete_btn"])
assert len(elements) == 0, "Delete button should not be visible after resource deletion"

# OR: check visibility directly
logger.info("Verifying delete button is not visible")
assert not self.is_element_visible(self.generic_locators["delete_btn"]), \
    "Delete button unexpectedly visible"
logger.assertion("delete_btn visible: expected=False, actual=False")
```

**Why this matters:** If the AI fallback fires on an absence check, it may succeed in finding a similarly-named element elsewhere in the DOM and return a false positive — the test passes but the assertion is wrong. Using list-based helpers bypasses the fallback entirely and makes the intent clear.

---

## 12. Additional Patterns

### `deep_get()` for nested dict access

```python
# CORRECT
value = self.deep_get(self.generic_locators, "nested", "key", default=None)

# WRONG — unguarded chaining raises KeyError
value = self.generic_locators["nested"]["key"]
```

### Screenshots on failure

Use the framework's `take_screenshot(name_suffix)` in error paths. Use name_suffix with meaningful name. Do not write custom screenshot logic.

### POM method chaining

Action methods should return `self` or the next page object to enable chaining:

```python
def fill_name(self, name):
    self.do_send_keys(self.bp_loc["pool_name_input"], name)
    return self

# fluent usage in test:
BlockPool().fill_name("ec-pool").select_replicas(2).submit()
```
