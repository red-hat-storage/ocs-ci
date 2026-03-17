# Claude Review Guide for OCS-CI

This guide complements `.claude/CLAUDE.md` — it defines **how to review** code, not how to write it.

## Review Scope

- Only review code added or modified by the PR — never flag existing code
- If the codebase already uses a pattern, don't flag it in new code that follows the same pattern
- Read docstrings and comments to understand the author's intent before flagging design decisions
- Ask "is this a realistic failure scenario?" — not "could this theoretically fail?"

## Severity Calibration

Map findings to severity levels based on real impact, not theoretical risk.

### BLOCKER

These violate critical project rules and must always be flagged:

- `except Exception` or bare `except:` — must use specific exceptions
- UI locators defined outside `ocs_ci/ocs/ui/views.py` (unless the locator contains a variable)
- Fixtures using `yield` — must use `request.addfinalizer()` instead
- Tier marker at class level when `parametrize` marks have different tiers
- Test classes that create resources without a `teardown()` method
- Version-specific CSS selectors (`.pf-c-` instead of `[class*='c-']`)

### HIGH

- Missing type hints on new public method parameters or return values
- Missing docstrings on new public methods
- Magic numbers (unnamed timeout values like `time.sleep(2)`)
- Use of `@pytest.mark.usefixtures` instead of passing fixtures as parameters
- Global variables used for sharing state between tests

### MEDIUM

- Missing `log_step()` in UI test methods (non-UI tests use `logger.info()`)
- Missing fallback selectors for UI elements known to be flaky
- Inconsistent return types (method returns different types based on a flag)

### LOW

- Missing exception chaining (`from e`)
- Minor formatting issues not caught by black/flake8

## File-Type Guidance

### conftest.py

- Focus on fixture teardown: are resources cleaned up via `request.addfinalizer()`?
- Check for `yield` usage — this is a BLOCKER
- Verify no `request.node.cls` usage for setting/reading class attributes
- Ignore docstrings on fixtures (per project convention)

### Page Objects (`ocs_ci/ocs/ui/page_objects/`)

- Verify locators are in `views.py`, not inline
- Check navigation pattern: destination pages should NOT navigate to themselves
- Look for lazy imports to prevent circular dependencies
- Verify selectors are version-agnostic (`[class*='c-']` not `.pf-c-`)

### Test Files (`tests/`)

- Verify `teardown()` exists if the class creates resources
- Check tier markers are only in `parametrize` marks, not at class level
- Verify `log_step()` usage in UI tests
- Check that factories are used for resource creation (automatic cleanup)
- Verify `create_unique_resource_name()` for naming resources

### views.py (`ocs_ci/ocs/ui/views.py`)

- Verify selectors follow the locator priority order: `data-test` > `id` > `aria-label` > text+ancestor > class > index
- Check for version-agnostic patterns
- Verify fallback selector lists for unreliable elements

## What NOT to Flag

- Style issues caught by black or flake8 (formatting, import ordering, line length)
- Existing codebase patterns used consistently in new code
- Missing docstrings on fixtures (project convention exempts them)
- Import ordering or grouping
- Use of `time.sleep()` when the timeout value IS a named constant
