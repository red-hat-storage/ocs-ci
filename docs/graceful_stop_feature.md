# Graceful Stop Feature for ocs-ci Test Execution

## Overview

The graceful stop feature allows you to safely abort a running ocs-ci test execution by creating a stop file in the repository root directory. This ensures that the currently running test completes properly with all fixtures and teardown operations, while skipping all remaining tests in the queue.

## Stop File Options

There are two stop file options available:

### 1. `.stop` - Fast Stop (No Log Collection)

Creates a stop file that skips log collection for faster termination.

**Usage:**
```bash
# SSH to the executor where ocs-ci is running
cd /path/to/ocs-ci

# Create the stop file
touch .stop
```

**Behavior:**
- Current test completes normally with all fixtures and teardown
- Test results are recorded (passed/failed/skipped)
- **Log collection is skipped** for failed tests
- All remaining tests are skipped
- Faster termination

**Use when:**
- You need to stop quickly
- Logs are not critical
- You want to minimize execution time

### 2. `.stop_gracefully` - Graceful Stop (With Log Collection)

Creates a stop file that allows log collection to proceed normally.

**Usage:**
```bash
# SSH to the executor where ocs-ci is running
cd /path/to/ocs-ci

# Create the graceful stop file
touch .stop_gracefully
```

**Behavior:**
- Current test completes normally with all fixtures and teardown
- Test results are recorded (passed/failed/skipped)
- **Log collection proceeds normally** for failed tests (must-gather, etc.)
- All remaining tests are skipped
- Takes longer due to log collection

**Use when:**
- You need logs from the current test
- Debugging is important
- You can wait for log collection to complete

## How It Works

### Detection
- Before each test starts, the framework checks for the presence of `.stop` or `.stop_gracefully` files in the repository root directory (`TOP_DIR`)
- The check happens in the `pytest_runtest_setup` hook

### Execution Flow

1. **Stop file detected**: Framework logs a warning message
2. **Current test**: Completes normally with all setup/teardown
3. **Test results**: Written to `failed_testcases.txt`, `passed_testcases.txt`, or `skipped_testcases.txt`
4. **Log collection**:
   - `.stop`: Skipped
   - `.stop_gracefully`: Proceeds normally
5. **Remaining tests**: All skipped with appropriate skip message
6. **Fixtures**: All teardown/finalizers execute properly

### Multicluster Support

The feature fully supports multicluster scenarios:
- Stop flags are set in the RUN config for all clusters
- Log collection behavior is consistent across all clusters
- Each cluster's context respects the stop flags

## Examples

### Example 1: Quick Stop During Long Test Run

```bash
# You're running a 100-test suite and want to stop after test 25
ssh executor-host
cd /path/to/ocs-ci
touch .stop

# Result:
# - Test 25 completes
# - Tests 26-100 are skipped
# - No log collection
# - Clean teardown of all resources
```

### Example 2: Stop with Debugging

```bash
# Test is failing and you want logs before stopping
ssh executor-host
cd /path/to/ocs-ci
touch .stop_gracefully

# Result:
# - Current test completes
# - Logs collected if test fails
# - Remaining tests skipped
# - Clean teardown of all resources
```

### Example 3: Removing Stop File

```bash
# If you change your mind, just remove the file
rm .stop
# or
rm .stop_gracefully

# Tests will continue normally
```

## Log Messages

### When `.stop` is detected:
```
WARNING: Stop file detected at /path/to/ocs-ci/.stop.
Current test will complete without log collection, remaining tests will be skipped.

Skipping test: tests/functional/... - Stop requested via .stop file - skipping log collection

INFO: Skipping log collection due to .stop file - use .stop_gracefully if you want logs collected
```

### When `.stop_gracefully` is detected:
```
WARNING: Graceful stop file detected at /path/to/ocs-ci/.stop_gracefully.
Current test will complete with log collection, remaining tests will be skipped.

Skipping test: tests/functional/... - Graceful stop requested via .stop_gracefully file
```

## Technical Details

### Implementation Files

- **`ocs_ci/framework/pytest_customization/ocscilib.py`**:
  - `check_stop_file()`: Checks for stop files
  - `set_stop_flags_for_all_clusters()`: Sets flags in config
  - `pytest_runtest_setup()`: Hook that checks before each test
  - `pytest_runtest_makereport()`: Hook that controls log collection

- **`ocs_ci/ocs/constants.py`**:
  - `TOP_DIR`: Repository root directory constant

### Configuration Flags

The following flags are set in `ocsci_config.RUN`:
- `stop_requested` (bool): True when any stop file is detected
- `graceful_stop` (bool): True for `.stop_gracefully`, False for `.stop`

## Best Practices

1. **Use `.stop_gracefully` by default** unless you're in a hurry
2. **Remove stop files** after use to avoid accidentally stopping future runs
3. **Check logs** to confirm the stop was detected and processed correctly
4. **Wait for current test** to complete - don't force kill the process
5. **Verify cleanup** after stop to ensure no resources are left behind

## Troubleshooting

### Stop file not working?

1. **Check file location**: Must be in repository root (`TOP_DIR`), not in a subdirectory
2. **Check file name**: Must be exactly `.stop` or `.stop_gracefully` (note the leading dot)
3. **Check timing**: File must exist before the next test starts
4. **Check permissions**: Ensure you have write access to create the file

### Tests still running after stop?

- The current test must complete first
- Wait for fixtures and teardown to finish
- Check logs for stop detection messages

### Logs not collected with `.stop_gracefully`?

- Verify the file name is exactly `.stop_gracefully`
- Check that `--collect-logs` was passed to run-ci
- Review log messages for any errors during collection

## Comparison with Other Stop Methods

| Method | Current Test | Fixtures | Log Collection | Remaining Tests | Safety |
|--------|--------------|----------|----------------|-----------------|--------|
| `.stop` | ✅ Completes | ✅ Runs | ❌ Skipped | ⏭️ Skipped | ✅ Safe |
| `.stop_gracefully` | ✅ Completes | ✅ Runs | ✅ Runs | ⏭️ Skipped | ✅ Safe |
| `Ctrl+C` (once) | ✅ Completes | ✅ Runs | ✅ Runs | ❌ Aborted | ⚠️ Depends |
| `Ctrl+C` (twice) | ❌ Aborted | ❌ Skipped | ❌ Skipped | ❌ Aborted | ❌ Unsafe |
| `kill -9` | ❌ Killed | ❌ Skipped | ❌ Skipped | ❌ Killed | ❌ Unsafe |
| `kill -TERM` | ✅ Completes | ✅ Runs | ✅ Runs | ❌ Aborted | ✅ Safe |

## Future Enhancements

Potential improvements for this feature:
- Web UI to create/remove stop files remotely
- Email notification when stop is detected
- Configurable stop behavior per test suite
- Stop after N more tests instead of immediately
- Integration with CI/CD pipelines
