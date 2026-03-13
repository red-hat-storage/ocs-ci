# JUnit XML Fix Utility

## Overview

This utility fixes corrupted JUnit XML files caused by SIGTERM interruption during ocs-ci test execution.

## The Problem

When `kill -TERM` is sent to a running ocs-ci process:
1. SIGTERM is converted to SIGINT to allow pytest fixtures to finalize
2. If SIGINT arrives during log collection (ThreadPoolExecutor cleanup), it causes a Python traceback
3. The traceback gets written into an unclosed `<error>` tag in the XML
4. The XML becomes malformed and cannot be parsed by CI/CD tools

## The Solution

The `fix_junit.py` script:
- Detects the corrupted testcase (last one with SIGTERM traceback)
- Creates a timestamped backup of the original file
- Removes the corrupted testcase
- Properly closes the XML structure
- Validates the fixed XML
- Replaces the original file with the fixed version

## Usage

### Automatic (Integrated with ocs-ci)

When you use `kill -TERM` on a running ocs-ci process, the XML is **automatically fixed** on exit.

No action needed! The SIGTERM handler in `ocs_ci/utility/framework/initialization.py` registers the fix function with `atexit`.

### Manual (Standalone Script)

If you have an already-corrupted XML file:

```bash
python3 scripts/python/xml/fix_junit.py /path/to/test_results.xml
```

### As Python Module

You can also import and use the function in your own scripts:

```python
from scripts.python.xml.fix_junit import fix_xml_file

# Fix a corrupted XML file
success = fix_xml_file('/path/to/test_results.xml')
if success:
    print("XML fixed successfully!")
```

## Example

### Before Fix

```xml
<testcase ...>
  <error message="...">
Traceback (most recent call last):
  File ".../initialization.py", line 35, in signal_term_handler
    os.kill(pid, signal.SIGINT)
  File ".../resource.py", line 603, in _sigint_handler
    sys.exit(signal_received)
SystemExit: 2</error></testcase></testsuite></testsuites>
```

The XML is malformed because the `<error>` tag contains raw traceback text.

### After Fix

```xml
</testcase>
</testsuite>
</testsuites>
```

The corrupted testcase is removed, and the XML is properly closed.

## What Gets Fixed

✅ **Removed:** Last testcase containing SIGTERM/SystemExit traceback
✅ **Created:** Timestamped backup (e.g., `test_results.xml.backup_20260213_103533`)
✅ **Validated:** Fixed XML is checked for well-formedness
✅ **Preserved:** All other test results remain intact

## Output Example

```
Fixing corrupted XML file: /path/to/test_results.xml
✓ Backup created: /path/to/test_results.xml.backup_20260213_103533
Found 372 </testcase> tags
Removing corrupted testcase (last one with SIGTERM traceback)
✓ Fixed XML is valid and well-formed
✓ Original file replaced with fixed version: /path/to/test_results.xml

============================================================
✓ XML file fixed successfully!
============================================================

Fixed file: /path/to/test_results.xml
Backup: /path/to/test_results.xml.backup_<timestamp>

Note: The last test case (interrupted by SIGTERM) was removed.
```

## Technical Details

### Detection Logic

The script:
1. Finds all `</testcase>` tags in the XML
2. Checks if there are at least 2 testcases
3. Uses the second-to-last `</testcase>` as the cutoff point
4. Removes everything after it (the corrupted testcase)
5. Adds proper closing tags: `</testsuite></testsuites>`

### Why This Works

- The last testcase is always the one interrupted by SIGTERM
- It contains the Python traceback in an unclosed `<error>` tag
- All previous testcases are complete and valid
- By removing only the last one, we preserve all other test results

### Validation

After fixing, the script validates the XML using Python's `xml.etree.ElementTree`:

```python
import xml.etree.ElementTree as ET
ET.fromstring(fixed_content)  # Raises exception if invalid
```

## Integration with ocs-ci

The fix is automatically triggered when:
1. `kill -TERM <pid>` is sent to ocs-ci
2. The SIGTERM handler detects `--junit-xml` in command line arguments
3. The fix function is registered with `atexit`
4. After pytest exits, the function runs automatically

See `ocs_ci/utility/framework/initialization.py` for implementation details.

## Troubleshooting

### "Found only 1 </testcase> tag(s)"

The XML may not be corrupted in the expected way, or there's only one test. The script will try to validate the XML as-is.

### "XML is invalid but cannot be fixed automatically"

The corruption is different than expected. You may need to manually inspect and fix the XML.

### Import fails

Make sure you're running from the ocs-ci repository root directory:

```bash
cd /path/to/ocs-ci
python3 -c "from scripts.python.xml.fix_junit import fix_xml_file"
```

## Related Documentation

- [Graceful Stop Feature](../../../docs/graceful_stop_feature.md) - Alternative to using `kill -TERM`
- [SIGTERM Handler](../../../ocs_ci/utility/framework/initialization.py) - Automatic XML fix integration

## Requirements

- Python 3.3+ (for namespace packages)
- No external dependencies (uses only stdlib)

## License

Part of the ocs-ci project.
