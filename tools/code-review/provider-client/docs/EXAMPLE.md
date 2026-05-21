# Provider/Client Pattern Analyzer - Example Usage

## What It Does

The analyzer detects missing context managers and markers in Provider/Client mode code. It helps prevent bugs where provider-specific functions are called without proper context.

## Quick Start

```bash
# Analyze your current changes before committing
python tools/code-review/provider-client/review.py

# Check a specific file
python tools/code-review/provider-client/review.py tests/my_test.py

# Review changes in a PR
python tools/code-review/provider-client/review.py --pr 1234
```

## Example: Finding Violations

Create a test file with violations:

```python
# bad_example.py
from ocs_ci import framework

def test_provider_operations():
    """Test that needs provider access"""
    # ❌ VIOLATION: Missing context manager!
    address = get_provider_address()
    info = get_provider_info()

    print(f"Provider: {address}")
```

Run the analyzer:

```bash
$ python tools/code-review/provider-client/review.py bad_example.py

Found 2 issue(s):

1. bad_example.py:6:14
   [ERROR] missing-provider-context
   Call to 'get_provider_address' requires RunWithProviderConfigContextIfAvailable context manager
   Code: address = get_provider_address()
   Suggestion:
   Wrap the call in a provider context manager:

              with config.RunWithProviderConfigContextIfAvailable():
                  get_provider_address(...)

   Or add @runs_on_provider marker to the test function.

2. bad_example.py:7:11
   [ERROR] missing-provider-context
   Call to 'get_provider_info' requires RunWithProviderConfigContextIfAvailable context manager
   Code: info = get_provider_info()
   ...
```

## Example: Correct Pattern

Fix the code using one of two approaches:

### Approach 1: Context Manager (Recommended)

```python
from ocs_ci import framework

config = framework.config

def test_provider_operations():
    """Test that needs provider access"""
    # ✅ CORRECT: Using context manager
    with config.RunWithProviderConfigContextIfAvailable():
        address = get_provider_address()
        info = get_provider_info()

    print(f"Provider: {address}")
```

### Approach 2: Test Marker

```python
from ocs_ci.framework.pytest_customization.marks import runs_on_provider

@runs_on_provider
def test_provider_operations():
    """Test marked as provider-only"""
    # ✅ CORRECT: Marker indicates provider context
    address = get_provider_address()
    info = get_provider_info()

    print(f"Provider: {address}")
```

## Analyzing Git Diffs

The analyzer can check only changed lines:

```bash
# Check uncommitted changes
python tools/code-review/provider-client/review.py

# Check commits
python tools/code-review/provider-client/review.py --diff HEAD~3..HEAD

# Check branch against master
python tools/code-review/provider-client/review.py --diff master..your-branch
```

Output shows only issues in modified code:

```
📋 Analyzing git diff: master..HEAD...

Found 1 issue(s):

1. tests/new_test.py:42:18
   [ERROR] missing-provider-context
   Call to 'get_provider_address' requires RunWithProviderConfigContextIfAvailable context manager
   ...
```

## CI/CD Integration

### GitHub Actions (Future)

The POC can be extended to automatically comment on PRs:

```yaml
# .github/workflows/ai-code-review.yml
name: Provider/Client Pattern Check
on: [pull_request]

jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run pattern analyzer
        run: python tools/code-review/provider-client/review.py --diff origin/${{ github.base_ref }}..HEAD
```

### Claude API Integration (Future)

Next step would add Claude API to provide:
- Deeper code understanding
- Contextual suggestions
- Learning from codebase patterns
- Interactive review comments

## Customizing Patterns

Edit `tools/code-review/provider-client/analyzers/patterns.yaml` to add new patterns:

```yaml
provider_functions:
  - get_provider_address
  - get_provider_info
  - your_new_function  # Add your function here

provider_markers:
  - runs_on_provider
  - provider_client_required
  - your_new_marker  # Add your marker here

exemptions:
  paths:
    - "ocs_ci/deployment/hub_spoke.py"
    - "your_exempt_file.py"  # Exempt specific files
```

## Exit Codes

- `0` - No issues found or only warnings
- `1` - Errors detected

Use in CI pipelines:

```bash
# This will fail the build if errors are found
python tools/code-review/provider-client/review.py --diff master..HEAD || exit 1
```

## Limitations

Current POC limitations:
- Static analysis only (no runtime context)
- Basic AST parsing (may miss complex patterns)
- No AI-powered suggestions yet (coming in next phase)
- Doesn't check indirect calls

These will be addressed when integrating Claude API for intelligent review.
