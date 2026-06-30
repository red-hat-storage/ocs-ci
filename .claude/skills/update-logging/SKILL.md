---
name: update-logging
description: Apply OCS-CI logging guidelines to a Python test file or module
authoritative_source: docs/logging_guide.md
last_synced: 2026-05-01
args:
  file_path:
    description: Path to the Python file to update
    required: true
---

# Update Logging

Applies OCS-CI logging guidelines from `docs/logging_guide.md` to Python files.

**Authoritative Source**: `docs/logging_guide.md` contains ALL guidelines, patterns, anti-patterns,
examples, log levels, and decision criteria. This skill is purely an automation wrapper.

## What This Skill Does

1. Reads the target file: {{ file_path }}
2. Scans for logging issues using criteria from `docs/logging_guide.md`
3. Fixes anti-patterns following patterns from `docs/logging_guide.md`
4. Adds strategic logging based on file type guidance in `docs/logging_guide.md`
5. Ensures log messages have appropriate context and structure per `docs/logging_guide.md`

## Execution Workflow

**CRITICAL - Read the Guide First**: Before making ANY changes, read `docs/logging_guide.md` in full.
It is your complete instruction set.

### Step 1: Identify File Type

Determine which category the file belongs to (see guide's "Usage by Code Type" section):
- Test files (`tests/`)
- Deployment code (`ocs_ci/deployment/`)
- Helper/utility files (`ocs_ci/helpers/`, `ocs_ci/utility/`)
- Resource classes (`ocs_ci/ocs/resources/`)
- Framework modules (`ocs_ci/framework/`)

Each type has different logging expectations documented in the guide.

### Step 2: Scan for Issues

Read the file and identify all logging issues documented in the guide's "Anti-Patterns to Avoid" section.

### Step 3: Fix Issues

Apply fixes using patterns from the guide's "Common Patterns" and level-specific sections
(CRITICAL, ERROR, WARNING, TEST_STEP, INFO, ASSERTION, DEBUG, AI_DATA).

### Step 4: Add Strategic Logging

Add missing logging at key points based on file type guidance and the guide's examples.

## Decision References

All decisions about logging come from `docs/logging_guide.md`:

- **Which log level?** → See guide's "Log Level Selection Guide" section
- **What patterns to apply?** → See guide's "Common Patterns" section
- **What anti-patterns to fix?** → See guide's "Anti-Patterns to Avoid" section
- **How to structure messages?** → See guide's "Special Topics" and level-specific examples
- **Exception handling?** → See guide's "Exception Logging" section
- **File type guidance?** → See guide's "Usage by Code Type" section
- **Test assertions?** → See guide's "Assertions: Before or After?" section
- **Loop/iteration logging?** → See guide's "Pattern: Iteration Logging" section
- **Performance concerns?** → See guide's "Performance-Sensitive Code" section

## Usage

```bash
/update-logging tests/functional/test_example.py
```

or

```bash
/update-logging ocs_ci/helpers/helpers.py
```

## Maintenance

When logging guidelines evolve:

1. **Update `docs/logging_guide.md`** (the authoritative source)
2. **Update `last_synced` date** in this file's frontmatter
3. **Test the skill** against sample files to verify it applies updated guidelines

**Note**: If the guide's structure or workflow changes significantly, this SKILL.md may need
updates to its workflow steps. Otherwise, all content changes happen in the guide only.
