---
name: ocs-ci-verify-script
description: Generate pytest verification scripts compatible with ocs-ci conventions
---

# ocs-ci verification scripts (AI-generated)

## Important

**Claude generates tests** — the framework does not embed issue-specific scenarios in Python.

After `execute_issue.sh` or the coordinator reaches script-generation:

1. Open `artifacts/{KEY}/verification-generation-prompt.md`
2. Search ocs-ci for relevant tests/helpers (`Grep`, codebase search)
3. Write `reproduce.py`, `verify.sh`, `repro-steps.yaml`, `test-environment.yaml`

`reproduce.py` must **not** contain `assert True` or TODO-only stubs.

## Layout

`artifacts/{KEY}/reproduce.py` — pytest run on cluster via `verify.sh`

## Conventions

- `logging`, retries, cleanup
- Prefer `ocs_ci` helpers when running with repo root on `PYTHONPATH`
- Run from ocs-ci root or set `PYTHONPATH` in `verify.sh`

## Validate

```bash
python3 .claude/jira-repro/check_script_generated.py --art .claude/workspace/artifacts/DFBUGS-XXXX
.claude/hooks/safety/validate_script.sh .claude/workspace/artifacts/DFBUGS-XXXX/verify.sh
```

## Optional auto-invoke

```bash
export DFBUGS_AUTO_GENERATE=1   # requires `claude` CLI
.claude/framework/orchestrator/execute_issue.sh DFBUGS-XXXX
```
