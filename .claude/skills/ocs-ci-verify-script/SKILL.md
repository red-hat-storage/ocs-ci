---
name: ocs-ci-verify-script
description: Generate pytest verification scripts compatible with ocs-ci conventions
---

# ocs-ci verification scripts

## Layout

Place tests under `artifacts/{KEY}/reproduce.py` or map to `tests/` if promoting to ocs-ci.

## Conventions

- Use `logging` module; follow `docs/logging_guide.md`
- Prefer existing helpers from `ocs_ci` when running inside ocs-ci venv
- Mark destructive tests with explicit guard and coordinator approval

## Template

Start from `.claude/jira-repro/templates/verify.py`.

## Run

```bash
# From ocs-ci repo root with venv active
pytest .claude/workspace/artifacts/DFBUGS-XXXX/reproduce.py -v
```

Include fixtures for namespace/PVC cleanup.
