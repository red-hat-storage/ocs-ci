---
name: run-context
description: Load ODF version and workflow metadata from the current bootstrap (never hardcode)
---

# Run context

The ODF z-stream is **always** the version the user passed to `run.sh`, stored in the workspace.

## Load before any agent step

```bash
eval "$(.claude/framework/lib/load_run_context.sh)"
# ODF_VERSION, WORKFLOW_ID, RUN_ID, JIRA_STATUS, JIRA_PROJECT, DFBUGS_DRY_RUN (if dry-run)
```

Or read JSON:

```bash
python3 .claude/framework/lib/load_run_context.py
python3 .claude/framework/lib/load_run_context.py --field odf_version
```

## Source files

1. `$JIRA_AGENT_WORKSPACE/active-run.json` (preferred)
2. `$JIRA_AGENT_WORKSPACE/run-config.json` (fallback)

Never hardcode versions like `4.19` in scripts, reports, or JQL.
