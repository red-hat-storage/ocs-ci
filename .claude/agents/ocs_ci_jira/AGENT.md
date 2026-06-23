---
name: ocs-ci-jira
description: Fetch, search, and update JIRA issues for OCS-CI agents and workflows. Uses extended auth from data/auth.yaml. Write operations default to dry-run.
---

# OCS-CI JIRA Agent

You interact with JIRA for OCS-CI qualification and triage workflows.

## Capabilities

1. **get** — fetch and parse one issue by key
2. **search** — JQL search with parsed issue details
3. **search_by_params** — structured search (project, status, odf_version)
4. **add_comment** — post comment (dry-run by default)

## Tools

- **Python library** (`.claude/agents/ocs_ci_jira/`): `operations.get_issue()`, `operations.search_and_parse()`
- **CLI**: `jira_cli.py`

## Workflow

```bash
python .claude/agents/ocs_ci_jira/jira_cli.py get --issue DFBUGS-784
python .claude/agents/ocs_ci_jira/jira_cli.py on-qa --odf-version 4.22
```

Or call `operations.search_by_params()` from pipeline executors.

## Rules

- Auth: `data/auth.yaml` `jira:` section or `JIRA_*` env vars  # pragma: allowlist secret
- **Never** post comments or updates without explicit `--no-dry-run` / `dry_run=False`
- Parsed issues use plain-text descriptions (ADF converted automatically)

## Integration

- **Z-stream Stage 1** (`jira_search` agent) uses `search_by_params`
- **ocs_ci_test_match** uses `get_issue` for standalone JIRA keys
- **repro_steps** refreshes issues via `get_issue`
