---
name: dry-run
description: Dry-run mode — execute workload without JIRA or GitHub mutations
---

# Dry-run mode

## Enable

```bash
.claude/framework/orchestrator/run.sh --workflow zstream-issue-verification <odf-version> --dry-run
# or
export DFBUGS_DRY_RUN=1
eval "$(.claude/framework/lib/load_run_context.sh)"
```

Marker: `$JIRA_AGENT_WORKSPACE/.dry-run` and `run-config.json` → `"dry_run": true`.

Check: `python3 .claude/framework/lib/dry_run.py` → prints `dry-run` or `live`.

## Executes normally

- JIRA **read** (`jira_issue_get`, search/JQL discovery)
- GitHub **search** (duplicate detection)
- Script generation, safety validation, pytest/oc on cluster
- Cluster health collection and analysis
- Infra diagnosis, local SQLite memory, reports/artifacts

## Skipped (write drafts instead)

| Action | Draft path |
|--------|------------|
| JIRA comment | `planned-actions/jira.json` |
| JIRA transition/label | `planned-actions/jira.json` |
| GitHub issue create | `planned-actions/github-issue-draft.md` |

## Outcome JSON

Always set `"dry_run": true` on `outcomes/{KEY}.json` when active.

## Policy

`.claude/configs/policies/safety.yaml` → `dry_run` section.
