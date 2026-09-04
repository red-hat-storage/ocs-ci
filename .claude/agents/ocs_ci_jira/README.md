# OCS-CI JIRA Agent

Fetch, search, and update JIRA issues for OCS-CI agents and YAML workflows.

**Package:** `.claude/agents/ocs_ci_jira/`

## Quick start

```bash
python .claude/agents/ocs_ci_jira/jira_cli.py get --issue DFBUGS-784
python .claude/agents/ocs_ci_jira/jira_cli.py on-qa --odf-version 4.22
python .claude/agents/ocs_ci_jira/jira_cli.py search --jql 'project = "Data Foundation Bugs" AND status = ON_QA'
```

## Python API

```python
from operations import get_issue, search_and_parse, search_by_params, add_comment

issue = get_issue("DFBUGS-784")
issues, jql = search_by_params({
    "project": "Data Foundation Bugs",
    "issue_type": "Bug",
    "odf_version": "4.22",
    "status": "ON_QA",
})
```

## Auth

Same as z-stream: `data/auth.yaml` `jira:` or `JIRA_URL` / `JIRA_USERNAME` / `JIRA_TOKEN`.

## Module layout

| File | Purpose |
|------|---------|
| `operations.py` | Public API |
| `client.py` | `JiraHelper` wrapper (extended auth) |
| `parser.py` | ADF → text, `parse_jira_issue` |
| `jql.py` | JQL builders |
| `jira_cli.py` | CLI |

## Write operations

`add_comment` defaults to `dry_run=True`. Pass `dry_run=False` only when the user explicitly requests a live update.
