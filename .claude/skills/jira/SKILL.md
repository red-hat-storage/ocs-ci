---
name: jira-dfbugs
description: JIRA MCP patterns for DFBUGS issue fetch, comments, labels, and transitions
---

# JIRA (DFBUGS)

## Discovery

Use `.claude/jira-repro/discovery/search_jql.py` for bulk ON_QA search (Cloud API).

## Single issue

- `jira_issue_get` — source of truth for fields
- `jira_comment_add` — verification evidence (**live only**; dry-run → `planned-actions/jira.json`)
- `jira_workflow_get_transitions` then `jira_workflow_transition` (**live only**)

Dry-run: read `.claude/skills/dry-run/SKILL.md`. `jira_issue_get` and search are always allowed.

## Policies

`.claude/configs/policies/safety.yaml`:

- `skip-ocsci-agent` → skip issue
- `Need Info` + Assigned when reproduction insufficient
- `FailedQA` + Assigned when bug still reproduces
- `VERIFIED` when fix confirmed

Never invent issue content.
