---
name: jira-dfbugs
description: JIRA MCP patterns for DFBUGS issue fetch, comments, labels, and transitions
---

# JIRA (DFBUGS)

## Discovery

Default: `https://redhat.atlassian.net` (see `configs/jira-discovery.yaml`).
Legacy: `https://issues.redhat.com` (API v2).

CLI ODF version maps to **Target Release** for JQL, e.g. `4.19` → `odf-4.19.z`:

```text
"Target Release" = odf-4.19.z AND status = ON_QA ORDER BY created DESC
```

```bash
export JIRA_URL=https://issues.redhat.com
export JIRA_EMAIL=you@redhat.com
export JIRA_API_TOKEN=your-pat
.claude/jira-repro/discovery/run.sh
```

Debug JQL / API errors:

```bash
python3 .claude/jira-repro/discovery/search_jql.py --odf-version "$ODF_VERSION" -v
python3 .claude/jira-repro/discovery/search_jql.py --print-jql --odf-version 4.20
```

JQL templates: `.claude/configs/jira-discovery.yaml`

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
