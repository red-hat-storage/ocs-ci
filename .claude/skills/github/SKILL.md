---
name: github-ocs-ci-backlog
description: GitHub MCP patterns for automation backlog issues in ocs-ci
---

# GitHub (ocs-ci automation backlog)

## Repository

`red-hat-storage/ocs-ci` (see `safety.yaml` → `github.repo`)

## Dry-run

Never call issue-create APIs. Write `planned-actions/github-issue-draft.md` instead.
Duplicate **search** is allowed. See `.claude/skills/dry-run/SKILL.md`.

## Duplicate search

Before create, search issues for:

- JIRA key (`DFBUGS-NNNN`)
- Similar title keywords from verification summary

## Issue body template

```markdown
## JIRA
- Key: DFBUGS-XXXX
- Link: ...

## Summary
...

## Reproduction
...

## Suggested automation
- Script: artifacts/DFBUGS-XXXX/reproduce.py
- Cluster requirements: ...

## QE notes
...
```

## Labels

`automation backlog`, `QE`, `ODF`
