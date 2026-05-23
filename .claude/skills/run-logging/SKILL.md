---
name: run-logging
description: Central run.log and live tail for DFBUGS verification workflows
---

# Run logging

## Central log file

```
$JIRA_AGENT_WORKSPACE/logs/run.log
```

## Write a line (agents and hooks)

```bash
.claude/framework/lib/log_run.sh INFO "jira-discovery: found 12 issues"
.claude/framework/lib/log_run.sh WARN "cluster-compat: ODF mismatch"
.claude/framework/lib/log_run.sh ERROR "verification-execution: pytest failed"
```

Levels: `INFO`, `WARN`, `ERROR`, `DEBUG`

## Progress dashboard

```bash
.claude/framework/orchestrator/watch.sh --status
# Shows: phase, discovery issue count, outcomes/artifacts counts
```

## Live tail (second terminal)

```bash
.claude/framework/orchestrator/watch.sh
.claude/framework/orchestrator/watch.sh --all    # include artifact *.log files
```

Discovery only: `.claude/framework/orchestrator/discover.sh`

## Per-issue detail logs

Still under `artifacts/DFBUGS-XXXX/logs/` (pytest) and `cluster-health/*.log`.

`watch.sh --all` follows those in addition to `run.log`.
