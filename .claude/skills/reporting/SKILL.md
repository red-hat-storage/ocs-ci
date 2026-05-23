---
name: reporting-dfbugs
description: Report formats for DFBUGS z-stream verification runs
---

# Reporting

## ODF version (do not hardcode)

```bash
eval "$(.claude/framework/lib/load_run_context.sh)"
```

Use `$ODF_VERSION` in `metrics.json`, `summary.md`, and all report headers.
See `.claude/skills/run-context/SKILL.md`.

## Directory layout

```
$JIRA_AGENT_WORKSPACE/reports/
├── summary.md
├── failures.md
├── skipped.md
├── verified.md
└── metrics.json
```

## metrics.json schema

```json
{
  "workflow_id": "zstream-issue-verification",
  "run_id": "<from active-run.json>",
  "odf_version": "<from active-run.json — CLI argument, varies per run>",
  "dry_run": false,
  "started_at": "",
  "finished_at": "",
  "counts": {
    "verified": 0,
    "failed": 0,
    "skipped": 0,
    "need_info": 0,
    "infra_blocked": 0
  },
  "issues": []
}
```

## summary.md

Executive summary for QE leads: scope, cluster, outcomes, automation backlog links.

## Cluster health section

Per issue, summarize from `cluster-health-report.json`:

- `cluster_health.status`, score, critical/warning counts
- `potential_bugs` with confidence ≥ 0.7
- Link to `artifacts/{KEY}/cluster-health/anomaly-report.md`
