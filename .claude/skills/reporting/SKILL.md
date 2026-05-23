---
name: reporting-dfbugs
description: Report formats for DFBUGS z-stream verification runs
---

# Reporting

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
  "odf_version": "4.19",
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
