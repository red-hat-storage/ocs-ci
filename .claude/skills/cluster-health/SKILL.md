---
name: cluster-health-detection
description: Post-verification cluster health scans, product bug detection, and drift analysis
---

# Cluster health & product bug detection

## When to run

Immediately after `verification-execution`, before `infra-diagnosis` and JIRA result processing.

## Artifact layout

```
artifacts/{KEY}/cluster-health/
├── pre-snapshot.json          # optional, from cluster-compat
├── ceph-status.txt
├── nodes.txt
├── pods-unhealthy.txt
├── events.log
├── failing-pods.log
├── storagecluster.txt
├── anomaly-report.md          # agent-written summary
└── stacktraces/               # grouped error excerpts
```

## Collection script

```bash
.claude/jira-repro/cluster-health/collect.sh "$JIRA_AGENT_WORKSPACE/artifacts/{KEY}"
```

## Health score (0.0–1.0)

Start at 1.0, deduct:

- −0.3 per Critical finding
- −0.15 per Major
- −0.05 per Minor
- Infra/Noise: no deduction unless cluster-wide instability

`status`: HEALTHY (≥0.85), DEGRADED (0.5–0.84), CRITICAL (&lt;0.5)

## Error grouping

Group logs by normalized signature (strip timestamps, pod suffixes).
Flag recurring signatures (≥3 occurrences) as higher confidence.

## Correlation with verification

Read `execution.json` and `repro-steps.yaml` — note whether failures appeared
only in namespaces/resources touched by the verify script.

## Known signatures

`.claude/configs/signatures/known-issues.yaml` — map regex → `classification`, `dfbugs_key`, `noise`.

## Persist learning

Append new entries directly to `.claude/configs/signatures/known-issues.yaml`:

```yaml
  - regex: '<pattern>'
    classification: Major
    component: rook-ceph-mon
    dfbugs_key: null
```

## Must-gather

Only suggest/trigger when `safety.yaml` and coordinator approve. Path:
`cluster-health/must-gather/` (gitignored).
