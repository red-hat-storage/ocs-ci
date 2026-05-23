---
name: log-analysis
description: Analyze verification logs, events, and must-gather snippets for failure classification
---

# Log analysis

## Inputs

- `artifacts/{KEY}/logs/*.log`
- `execution.json`, pod logs, `oc get events`

## Patterns

| Signal | Likely class |
|--------|----------------|
| `connection refused` to API | infra / API |
| `No space left on device` | infra / disk |
| `MON_DOWN`, `OSD_DOWN` | product or infra (check timing) |
| Assertion in reproduce.py | product if matches JIRA symptom |
| Wrong image tag / CSV version | cluster_misconfig |

## Output

Structured bullets for JIRA comments and `diagnosis.json`.
