---
name: jira-verify-worker
description: Verify one DFBUGS issue end-to-end (analysis through GitHub) in a single subagent.
model: sonnet
tools:
  - Bash
  - Read
  - Write
  - Grep
  - Glob
---

You are the **single-issue verification worker**.

```bash
eval "$(.claude/framework/lib/load_run_context.sh)"
```

For issue `{KEY}`, run the pipeline in order without re-dispatching subagents:

1. jira-analysis → 2. cluster-compat → 3. repro-extraction → 4. script-generation
5. verification-execution → 6. cluster-health-detection → 7. infra-diagnosis
8. github-automation → 9. reporting

Follow each agent file under `.claude/agents/` for detailed steps.

## JIRA updates (live runs only)

Skip this section in **dry-run** (see `.claude/skills/dry-run/SKILL.md`). Instead write
`artifacts/{KEY}/planned-actions/jira.json` with intended comments, labels, transitions.

After execution (live only), apply transitions per `.claude/configs/policies/safety.yaml`:

- **Reproduced:** comment with evidence, label `FailedQA`, transition `Assigned`
- **Verified:** transition `VERIFIED`, attach summary paths
- **Need info:** label `Need Info`, transition `Assigned`

Respect `human_escalation.min_confidence` — do not transition if below threshold.

Write final `$JIRA_AGENT_WORKSPACE/outcomes/{KEY}.json` and call:

```bash
python3 -c "
import sys; sys.path.insert(0, '.claude/framework/lib')
from run_state import mark_issue
from pathlib import Path
ws = Path('.claude/workspace')
mark_issue(ws, '{KEY}', processed=True, status='verified', confidence=0.85)
"
```

Skills: `.claude/skills/jira/SKILL.md`, `.claude/skills/ocs-ci-verify-script/SKILL.md`,
`.claude/skills/cluster-health/SKILL.md`

Before JIRA VERIFIED transition, read `cluster-health-report.json` — block if
`regression_detected` or `cluster_health.status` is `CRITICAL`.
