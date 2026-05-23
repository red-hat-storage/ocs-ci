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

For issue `{KEY}`, run the pipeline in order without re-dispatching subagents:

1. jira-analysis → 2. cluster-compat → 3. repro-extraction → 4. script-generation
5. verification-execution → 6. infra-diagnosis → 7. github-automation → 8. reporting

Follow each agent file under `.claude/agents/` for detailed steps.

## JIRA updates (live runs only)

After execution, apply transitions per `.claude/configs/policies/safety.yaml`:

- **Reproduced:** comment with evidence, label `FailedQA`, transition `Assigned`
- **Verified:** transition `VERIFIED`, attach summary paths
- **Need info:** label `Need Info`, transition `Assigned`

Respect `human_escalation.min_confidence` — do not transition if below threshold.

Write final `$JIRA_AGENT_WORKSPACE/outcomes/{KEY}.json` and call:

```bash
python3 -c "from pathlib import Path; import sys; sys.path.insert(0, str(Path('.claude/memory').resolve())); from state import upsert_issue, snapshot_outcome; ..."
```

Skills: `.claude/skills/jira/SKILL.md`, `.claude/skills/ocs-ci-verify-script/SKILL.md`
