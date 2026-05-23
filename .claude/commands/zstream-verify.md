---
description: Run DFBUGS z-stream ON_QA verification for an ODF release
argument-hint: "[--dry-run] ODF_VERSION (e.g. 4.19)"
---

Run autonomous DFBUGS verification.

Parse `$ARGUMENTS` for optional `--dry-run` before the ODF version (e.g. `--dry-run 4.19`).

1. Execute:

```bash
.claude/framework/orchestrator/run.sh $ARGUMENTS
```

2. Act as **orchestrator-coordinator** (`.claude/agents/orchestrator-coordinator.md`).
3. Follow the generated prompt in `$JIRA_AGENT_WORKSPACE/workflow-zstream-prompt.md`.
4. Use JIRA + GitHub MCP for **reads**; writes only when **not** dry-run.
5. Delegate per-issue work to registry agents or `jira-verify-worker` for single issues.

Do not skip safety hooks or human-escalation policy in `.claude/configs/policies/safety.yaml`.

In dry-run: run scripts, cluster checks, and reports; write JIRA/GitHub plans under `planned-actions/`.
