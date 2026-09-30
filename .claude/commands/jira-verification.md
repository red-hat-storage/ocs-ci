---
description: Write ON_QA Jira verification reports for one ODF version
argument-hint: <version>
allowed-tools: Bash(python:*), Bash(OCS_AGENT_PROVIDER=claude *), Read
---

Run the ODF Jira verification agent for version $ARGUMENTS.

From the repository root, use Claude Code as the model. Do not use OpenAI.

```bash
OCS_AGENT_PROVIDER=claude .venv/bin/python -m ocs_ci.agents.runtime.run \
  --agent jira_verification \
  --message "$ARGUMENTS"
```

When the process exits, read `ocs_ci/agents/jira_verification/reports/$ARGUMENTS/index.yaml` and each report it lists. Summarize the saved issue keys. Do not change a cluster and do not run the `oc` commands stored in those reports.
