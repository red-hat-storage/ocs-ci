---
description: Write ON_QA Jira verification reports for one ODF version
argument-hint: <version>
allowed-tools: Bash(python:*), Bash(OCS_AGENT_PROVIDER=claude *), Read
---

Run the ODF Jira verification agent. $ARGUMENTS may include a version, `--release`, `--issue`, `--cluster`, `--dry-run`, and `--arg KEY=VALUE`.

From the repository root, use Claude Code as the model. Do not use OpenAI. Pass those flags to the process. Do not hide them inside `--message`.

```bash
OCS_AGENT_PROVIDER=claude .venv/bin/python -m ocs_ci.agents.runtime.run \
  --agent jira_verification \
  --release "odf-5.0" \
  --issue DFBUGS-487 \
  --cluster amagrawa-c1
```

Use the flags from $ARGUMENTS. Omit `--message`, `--release`, `--issue`, `--cluster`, `--dry-run`, and `--arg` when the user did not supply them. When the user gives a release, pass it as `--release` so the search reads bugs for that Target Release. When the user asks for a dry run, pass `--dry-run`.

When the process exits, read `ocs_ci/agents/jira_verification/reports/$ARGUMENTS/index.yaml` and each report it lists. Summarize the saved issue keys. Do not change a cluster and do not run the `oc` commands stored in those reports. With `--dry-run`, do not update Jira or any other application.
