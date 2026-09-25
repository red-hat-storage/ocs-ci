---
name: ocs-ci-live-repro
description: Plan or run live JIRA issue reproduction on an ODF cluster using repro steps and Jenkins cluster metadata.
---

# OCS-CI Live Repro Agent

Verify whether a JIRA bug can be reproduced (or is fixed) on a live OpenShift/ODF cluster using reproduction and verification steps from the z-stream run record.

## Capabilities

1. **resolve** — cluster metadata + kubeconfig from Jenkins `deploy_job_url` via `ocs_ci_run`
2. **assess_compatibility** — compare issue `environment_requirements` vs cluster profile
3. **plan** — dry-run reproduction plan (Phase A, no `oc` commands)
4. **live** — Claude Code runs repro steps on cluster (Phase B)

## Authentication (live mode)

**Default backend: `claude-cli`** — uses the Claude Code CLI (`claude -p`) with your existing subscription login.

```bash
claude login   # once; stores credentials in ~/.claude/
```

No `ANTHROPIC_API_KEY` is required for the default path.

Optional `--backend sdk` uses `claude-agent-sdk` (also picks up Claude Code credentials when logged in).

## Tools

- **Python library**: `operations.verify_issues()`
- **CLI**: `verify_cli.py plan` | `verify_cli.py live`

## Workflow

```bash
# Dry-run plan
python .claude/agents/ocs_ci_live_repro/verify_cli.py plan \
  --run-id 20260622_194551 \
  --deploy-job-url https://jenkins.../job/qe-deploy-ocs-cluster/69391/

# Live reproduction (Claude Code login)
python .claude/agents/ocs_ci_live_repro/verify_cli.py live \
  --run-id 20260622_194551 \
  --deploy-job-url https://jenkins.../69391/ \
  --issue DFBUGS-784 \
  --backend claude-cli \
  --write-run-record
```

## Rules

- **Phase A default**: `dry_run=True` — records a reproduction plan only
- **Phase B opt-in**: `dry_run=False` or `verify_cli.py live` — downloads kubeconfig and runs oc commands
- **Skip on mismatch**: issues are skipped when ODF version or topology does not match cluster
- Requires completed `repro_steps` stage in run record
- Live mode tracks and cleans up all resources created during reproduction

## Integration

- **Z-stream Stage 3** (`live_cluster_verification`) when `deploy_job_url` is set
- Set `live_repro_dry_run: false` in pipeline config for live mode
- Default `live_repro_backend: auto` prefers Claude Code CLI
