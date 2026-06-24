# OCS-CI Live Repro Agent

Runs live JIRA issue reproduction on an ODF cluster for z-stream qualification: compares issue environment requirements against a Jenkins deploy cluster, then plans or executes repro steps per issue.

**Phase A** (default): dry-run only — no `oc`/`kubectl` execution.

**Phase B**: Claude Code CLI (`claude -p`) or optional Agent SDK — runs reproduction steps on the live cluster.

## Prerequisites

- Completed z-stream `repro_steps` stage in run record
- Jenkins deploy build URL (`deploy_job_url`)
- Jenkins auth in `data/auth.yaml` (same as `ocs_ci_run`)
- **Live mode**: [Claude Code](https://code.claude.com/) installed and authenticated (`claude login`). No `ANTHROPIC_API_KEY` required when using the default `claude-cli` backend.
- Optional SDK fallback: `pip install -r .claude/agents/ocs_ci_live_repro/requirements-agent.txt`

## CLI

```bash
# Dry-run plan for all issues in a run
python .claude/agents/ocs_ci_live_repro/verify_cli.py plan \
  --run-id 20260622_194551 \
  --deploy-job-url https://jenkins-csb-odf-qe-ocs4.dno.corp.redhat.com/job/qe-deploy-ocs-cluster/69391/

# Single issue + write to run record
python .claude/agents/ocs_ci_live_repro/verify_cli.py plan \
  --run-id 20260622_194551 \
  --deploy-job-url https://jenkins.../69391/ \
  --issue DFBUGS-784 \
  --write-run-record

# Live reproduction on cluster (uses Claude Code login — no API key)
python .claude/agents/ocs_ci_live_repro/verify_cli.py live \
  --run-id 20260622_194551 \
  --deploy-job-url https://jenkins.../69391/ \
  --issue DFBUGS-784 \
  --backend claude-cli \
  --write-run-record
```

## Pipeline integration

Stage `live_cluster_verification` runs when `deploy_job_url` is set:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22 \
  --param deploy_job_url=https://jenkins.../69391/ \
  --run-id 20260622_194551 \
  --from-stage live_cluster_verification
```

## Skip logic

Per issue, live repro is **skipped** when:

| Condition | `skip_reason` |
|-----------|---------------|
| Missing `repro_steps` | `missing_repro_steps` |
| ODF version mismatch | `env_mismatch` |
| Topology mismatch (e.g. Regional DR on standard IPI) | `env_mismatch` |

Use `--force` (CLI) or `force_live_repro: true` (pipeline) to run anyway.

## Downstream gating

When live repro fails (`live_repro_dry_run: false`), the issue is marked `qualification_status: manual_verification_failed` and is **excluded** from `test_matching` and `ocs_ci_execution`. Dry-run plans do not block later stages.

## Output (`stages.live_cluster_verification.data`)

Dry-run:

```json
{
  "verdict": "dry_run",
  "matcher": "dry_run",
  "dry_run": true,
  "verification_plan": [...]
}
```

Live:

```json
{
  "verdict": "fixed",
  "matcher": "claude_code_cli",
  "backend": "claude-cli",
  "dry_run": false,
  "issue_reproduced": "No",
  "reproduction_steps_summary": [{"step": "...", "status": "Pass", "details": "..."}],
  "expected_results_validation": [...],
  "resources_created": ["namespace/ocs-verify-dfbugs-784"],
  "cleanup_status": {"all_deleted": true, "details": "..."},
  "output_log_path": "_ocs_ci_live_repro/DFBUGS-784/69391/.../verification.log",
  "conclusion": "..."
}
```

## Roadmap

- **Phase C**: JIRA comment with verdict
