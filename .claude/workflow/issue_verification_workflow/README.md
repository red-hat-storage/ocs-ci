# Issue Verification Workflow

Automates ODF z-stream qualification intake for bugs in **ON_QA** status: JIRA fetch → reproduction/verification steps → optional live cluster repro → ocs-ci test matching → optional Jenkins execution.

Run all commands from the **ocs-ci repository root**.

## Pipeline

```text
Stage 1: jira_intake                 → Fetch bugs (ON_QA JQL or explicit issue list)
Stage 2: repro_steps                 → Claude + JIRA context (repro/verification steps)
Stage 3: live_cluster_verification   → Live issue repro on cluster (optional; needs deploy_job_url)
Stage 4: test_matching               → Claude agent finds ocs-ci tests (skips failed live repro)
Stage 5: ocs_ci_execution            → Trigger matched tests on Jenkins (needs deploy_job_url)
```

Each stage appends results to a timestamped **run record** under `run_record/`. Stages 2–5 require `--run-id` from stage 1.

### Shared workflow config (recommended)

One YAML file drives the pipeline and standalone agent CLIs:

```bash
cp .claude/workflow/issue_verification_workflow/config/workflow.example.yaml \
   .claude/workflow/issue_verification_workflow/config/workflow.yaml
# Edit odf_version, deploy_job_url, agent settings

python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification

python .claude/agents/ocs_ci_test_match/test_match_cli.py match --run-id 20260622_194551
python .claude/agents/ocs_ci_live_repro/verify_cli.py plan --run-id 20260622_194551
```

Keep **secrets** in `data/auth.yaml` (`jira:` and `jenkins:` sections). The workflow config only references paths and run parameters.

| Section | Purpose |
|---------|---------|
| `parameters` | `odf_version`, `deploy_job_url`, `issues` — passed to all workflow stages |
| `defaults` | Pipeline defaults (`dry_run`, `top_n`, `live_repro_dry_run`, …) |
| `agents.*` | Per-agent settings for workflow stages and standalone CLIs |
| `auth` | Optional paths to credential files (not secrets themselves) |
| `run` | `run_id`, `from_stage`, `until_stage` for resume/slice runs |

#### Explicit issue list (skip JIRA search)

```yaml
parameters:
  odf_version: "4.22"
  issues:
    - DFBUGS-784
    - DFBUGS-1234
```

CLI alternative:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22 \
  --param issues=DFBUGS-784,DFBUGS-1234
```

Omit `issues` to use the default ON_QA search for the target ODF version.

### YAML pipeline orchestrator

Uses the generic **workflow_lib** engine (`.claude/workflow/workflow_lib/`) with issue verification executors and run record.

```bash
# Stages 1–4 (stage 5 skipped without deploy_job_url)
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22

# Full pipeline with cluster verification + Jenkins test execution
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22 \
  --param deploy_job_url=https://jenkins.../job/qe-deploy-ocs-cluster/69391/

python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --config ~/path/to/workflow.yaml
```

Stage 1 uses **`ocs_ci_jira`** agent (`jira_search`). Agent registry: `agents/registry.yaml`. Workflow: `pipelines/issue_verification.yaml`.

Resume from a specific stage:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22 \
  --run-id 20260622_194551 \
  --from-stage test_matching
```

Re-run live repro only:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param deploy_job_url=https://jenkins.../69391/ \
  --run-id 20260622_194551 \
  --from-stage live_cluster_verification
```

## Prerequisites

### Python dependencies

```bash
pip install atlassian-python-api
pip install -r .claude/workflow/issue_verification_workflow/requirements-pipeline.txt
pip install -r .claude/workflow/issue_verification_workflow/requirements-repro-agent.txt
```

Optional SDK fallback (when Claude Code CLI is unavailable):

```bash
pip install -r .claude/agents/ocs_ci_test_match/requirements-agent.txt
pip install -r .claude/agents/ocs_ci_live_repro/requirements-agent.txt
```

### Claude (required for stages 2–4)

Stages 2 (`repro_steps`), 3 (`live_cluster_verification`), and 4 (`test_matching`) use **Claude Code CLI** (`claude -p`) by default.

Install [Claude Code](https://code.claude.com/) and authenticate:

| Provider | Setup |
|----------|--------|
| Anthropic | `claude login` |
| Google Vertex | Claude Code settings → Vertex (third-party provider) |
| AWS Bedrock | Claude Code settings → Bedrock |

```bash
claude --version
claude auth status   # expect loggedIn: true
```

No `ANTHROPIC_API_KEY` is needed when using Claude Code CLI (including Vertex). Set `test_match_backend: claude-sdk` or `repro_steps_backend: sdk` only if you prefer the Agent SDK.

**Claude sessions:** one session per JIRA issue across stages 2–4 (`claude_session_id` on the run record). Re-running a single stage resumes that issue's session when the id is already stored.

There is no public Atlassian Rovo API; Claude analyzes JIRA description + comments and **linked fix pull requests** to produce reproduction/verification steps.

### JIRA credentials

Resolution order for `JiraHelper` (`allow_extended_sources=True`):

1. `config.AUTH.jira` (pytest/ocsci config)
2. `--jira-config` path
3. `/etc/jira.cfg`
4. `data/auth.yaml` (`jira:` section)
5. `JIRA_URL`, `JIRA_USERNAME`/`JIRA_EMAIL`, `JIRA_TOKEN`/`JIRA_PASSWORD` env vars

Example `data/auth.yaml`:

```yaml
jira:
  url: https://redhat.atlassian.net
  email: you@redhat.com
  token: <api-token>
```

### Fix pull requests (stage 2)

When `include_fix_prs: true` (default), stage 2 discovers GitHub PRs from JIRA remote links and description/comments. PR metadata is fetched via `gh pr view` or GitHub API when available.

Disable with `agents.repro_steps.include_fix_prs: false`.

## Quick start

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22
```

Note the **run id** from stderr (e.g. `20260622_194551`).

Stage 1 JQL (when `issues` is not set):

```text
project = "Data Foundation Bugs" AND issuetype = Bug
AND "target version" = odf-4.22 AND status = ON_QA
```

### Pipeline parameters

| Parameter | Default | Stages | Description |
|-----------|---------|--------|-------------|
| `odf_version` | — | 1–5 | Target ODF z-stream version |
| `deploy_job_url` | — | 3, 5 | Jenkins deploy URL; enables live repro + test execution |
| `issues` | — | 1 | Explicit JIRA keys (skips ON_QA search) |
| `top_n` | 10 | 4 | Max matching tests per issue |
| `test_match_backend` | auto | 4 | `auto`, `claude-cli`, or `claude-sdk` |
| `repro_steps_backend` | auto | 2 | `auto`, `claude-cli`, or `sdk` |
| `live_repro_backend` | auto | 3 | `auto`, `claude-cli`, or `sdk` |
| `live_repro_dry_run` | true | 3 | Plan only; set `false` for live `oc` reproduction |
| `live_repro_model` | — | 3 | Claude model override (e.g. Vertex model id) |
| `repro_claude_model` | — | 2 | Claude model for repro steps |
| `skip_on_env_mismatch` | true | 3 | Skip when cluster env mismatches issue |
| `force_live_repro` | false | 3 | Run repro despite env mismatch |
| `dry_run` | true | 5 | Jenkins trigger dry-run |
| `tests_per_issue` | 1 | 5 | Tests to trigger per issue |
| `use_claude` | false | 4 | Legacy: forces `claude-sdk` when `true` |

### Manual verification gating

When `live_repro_dry_run: false` and live repro **fails** (`not_fixed`, `issue_reproduced: Yes`, `inconclusive`, or stage `failed`), the issue gets `qualification_status: manual_verification_failed` and is **skipped** in stages 4–5.

Re-run test matching:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --run-id 20260622_194551 \
  --from-stage test_matching
```

Force Claude Agent SDK for test matching:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --run-id 20260622_194551 \
  --from-stage test_matching \
  --param test_match_backend=claude-sdk
```

## Run record

```text
.claude/workflow/issue_verification_workflow/run_record/<run_id>_odf-<version>/
  <run_id>.log
  <run_id>_issues.json
```

Run outputs are gitignored; only `run_record/.gitkeep` is tracked.

### Issues JSON structure

```json
{
  "key": "DFBUGS-784",
  "claude_session_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "summary": "...",
  "components": ["noobaa"],
  "stages": {
    "jira_intake": { "status": "completed", "completed_at": "..." },
    "repro_steps": {
      "status": "completed",
      "data": {
        "topology": "standard_ipi",
        "reproduction_steps": ["..."],
        "verification_steps": ["..."],
        "expected_result": "...",
        "generator": "claude_code_cli"
      }
    },
    "live_cluster_verification": {
      "status": "completed",
      "data": {
        "verdict": "dry_run",
        "matcher": "dry_run",
        "dry_run": true
      }
    },
    "test_matching": {
      "status": "completed",
      "data": {
        "matcher": "claude_code_cli",
        "matching_test_count": 3,
        "matching_tests": [
          {
            "test_node_id": "tests/.../test_foo.py::test_bar",
            "relevance_score": 85,
            "match_reasons": ["covers verification step: ..."],
            "pytest_command": "pytest tests/.../test_foo.py::test_bar"
          }
        ]
      }
    }
  }
}
```

## Module layout

| File / package | Purpose |
|----------------|---------|
| `pipeline_cli.py` | CLI entry point → workflow engine |
| `executors.py` | Stage executors |
| `workflow_context.py` | RunContext + factory |
| `workflow_config.py` | Shared config loader |
| `config/workflow.example.yaml` | Template for `workflow.yaml` |
| `pipelines/issue_verification.yaml` | Pipeline definition |
| `run_record.py` | Timestamped runs, issues JSON |
| `repro_steps_generator.py` | Stage 2 orchestration |
| `claude_repro_generator.py` | Claude repro/verification generation |
| `.claude/agents/ocs_ci_jira/` | JIRA intake (`pr_context.py` for fix PRs) |
| `.claude/agents/ocs_ci_live_repro/` | Live cluster verification (stage 3) |
| `.claude/agents/ocs_ci_test_match/` | Test matching (stage 4) |
| `.claude/agents/ocs_ci_run/` | Jenkins test execution (stage 5) |

## Test matching (stage 4)

Claude agent — default `test_match_backend: auto`:

1. Reads **reproduction + verification steps** from stage 2 (plus topology, fix PRs).
2. Searches `tests/` with **Read / Glob / Grep / Bash**.
3. Returns ranked pytest node ids and `pytest_command` values.

No vector DB or coverage mapper. `test_match_backend: vector_db` is no longer supported.

See `.claude/agents/ocs_ci_test_match/README.md` for CLI and API details.

## Roadmap

- `z_stream` pytest marker for selected regression scope
- JIRA comment with live repro verdict (Phase C)
- Pytest generation and PR workflow
