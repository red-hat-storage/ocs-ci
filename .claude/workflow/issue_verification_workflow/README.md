# Issue Verification Workflow

Automates ODF z-stream qualification intake for bugs in **ON_QA** status: JIRA fetch → reproduction/verification steps → ocs-ci test matching (via `ocs_ci_test_match` agent).

Run all commands from the **ocs-ci repository root**.

## Pipeline

```text
Stage 1: jira_intake                 → Fetch bugs (ON_QA search or explicit issue list)
Stage 2: repro_steps                 → Claude + JIRA context (Rovo-equivalent repro/verification steps)
Stage 3: live_cluster_verification   → Live issue repro on cluster (optional, needs deploy_job_url)
Stage 4: test_matching               → Find ocs-ci tests (skips issues that failed live repro)
Stage 5: ocs_ci_execution            → Trigger matched tests on Jenkins (ocs_ci_run agent)
```

Each stage appends results to a timestamped **run record** under `run_record/`. Stages 2–4 require `--run-id` from stage 1.

### Shared workflow config (recommended)

One YAML file drives the pipeline and standalone agent CLIs:

```bash
cp .claude/workflow/issue_verification_workflow/config/workflow.example.yaml \
   .claude/workflow/issue_verification_workflow/config/workflow.yaml
# Edit odf_version, deploy_job_url, agent settings

# Pipeline auto-loads config/workflow.yaml when present
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification

# Agents read the same file
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

When `parameters.issues` (or `agents.jira_intake.issues`) is set, stage 1 fetches only those JIRA keys and skips the ON_QA JQL search:

```yaml
parameters:
  odf_version: "4.22"
  issues:
    - DFBUGS-784
    - DFBUGS-1234
```

CLI alternative (comma-separated):

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
# Full pipeline (Stages 1–4; Stage 5 skipped without deploy_job_url)
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22

# With cluster verification + Jenkins test execution
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22 \
  --param deploy_job_url=https://jenkins.../job/qe-deploy-ocs-cluster/69391/

# Explicit config path (overrides auto-detected workflow.yaml)
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --config .claude/workflow/issue_verification_workflow/config/workflow.yaml
```

Legacy per-run configs under `pipelines/configs/` still work via `--config`.

Stage 1 JIRA intake uses **`ocs_ci_jira`** agent (`jira_search`).

Agent registry: `agents/registry.yaml`. Workflow: `pipelines/issue_verification.yaml`.

Resume from a specific stage:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22 \
  --param deploy_job_url=https://jenkins.../69391/ \
  --run-id 20260622_194551 \
  --from-stage live_cluster_verification
```

Stage 3 standalone (via test-match agent):

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260622_194551
```

## Prerequisites

### Python dependencies

Use the ocs-ci virtualenv with atlassian-python-api installed (standard ocs-ci deps):

```bash
pip install atlassian-python-api
pip install -r .claude/workflow/issue_verification_workflow/requirements-pipeline.txt
pip install -r .claude/workflow/issue_verification_workflow/requirements-repro-agent.txt
```

Stage 2 **requires Claude** (`claude login` via Claude Code CLI, or `claude-agent-sdk`).
There is no public Atlassian Rovo API; Claude analyzes JIRA description + comments
and **linked fix pull requests** to produce Rovo-quality reproduction/verification steps.

For semantic test matching with Claude Agent SDK (optional):

```bash
pip install -r .claude/agents/ocs_ci_test_match/requirements-agent.txt
```

### JIRA credentials

The agent uses `JiraHelper` with **extended auth** (`allow_extended_sources=True`). Resolution order:

1. `config.AUTH.jira` (pytest/ocsci config)
2. `--jira-config` path (INI file with `url`, `username`/`email`, `password`/`token`)
3. `/etc/jira.cfg`
4. `data/auth.yaml` (`jira:` or `AUTH.jira:` section)
5. `JIRA_URL`, `JIRA_USERNAME`/`JIRA_EMAIL`, `JIRA_TOKEN`/`JIRA_PASSWORD` env vars

Example `data/auth.yaml`:

```yaml
jira:
  url: https://redhat.atlassian.net
  email: you@redhat.com
  token: <api-token>
```

**Note:** Existing ocs-ci code (`utils.py`, `rados_utils.py`) calls `JiraHelper()` without extended sources and keeps the legacy path: `AUTH.jira` → `/etc/jira.cfg` only.

### Fix pull requests (stage 2)

When `include_fix_prs: true` (default), stage 2 discovers GitHub PRs linked to each JIRA issue:

1. JIRA **remote issue links** (GitHub development panel / manual links)
2. GitHub PR URLs in the issue **description** and **comments**

PR title, body, and changed files are fetched via `gh pr view` or the GitHub API when
`gh` / `GITHUB_TOKEN` / `GH_TOKEN` is available (optional — URLs alone still help Claude).

Disable with `agents.repro_steps.include_fix_prs: false` or `defaults.include_fix_prs: false`.

## Quick start

Run the full pipeline (Stages 1–3; Stage 4 runs only when `deploy_job_url` is set):

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22
```

Note the **run id** from stderr (e.g. `20260622_194551`).

JQL used for Stage 1:

```text
project = "Data Foundation Bugs" AND issuetype = Bug
AND "target version" = odf-4.22 AND status = ON_QA
```

### Stage 3 options

Pass pipeline defaults via `--param` or run config YAML:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `top_n` | 10 | Max matching tests per issue |
| `test_match_backend` | auto | `auto` (Claude CLI when available), `claude-cli`, `claude-sdk` |
| `use_claude` | false | Legacy: forces full Claude Agent SDK search when `true` |
| `claude_model` | — | Claude model for test matching |
| `deploy_job_url` | — | Jenkins deploy URL for Stage 3 + Stage 5 |
| `live_repro_dry_run` | true | Stage 3 dry-run plan; set `false` for live oc reproduction |
| `live_repro_model` | — | Claude model for live reproduction |
| `oc_command_path` | oc | Path to `oc` binary for live reproduction |
| `live_repro_max_turns` | 40 | Max agent turns (sdk backend only) |
| `live_repro_backend` | auto | `claude-cli` (default when `claude` on PATH) or `sdk` |
| `skip_on_env_mismatch` | true | Skip issues when cluster env mismatches |
| `force_live_repro` | false | Run reproduction despite env mismatch |

### Manual verification gating

When `live_repro_dry_run: false` and live repro **fails** for an issue (`not_fixed`, `issue_reproduced: Yes`, `inconclusive`, or stage `failed`), that JIRA is marked `qualification_status: manual_verification_failed` and skipped in stages 4–5.

| `dry_run` | true | Stage 5 Jenkins trigger dry-run |

Example re-running test matching (default `test_match_backend: auto`):

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22 \
  --run-id 20260622_194551 \
  --from-stage test_matching
```

Full Claude Agent SDK search (reads `tests/` with tools; needs `claude-agent-sdk`):

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --run-id 20260622_194551 \
  --from-stage test_matching \
  --param test_match_backend=claude-sdk
```

## Run record

Each run creates a directory:

```text
.claude/workflow/issue_verification_workflow/run_record/<run_id>_odf-<version>/
  <run_id>.log
  <run_id>_issues.json
```

Run outputs are gitignored (see `.claude/.gitignore`); only `run_record/.gitkeep` is tracked.

### Issues JSON structure

Each issue accumulates stage data:

```json
{
  "key": "DFBUGS-784",
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
        "expected_result": "..."
      }
    },
    "test_matching": {
      "status": "completed",
      "data": {
        "matcher": "claude_code_cli",
        "matching_tests": [
          {
            "test_node_id": "tests/.../test_foo.py::test_bar",
            "relevance_score": 85,
            "match_reasons": ["covers MON quorum loss during chaos", "..."],
            "pytest_command": "pytest tests/.../test_foo.py::test_bar"
          }
        ]
      }
    }
  }
}
```

## Module layout

| File | Purpose |
|------|---------|
| `pipeline_cli.py` | CLI entry point → generic `workflow` engine |
| `executors.py` | Issue verification workflow stage executors |
| `workflow_context.py` | Issue verification RunContext + factory |
| `workflow_config.py` | Shared config loader for pipeline + agent CLIs |
| `config/workflow.example.yaml` | Template for `config/workflow.yaml` (gitignored) |
| `pipelines/` | Workflow definitions (`issue_verification.yaml`) |
| `agents/registry.yaml` | Agent name → run-record stage mapping |
| `run_record.py` | Timestamped runs, shared issues JSON |
| `repro_steps_generator.py` | Stage 2 orchestration (JIRA refresh + fix PRs + Claude) |
| `claude_repro_generator.py` | Mandatory Claude repro/verification step generation |
| `ocs_ci_jira/pr_context.py` | JIRA remote links + GitHub PR enrichment |
| `prompts/repro_steps_*.txt` | Claude prompts (Rovo-equivalent analysis) |
| `topology_mapper.py` | Heuristic fix → topology mapping |

Live issue reproduction: `.claude/agents/ocs_ci_live_repro/`.

## Test matching

Stage 4 is a **Claude agent** (default `test_match_backend: auto`):

1. Passes **reproduction + verification steps** from stage 2 (plus topology, env, fix PRs).
2. Claude searches `tests/` with **Read / Glob / Grep** and returns ranked pytest node ids.
3. No heuristic coverage mapper — matching is driven by verification-step similarity.

Requires `claude login` (Claude Code CLI). Run record field `matcher` is `claude_code_cli` or `claude_agent_sdk`.

## Roadmap

- `z_stream` pytest marker for selected regression scope
- Live cluster verification (Phase A)
- Pytest generation and PR workflow (Phase B)
- Jenkins integration
