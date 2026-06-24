# Issue Verification Workflow

Automates ODF z-stream qualification intake for bugs in **ON_QA** status: JIRA fetch → reproduction/verification steps → ocs-ci test matching (via `ocs_ci_test_match` agent).

Run all commands from the **ocs-ci repository root**.

## Pipeline

```text
Stage 1: jira_intake                 → Fetch ON_QA bugs for target ODF version
Stage 2: repro_steps                 → Generate reproduction & verification steps
Stage 3: live_cluster_verification   → Live issue repro on cluster (optional, needs deploy_job_url)
Stage 4: test_matching               → Find ocs-ci tests (skips issues that failed live repro)
Stage 5: ocs_ci_execution            → Trigger matched tests on Jenkins (ocs_ci_run agent)
```

Each stage appends results to a timestamped **run record** under `run_record/`. Stages 2–4 require `--run-id` from stage 1.

### YAML pipeline orchestrator (recommended)

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

# Using a YAML run config
cp .claude/workflow/issue_verification_workflow/pipelines/configs/issue_verification.example.yaml \
   .claude/workflow/issue_verification_workflow/pipelines/configs/my-odf-4.22.yaml

python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --config .claude/workflow/issue_verification_workflow/pipelines/configs/my-odf-4.22.yaml
```

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
```

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
| `use_claude` | false | Use Claude Agent SDK for semantic search |
| `claude_model` | — | Model when `use_claude=true` |
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

Example with Claude matching:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --param odf_version=4.22 \
  --run-id 20260622_194551 \
  --from-stage test_matching \
  --param use_claude=true
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
        "issue_coverage_areas": {
          "coverage_areas": ["noobaa-mcg", "ocs-operator"],
          "upstream_repos": ["noobaa-core", "ocs-operator"],
          "preferred_test_dirs": ["tests/functional/object/mcg", "..."]
        },
        "matching_tests": [
          {
            "test_node_id": "tests/.../test_foo.py::test_bar",
            "coverage_areas": ["noobaa-mcg"],
            "relevance_score": 214,
            "match_reasons": ["code coverage area: NooBaa / MCG (S3)", "..."],
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
| `workflow_paths.py` | Paths to pipelines and agent registry |
| `pipelines/` | Workflow definitions (`issue_verification.yaml`) |
| `agents/registry.yaml` | Agent name → run-record stage mapping |
| `run_record.py` | Timestamped runs, shared issues JSON |
| `repro_steps_generator.py` | Stage 2: reproduction/verification steps |
| `topology_mapper.py` | Heuristic fix → topology mapping |

Live issue reproduction: `.claude/agents/ocs_ci_live_repro/`.

## Test matching

Tests are ranked by semantic similarity via the shared **vector DB** (`.claude/vectorDB/`), using reproduction/verification steps, issue summary, components, and coverage areas.

The Claude matcher uses a two-phase flow: tool-based search over `tests/`, then structured JSON formatting. On failure it falls back to the vector DB matcher.

## Roadmap

- `z_stream` pytest marker for selected regression scope
- Live cluster verification (Phase A)
- Pytest generation and PR workflow (Phase B)
- Jenkins integration
