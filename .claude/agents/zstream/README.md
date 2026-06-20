# Z-Stream Lane C Issue Verification Agent

Automates ODF z-stream qualification intake for bugs in **ON_QA** status: JIRA fetch → reproduction/verification steps → ocs-ci test matching.

Run all commands from the **ocs-ci repository root**.

## Pipeline

```text
Stage 1: jira_intake      → Fetch ON_QA bugs for target ODF version
Stage 2: repro_steps      → Generate reproduction & verification steps
Stage 3: test_matching    → Find ocs-ci tests that cover the fix area
```

Each stage appends results to a timestamped **run record** under `run_record/`. Stages 2 and 3 require `--run-id` from stage 1.

## Prerequisites

### Python dependencies

Use the ocs-ci virtualenv with atlassian-python-api installed (standard ocs-ci deps):

```bash
pip install atlassian-python-api
```

For semantic test matching with Claude Agent SDK (optional):

```bash
pip install -r .claude/agents/zstream/requirements-agent.txt
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

### Stage 1 — JIRA intake

```bash
python .claude/agents/zstream/zstream_issue_verification.py \
  --odf-version 4.22 \
  --list-jira
```

Print the JQL without querying JIRA:

```bash
python .claude/agents/zstream/zstream_issue_verification.py \
  --odf-version 4.22 \
  --print-jql
```

JQL template:

```text
project = "Data Foundation Bugs" AND issuetype = Bug
AND "target version" = odf-4.22 AND status = ON_QA
```

Note the **run id** from stderr (e.g. `20260614_232133`).

### Stage 2 — Reproduction steps

```bash
python .claude/agents/zstream/zstream_issue_verification.py \
  --odf-version 4.22 \
  --run-id 20260614_232133 \
  --generate-repro-steps
```

Use `--no-jira-refresh` to skip re-fetching issues from JIRA and work from the run record only.

### Stage 3 — Test matching

**Heuristic** (fast, offline — keyword, topology, and code coverage area scoring):

```bash
python .claude/agents/zstream/zstream_issue_verification.py \
  --odf-version 4.22 \
  --run-id 20260614_232133 \
  --find-matching-tests
```

**Claude Agent SDK** (semantic search with Read/Glob/Grep over `tests/`):

```bash
python .claude/agents/zstream/zstream_issue_verification.py \
  --odf-version 4.22 \
  --run-id 20260614_232133 \
  --find-matching-tests \
  --use-claude-agent
```

Optional flags:

| Flag | Description |
|------|-------------|
| `--top-tests N` | Max matches per issue (default: 10) |
| `--claude-model MODEL` | Claude model for `--use-claude-agent` |
| `--jira-config PATH` | Explicit JIRA INI config file |
| `--output FORMAT` | `keys`, `raw`, `details`, `repro-steps`, `matching-tests` |

## Run record

Each run creates a directory:

```text
.claude/agents/zstream/run_record/<run_id>_odf-<version>/
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
| `zstream_issue_verification.py` | CLI orchestrator |
| `agent_helper.py` | JIRA JQL, fetch, parse ON_QA bugs |
| `run_record.py` | Timestamped runs, shared issues JSON |
| `repro_steps_generator.py` | Stage 2: reproduction/verification steps |
| `topology_mapper.py` | Heuristic fix → topology mapping |
| `coverage_mapper.py` | Upstream component → test directory / coverage areas |
| `test_matcher.py` | Stage 3: vector DB test matching |
| `claude_test_matcher.py` | Stage 3: Claude Agent SDK test matching |
| `prompts/` | Claude prompts for semantic test matching |

## Test matching

Tests are ranked by:

1. **Code coverage area overlap** — maps JIRA components/keywords to upstream repos (noobaa, ocs-operator, rook, ramen, etc.) and preferred `tests/` directories
2. Direct `@jira("DFBUGS-xxx")` links in test files
3. Keyword overlap with reproduction/verification steps
4. Topology and component directory hints
5. Docstring similarity

The Claude matcher uses a two-phase flow: tool-based search (prose analysis), then structured JSON formatting. On failure it falls back to the vector DB matcher.

## Roadmap

- `z_stream` pytest marker for selected regression scope
- Live cluster verification (Phase A)
- Pytest generation and PR workflow (Phase B)
- Jenkins integration
