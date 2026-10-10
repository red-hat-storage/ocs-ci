# OCS-CI Test Match Agent

Find ocs-ci pytest tests that cover JIRA bug reproduction and verification steps.

**Package:** `.claude/agents/ocs_ci_test_match/`

## When to use

- **Issue verification Stage 4** (`test_matching`): match ON_QA bugs to regression tests
- **Ad-hoc triage**: given a JIRA key or issue JSON, find relevant tests
- **Claude Code**: invoke `operations.match_issue()` interactively

## Prerequisites

### Claude Code CLI (default — recommended)

Install [Claude Code](https://code.claude.com/) and authenticate once. Any supported auth provider works:

| Provider | Setup |
|----------|--------|
| Anthropic | `claude login` |
| Google Vertex | Claude Code settings → third-party / Vertex (no `ANTHROPIC_API_KEY`) |
| AWS Bedrock | Claude Code settings → Bedrock |

Verify:

```bash
claude --version
claude auth status   # loggedIn: true
```

Stages 2–4 of the issue verification workflow use `claude -p` subprocesses with **one Claude session per JIRA issue** (`--session-id` / `--resume`), so repro → live verify → test match share context without bleeding across issues.

### Claude Agent SDK (optional fallback)

Use when Claude Code CLI is unavailable:

```bash
pip install -r .claude/agents/ocs_ci_test_match/requirements-agent.txt
```

Force SDK via `--use-claude-agent` (CLI) or `test_match_backend: claude-sdk` (pipeline).

### JIRA credentials (for `--jira-key`)

Same as issue verification workflow: `data/auth.yaml` `jira:` section or `JIRA_*` env vars.

## Quick start

### From issue verification run record

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260620_091223
```

Single issue:

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260620_091223 --issue DFBUGS-784
```

Write results back to the run record:

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260620_091223 --update-run-record
```

### From JIRA key

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --jira-key DFBUGS-784 --top-tests 10
```

### Shared workflow config

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260620_091223 \
  --workflow-config .claude/workflow/issue_verification_workflow/config/workflow.yaml
```

### Claude Agent SDK

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --jira-key DFBUGS-784 --use-claude-agent
```

### Python API

```python
from operations import match_issue, load_issue_from_jira

issue = load_issue_from_jira("DFBUGS-784")
result = match_issue(issue, top_n=10)
print(result["matching_tests"])
```

## Backends

| `test_match_backend` | Behavior |
|----------------------|----------|
| `auto` (default) | Claude Code CLI when `claude` is on PATH; fails clearly if unavailable |
| `claude-cli` | Force Claude Code CLI (`claude -p`) |
| `claude-sdk` | Force `claude-agent-sdk` (needs `ANTHROPIC_API_KEY`) |
| `vector_db` | **Removed** — raises an error if set in old configs |

Set in `config/workflow.yaml`:

```yaml
defaults:
  test_match_backend: auto
agents:
  test_match:
    backend: auto
    top_n: 10
    model: null   # optional Vertex model id, e.g. claude-sonnet-4-5@20250929
```

## How matching works

Two-phase Claude agent (CLI or SDK):

1. **Search** — Claude reads reproduction + **verification steps** from the `repro_steps` stage, then searches `tests/` with Read / Glob / Grep / Bash.
2. **Format** — Claude converts the analysis into structured JSON (`matching_tests`, scores, `pytest_command`).

No heuristic coverage mapper. Verification steps are the primary signal.

### Ranking signals

1. Verification steps (primary)
2. Reproduction steps and expected result
3. Issue summary, topology, components, fix PR context
4. `@jira("DFBUGS-xxx")` links found during search

## Output

Run record field `matcher` is `claude_code_cli` or `claude_agent_sdk`.

```json
{
  "issue_id": "DFBUGS-784",
  "matcher": "claude_code_cli",
  "matching_test_count": 3,
  "matching_tests": [
    {
      "test_node_id": "tests/cross_functional/krkn_chaos/test_krkn.py::test_krkn_ceph_component_network_outage[mon-1pod]",
      "file_path": "tests/cross_functional/krkn_chaos/test_krkn.py",
      "test_name": "test_krkn_ceph_component_network_outage[mon-1pod]",
      "relevance_score": 92,
      "match_reasons": ["verification step: inject MON network outage", "..."],
      "pytest_command": "pytest tests/cross_functional/krkn_chaos/test_krkn.py::test_krkn_ceph_component_network_outage[mon-1pod]"
    }
  ],
  "analysis_notes": "Claude CLI matched tests from verification steps via repo search."
}
```

## Module layout

| File | Purpose |
|------|---------|
| `operations.py` | High-level API: `match_issues()`, `match_issue()` |
| `matcher.py` | Stage orchestration; routes to Claude CLI or SDK |
| `claude_cli_matcher.py` | Claude Code CLI two-phase matching |
| `claude_matcher.py` | Claude Agent SDK two-phase matching + shared prompts |
| `models.py` | Constants (`STAGE_TEST_MATCHING`, matcher identifiers) |
| `test_match_cli.py` | CLI entry point |
| `prompts/` | System/user/format prompts for test matching |

## Integration

```text
issue verification Stage 1–2  →  ocs_ci_live_repro (Stage 3, optional)  →  ocs_ci_test_match (Stage 4)  →  ocs_ci_run (Stage 5)
```

Pipeline re-run from test matching only:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --pipeline issue_verification \
  --run-id 20260620_091223 \
  --from-stage test_matching
```

Issues that failed live repro (`manual_verification_failed`) are skipped in stage 4.
