# OCS-CI Test Match Agent

Find ocs-ci pytest tests that cover JIRA bug reproduction and verification steps.

**Package:** `.claude/agents/ocs_ci_test_match/`

## When to use

- **Issue verification Stage 4**: match ON_QA bugs to regression tests
- **Ad-hoc triage**: given a JIRA key or issue JSON, find relevant tests
- **Claude Code**: invoke `operations.match_issue()` interactively

## Prerequisites

### Claude Code CLI (default matcher)

```bash
claude login
```

### Claude Agent SDK (optional)

```bash
pip install -r .claude/agents/ocs_ci_test_match/requirements-agent.txt
```

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

### From JIRA key

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --jira-key DFBUGS-784 --top-tests 10
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

## Module layout

| File | Purpose |
|------|---------|
| `operations.py` | High-level API: `match_issues()`, `match_issue()` |
| `matcher.py` | Stage orchestration; routes to Claude CLI or SDK |
| `claude_cli_matcher.py` | Claude Code CLI two-phase matching |
| `claude_matcher.py` | Claude Agent SDK two-phase matching |
| `models.py` | Constants and matcher identifiers |
| `test_match_cli.py` | CLI entry point |
| `prompts/` | Claude prompts for semantic test matching |

## Test matching signals

Claude ranks tests by similarity to:

1. **Verification steps** from repro_steps stage (primary signal)
2. Reproduction steps and expected result
3. Issue summary, topology, and fix PR context
4. Direct `@jira("DFBUGS-xxx")` links when found during search

## Integration

```text
issue verification Stage 1–2  →  ocs_ci_live_repro (Stage 3, optional)  →  ocs_ci_test_match (Stage 4)  →  ocs_ci_run (Stage 5)
```

Stage 4 calls `operations.match_issues()` and writes results to the run record `test_matching` stage.
