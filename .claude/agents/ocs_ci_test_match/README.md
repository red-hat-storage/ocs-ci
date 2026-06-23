# OCS-CI Test Match Agent

Find ocs-ci pytest tests that cover JIRA bug reproduction and verification steps.

**Package:** `.claude/agents/ocs_ci_test_match/`

## When to use

- **Z-stream Stage 3**: match ON_QA bugs to regression tests
- **Ad-hoc triage**: given a JIRA key or issue JSON, find relevant tests
- **Claude Code**: invoke `operations.match_issue()` interactively

## Prerequisites

### Vector DB (default matcher)

```bash
python .claude/vectorDB/vector_db_cli.py create
```

### Claude Agent SDK (optional)

```bash
pip install -r .claude/agents/ocs_ci_test_match/requirements-agent.txt
```

### JIRA credentials (for `--jira-key`)

Same as z-stream agent: `data/auth.yaml` `jira:` section or `JIRA_*` env vars.

## Quick start

### From z-stream run record

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

### Claude semantic search

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --jira-key DFBUGS-784 --use-claude-agent
```

### Python API

```python
from operations import match_issue, load_issue_from_jira

issue = load_issue_from_jira("DFBUGS-784")
result = match_issue(issue, top_n=10, use_claude=False)
print(result["matching_tests"])
```

## Module layout

| File | Purpose |
|------|---------|
| `operations.py` | High-level API: `match_issues()`, `match_issue()` |
| `matcher.py` | Vector DB semantic matching + test file parsing |
| `claude_matcher.py` | Claude Agent SDK two-phase matching |
| `coverage_mapper.py` | JIRA component → upstream repo / test directory mapping |
| `models.py` | Constants and matcher identifiers |
| `test_match_cli.py` | CLI entry point |
| `prompts/` | Claude prompts for semantic test matching |

## Test matching signals

Tests are ranked by:

1. **Code coverage area overlap** — upstream repos and preferred `tests/` directories
2. Direct `@jira("DFBUGS-xxx")` links in test files
3. Semantic similarity to reproduction/verification steps (vector DB)
4. Topology and component directory hints
5. Docstring similarity

## Integration

```text
zstream Stage 1–2  →  ocs_ci_live_repro (Stage 3, optional)  →  ocs_ci_test_match (Stage 4)  →  ocs_ci_run (Stage 5)
```

Z-stream Stage 3 calls `operations.match_issues()` and writes results to the run record `test_matching` stage.
