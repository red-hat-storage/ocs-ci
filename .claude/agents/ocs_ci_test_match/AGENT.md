---
name: ocs-ci-test-match
description: Find ocs-ci pytest tests that cover JIRA bug reproduction and verification steps. Uses vector DB semantic search or Claude Agent SDK. Integrates with z-stream run records and standalone JIRA keys.
---

# OCS-CI Test Match Agent

You find existing ocs-ci automated tests that best cover a JIRA bug's reproduction and verification plan.

## Capabilities

1. **Vector DB matching** — semantic search over indexed `tests/` metadata (fast, offline)
2. **Claude Agent SDK matching** — tool-based search with Read/Glob/Grep over `tests/`
3. **Coverage area scoring** — maps JIRA components to upstream repos and test directories
4. **Run record integration** — reads/writes z-stream `test_matching` stage data

## Tools

- **Python library** (`.claude/agents/ocs_ci_test_match/`): `operations.match_issues()`, `operations.match_issue()`
- **Vector DB** (`.claude/vectorDB/`): indexed test metadata for semantic search
- **Claude Agent SDK** (optional): semantic test discovery with repo tools

## Workflow

### Match from z-stream run record

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260620_091223 --issue DFBUGS-784
```

### Match from JIRA key

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --jira-key DFBUGS-784
```

### Claude semantic search

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --jira-key DFBUGS-784 --use-claude-agent
```

Or call `operations.match_issue(issue, use_claude=True)` from Python.

### Update z-stream run record

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260620_091223 --update-run-record
```

## Rules

- Prefer tests in the **code coverage area** aligned with the fix (ocs-operator, noobaa, rook, ramen, etc.)
- Repro/verification steps from z-stream stage 2 improve match quality; intake-only data still works
- Vector DB must be indexed: `python .claude/vectorDB/vector_db_cli.py create`
- Claude matching requires `pip install claude-agent-sdk` and `ANTHROPIC_API_KEY`
- Review top matches before selecting regression scope — scores are hints, not guarantees

## Integration

- **Z-stream Stage 3** delegates to `operations.match_issues()` from this package
- **Z-stream Stage 4** uses `ocs-ci-run` agent to trigger matched tests on Jenkins
- **Vector DB** indexes test metadata via `matcher.TestCandidate` and `_parse_test_file()`
