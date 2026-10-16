---
name: ocs-ci-test-match
description: Find ocs-ci pytest tests that cover JIRA bug reproduction and verification steps. Uses Claude Code CLI or Claude Agent SDK. Integrates with issue verification run records and standalone JIRA keys.
---

# OCS-CI Test Match Agent

You find existing ocs-ci automated tests that best cover a JIRA bug's reproduction and verification plan.

## Capabilities

1. **Claude Code CLI matching** (default) — tool-based search with Read/Glob/Grep over `tests/`
2. **Claude Agent SDK matching** — same approach via `claude-agent-sdk`
3. **Run record integration** — reads/writes issue verification `test_matching` stage data

## Tools

- **Python library** (`.claude/agents/ocs_ci_test_match/`): `operations.match_issues()`, `operations.match_issue()`
- **Claude Code CLI** (default): requires `claude login`
- **Claude Agent SDK** (optional): `pip install claude-agent-sdk` and `ANTHROPIC_API_KEY`

## Workflow

### Match from issue verification run record

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260620_091223 --issue DFBUGS-784
```

### Match from JIRA key

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --jira-key DFBUGS-784
```

### Claude Agent SDK

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --jira-key DFBUGS-784 --use-claude-agent
```

Or call `operations.match_issue(issue, use_claude=True)` from Python.

### Update run record

```bash
python .claude/agents/ocs_ci_test_match/test_match_cli.py match \
  --run-id 20260620_091223 --update-run-record
```

## Rules

- Matching is driven by **reproduction and verification steps** from stage 2 (repro_steps)
- Claude searches `tests/` with Read/Glob/Grep — no heuristic coverage mapper
- Claude Code CLI requires `claude login`; SDK requires `ANTHROPIC_API_KEY`
- Review top matches before selecting regression scope — scores are hints, not guarantees

## Integration

- **Issue verification Stage 4** delegates to `operations.match_issues()` from this package
- **Stage 5** uses `ocs-ci-run` agent to trigger matched tests on Jenkins
