# Shared workspace memory

Persistent state for long-running DFBUGS verification workflows.

## State tracking

Issue state is now tracked via a JSON file at `$JIRA_AGENT_WORKSPACE/run-state.json`,
managed by `.claude/framework/lib/run_state.py`. No initialization step is needed --
the file is created on first write.

| Path | Purpose |
|------|---------|
| `run-state.json` (in workspace) | JSON issue execution state (resume/retry) |
| `issue-history/` (in workspace) | Per-issue JSON snapshots from past runs |

## API (`framework/lib/run_state.py`)

- `mark_issue(workspace, key, status=..., processed=..., confidence=..., notes=...)` -- upsert an issue entry
- `get_issue(workspace, key)` -- retrieve an issue entry or `None`
- `load_state(workspace)` / `save_state(state, workspace)` -- raw read/write

## Issue record fields

- `status`, `processed`, `confidence`, `notes`, `updated_at`
