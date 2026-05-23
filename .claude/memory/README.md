# Shared workspace memory

Persistent state for long-running DFBUGS verification workflows.

| Path | Purpose |
|------|---------|
| `workflow_state.db` | SQLite issue execution state (resume/retry) |
| `issue-history/` | Per-issue JSON snapshots from past runs |
| `embeddings/` | Reserved for future vector search |
| `anomaly-signatures/` | Persisted error signatures from cluster-health runs |

Initialize before a workflow run:

```bash
python3 .claude/memory/init_state.py
```

Issue record schema (`issue_state` table):

- `issue_id`, `processed`, `status`, `retry_count`
- `cluster`, `github_issue`, `confidence`, `workflow_id`, `odf_version`
- `updated_at`, `notes`
