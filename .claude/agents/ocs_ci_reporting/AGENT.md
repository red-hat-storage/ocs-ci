# ocs_ci_reporting

Generic reporting agent for OCS-CI workflows. Renders Jinja2 templates from an
arbitrary context dict and delivers to file, Slack, or email.

## API

```python
from operations import build_and_deliver

delivery = build_and_deliver(
    context,  # workflow-specific dict
    template="issue_verification.md.j2",
    channels=[
        {"type": "file"},
        {"type": "slack"},
        {"type": "email", "to": ["team@example.com"]},
    ],
    dry_run=True,
    auth_path="data/auth.yaml",
)
```

## Channels

| Type | Config | Auth defaults |
|------|--------|---------------|
| `file` | `output_dir`, `filename` | — |
| `slack` | `webhook_url`, `channel` | `reporting.slack` in auth.yaml |
| `email` | `smtp_*`, `from`, `to` | `reporting.email` in auth.yaml |

## Templates

Bundled under `templates/`:

- `issue_verification.md.j2` — issue verification workflow report
- `plain.md.j2` — minimal generic report

Workflows may pass a custom template path.

## CLI

```bash
python .claude/agents/ocs_ci_reporting/report_cli.py send \
  --run-id 20260620_091223 \
  --workflow issue_verification \
  --channel file
```

Use `--no-dry-run` to actually send Slack/email (file is always written when not dry-run).

## Dependencies

```bash
pip install -r .claude/agents/ocs_ci_reporting/requirements-agent.txt
```
