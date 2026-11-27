# OCS-CI Reporting Agent

Workflow-agnostic reporting: **context + template → rendered report → delivery channels**.

## Quick start

```bash
# From ocs-ci repo root — dry-run (default)
python .claude/agents/ocs_ci_reporting/report_cli.py send \
  --run-id 20260620_091223 \
  --workflow issue_verification

# Write report file + send Slack (requires auth.yaml)
python .claude/agents/ocs_ci_reporting/report_cli.py send \
  --run-id 20260620_091223 \
  --channel file --channel slack \
  --no-dry-run
```

## Issue verification report

The `issue_verification.md.j2` template includes:

**Run details:** ODF version, run ID, issue count, stages completed, deploy job, Jenkins status.

**Per-issue table:**

| Column | Description |
|--------|-------------|
| Issue ID: Title | JIRA key and summary |
| Repro steps | Step counts from repro stage |
| Repro status | Pass / Fail / Skipped / Pending |
| Live repro | Live cluster verification result |
| Test match | Matched pytest count and top test |
| Qualification | Overall qualification status |
| Observation | Errors, verdicts, analysis notes |

Context is built by `issue_verification_workflow/report_context.py`.

## Auth configuration

Add to `data/auth.yaml`:

```yaml
reporting:
  slack:
    webhook_url: https://hooks.slack.com/services/...
    channel: "#odf-qe-reports"
  email:
    smtp_host: smtp.example.com
    smtp_port: 587
    use_tls: true
    from: ocs-ci@example.com
    to:
      - team@example.com
    username: null
    password: null
```

Per-channel settings in workflow config override auth defaults.

## Workflow integration

The issue verification pipeline runs a `reporting` stage after all issue
processing stages. Configure in `workflow.yaml`:

```yaml
defaults:
  reporting_dry_run: true
  reporting_channels:
    - type: file
    - type: slack

agents:
  reporting:
    template: issue_verification.md.j2
    dry_run: true
    channels:
      - type: file
```

Re-run only reporting:

```bash
python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \
  --run-id 20260620_091223 \
  --from-stage reporting --force
```

## Custom workflows

1. Create a Jinja2 template (or use `plain.md.j2`).
2. Build a context dict from your run record.
3. Call `build_and_deliver()` from `operations.py`.

See `AGENT.md` for the Python API.
