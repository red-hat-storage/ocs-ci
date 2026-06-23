# Workflow orchestration

YAML-driven multi-stage pipelines for OCS-CI. This is **not** an agent — it is a generic engine that calls capability agents.

## Layout

```text
.claude/
  workflow/
    workflow_lib/           # generic orchestrator (engine)
      workflow_cli.py
      runner.py
      loader.py
      ...
    zstream_workflow/       # z-stream Lane C workflow family
      pipeline_cli.py       # recommended entry point
      executors.py
      pipelines/
      run_record/
  agents/                   # capability agents only
    ocs_ci_jira/
    ocs_ci_live_repro/
    ocs_ci_test_match/
    ocs_ci_run/
```

## Run z-stream verification

```bash
python .claude/workflow/zstream_workflow/pipeline_cli.py run \
  --pipeline zstream_verification \
  --param odf_version=4.22
```

## Generic CLI

For other workflow families, wire executors and context factory explicitly:

```bash
python .claude/workflow/workflow_lib/workflow_cli.py run \
  --workflows-dir .claude/workflow/zstream_workflow/pipelines \
  --registry .claude/workflow/zstream_workflow/agents/registry.yaml \
  --executors-module executors \
  --context-factory workflow_context:ZstreamContextFactory \
  --pipeline zstream_verification \
  --param odf_version=4.22
```

Add `.claude/workflow` to `PYTHONPATH` for `workflow_lib` imports, and `.claude/workflow/zstream_workflow` when using `--executors-module executors`.

Capability agents live under `.claude/agents/` — each has its own `AGENT.md`.
