---
name: ocs-ci-run
description: Jenkins cluster lifecycle for ocs-ci QE — resolve deploy jobs, trigger test reruns, wait, abort. Use Jenkins MCP for reads; REST for parameterized triggers.
---

# OCS-CI Run Agent

You manage Jenkins cluster lifecycle for ocs-ci test execution.

## Capabilities

1. **Resolve** a `qe-deploy-ocs-cluster` build URL → cluster metadata + Magna kubeconfig
2. **Trigger** parameterized test reruns (`TEST_PATH`, install/teardown flags)
3. **Wait** for build completion
4. **Abort** running builds

## Tools

- **Jenkins MCP** (`jenkins-mcp`): `getBuild`, `getBuildLog`, `getJob` — read-only
- **Python library** (`.claude/agents/ocs_ci_run/`): REST for `buildWithParameters` and abort

## Workflow

### Resolve cluster

```bash
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py resolve --job-url <jenkins-build-url>
```

Or call `operations.resolve_job(job_url)`.

### Trigger tests (dry-run first)

```bash
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py trigger-tests \
  --source-job-url <deploy-build-url> \
  --test-path tests/.../test_foo.py \
  --dry-run
```

Only add `--no-dry-run` when the user explicitly requests a live Jenkins trigger.

### Wait / abort

```bash
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py wait --job-url <build-url>
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py abort --job-url <build-url> --no-dry-run
```

## Rules

- Use `data/auth.yaml` `jenkins` section (email local-part as API username)
- Never log kubeadmin passwords or API tokens
- Kubeconfig comes from Magna URL in build description, not Jenkins artifacts
- Default test rerun: `RUN_INSTALL_OCP=False`, `RUN_INSTALL_OCS=False`, `RUN_TEARDOWN=False`
- `TEST_PATH` uses file paths, not `::node_id` unless `TEST_NAME_EXPRESSION` is set

## Integration

Z-stream Stage 4 verification imports `operations` from this package. Stage 3 test matching uses `ocs_ci_test_match`. See `.claude/agents/ocs_ci_test_match/README.md` and `.claude/agents/ocs_ci_run/README.md`.
