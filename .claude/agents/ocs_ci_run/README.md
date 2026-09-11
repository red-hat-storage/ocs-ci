# OCS-CI Run Agent

Shared Jenkins cluster lifecycle for ocs-ci QE: resolve deploy builds, trigger parameterized test reruns, wait, and abort.

**Package:** `.claude/agents/ocs_ci_run/`

## When to use

- **Z-stream Stage 4** (verification): resolve cluster from a deploy job URL, rerun matched tests
- **Claude Code** with Jenkins MCP: read builds and logs interactively; use this library for parameterized triggers
- **CLI / scripts**: REST fallback without MCP

## Auth

Credentials from `data/auth.yaml`:

```yaml
jenkins:
  email: you@redhat.com   # API username = local-part (you)
  token: <api-token>
```

Or set `JENKINS_USER` and `JENKINS_TOKEN`.

For internal Jenkins with corporate TLS, set `JENKINS_SSL_VERIFY=false`.

## Jenkins backends

| Operation | Jenkins MCP | REST (`rest_client.py`) |
|-----------|-------------|-------------------------|
| Read build / poll | `getBuild` | `GET .../api/json` |
| Console log | `getBuildLog` | optional |
| Trigger with `TEST_PATH` | not supported | `buildWithParameters` |
| Abort build | not supported | `POST .../stop` |
| Kubeconfig | — | Magna HTTP from build description |

Parameterized test reruns **always** use REST. MCP is read-only in this agent.

### Claude Code MCP config

```json
"jenkins-mcp": {
  "command": "podman",
  "args": ["run", "-i", "--rm", "-e", "JENKINS_URL", "-e", "JENKINS_TOKEN", "-e", "MCP_TRANSPORT", "quay.io/redhat-ai-tools/jenkins-mcp:latest"],
  "env": {
    "JENKINS_URL": "https://jenkins-csb-odf-qe-ocs4.dno.corp.redhat.com/",
    "JENKINS_TOKEN": "<sync with auth.yaml>",
    "MCP_TRANSPORT": "stdio"
  }
}
```

Register MCP in Python via `jenkins.client.set_mcp_caller(callable)` when embedding in Claude Code.

## CLI

From ocs-ci repo root:

```bash
# Resolve cluster from deploy build (downloads kubeconfig from Magna)
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py resolve \
  --job-url https://jenkins-csb-odf-qe-ocs4.dno.corp.redhat.com/job/qe-deploy-ocs-cluster/69391/

# Prepare test rerun parameters (dry-run default — does not trigger Jenkins)
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py trigger-tests \
  --source-job-url https://jenkins-csb-odf-qe-ocs4.dno.corp.redhat.com/job/qe-deploy-ocs-cluster/69391/ \
  --test-path tests/functional/pv/test_pv.py \
  --dry-run

# Actually trigger Jenkins (explicit opt-in)
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py trigger-tests \
  --source-job-url <url> --test-path <path> --no-dry-run

# Wait for build completion
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py wait --job-url <build-url>

# Abort running build
python .claude/agents/ocs_ci_run/ocs_ci_run_cli.py abort --job-url <build-url> --no-dry-run
```

## Python API

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(".claude/agents/ocs_ci_run").resolve()))

from operations import resolve_job, trigger_test_run, wait_for_job, abort_job

profile = resolve_job("https://.../qe-deploy-ocs-cluster/69391/")
result = trigger_test_run(
    "https://.../69391/",
    ["tests/functional/pv/test_pv.py"],
    dry_run=True,
)
```

## Test rerun defaults

When copying parameters from a source deploy build:

- `RUN_INSTALL_OCP=False`, `RUN_INSTALL_OCS=False`, `RUN_TEST=True`
- `RUN_TEARDOWN=False` (cluster kept for verification)
- `TEST_PATH` = space-separated file paths (node ids stripped to file path)

## Related docs

- Design: `docs/superpowers/specs/2026-06-20-ocs-ci-run-agent-design.md`
- Plan: `docs/superpowers/plans/2026-06-20-ocs-ci-run-agent.md`
- Z-stream Stage 4: `docs/superpowers/specs/2026-06-20-zstream-verification-stage-design.md`
