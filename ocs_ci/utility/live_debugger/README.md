# Live Cluster Debugger

When a test fails during execution, the live debugger spawns a Claude Code session that autonomously investigates the **live cluster** to determine the root cause. It reads the test source code and logs, runs read-only `oc` commands, and classifies the failure.

## Quick Start

```bash
# Basic usage — investigate failures with default settings
run-ci ... --live-debug

# With must-gather (runs in parallel)
run-ci ... --live-debug --collect-logs

# Custom model, budget, and timeout
run-ci ... --live-debug --live-debug-model opus --live-debug-budget 2.00 --live-debug-timeout 600
```

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--live-debug` | `False` | Enable live cluster debugging on test failure |
| `--live-debug-model` | `sonnet` | Claude model to use (`sonnet`, `opus`, `haiku`) |
| `--live-debug-budget` | `1.00` | Max USD spend per investigation |
| `--live-debug-timeout` | `300` | Timeout in seconds per investigation |

## How It Works

```
Test fails in pytest
    │
    ├── [Thread 1] must-gather (existing, unchanged, if --collect-logs)
    │
    └── [Thread 2] Live Debugger
            │
            └── claude -p <prompt> --tools "Bash,Read" --output-format json
                    │
                    ├── Phase 1: Understand the test
                    │   ├── Read test source file (item.fspath)
                    │   ├── Read test log file (per-test log from pytest-logger)
                    │   └── Analyze the traceback
                    │
                    ├── Phase 2: Targeted cluster investigation
                    │   ├── oc get/describe resources
                    │   ├── oc logs <pod> --tail=100
                    │   ├── oc get events --sort-by='.lastTimestamp'
                    │   ├── oc exec <tools-pod> -- ceph status
                    │   └── ... (follows the evidence chain)
                    │
                    └── Phase 3: Classify and recommend
                        ├── product_bug / test_bug / infra_issue / known_issue
                        ├── Root cause + evidence
                        └── Code fix suggestion (if test_bug)
```

### Execution Flow

1. The debugger thread starts **before** must-gather, so they run in parallel.
2. Must-gather runs synchronously as before.
3. After must-gather and other post-failure processing, pytest **waits** for the debugger thread to complete before moving to the next test.
4. This means the debugger adds zero wall-clock time when it finishes before must-gather, and only the delta when it takes longer.

### Failure Phases

The debugger triggers on **all failure phases**: setup, call, and teardown. This is independent of `--collect-logs` — the debugger works standalone.

## Output

### Per-test output (in `ocsci_log_path()`)

- `{test_name}_live_debug.json` — structured investigation results
- `{test_name}_live_debug.html` — single-test HTML report

### Session-level output

- `live_debug_report.html` — aggregated report with:
  - Summary table (test name, category, root cause, cost)
  - Category breakdown (product_bug: N, test_bug: N, etc.)
  - Total cost and timing
  - Per-test expandable sections with full investigation narrative

## Module Structure

```
ocs_ci/utility/live_debugger/
├── __init__.py                          # Public API: LiveClusterDebugger
├── debugger.py                          # Core class — spawns claude -p
├── safety.py                            # Post-hoc command audit
├── report_builder.py                    # HTML report generation
├── prompt_templates/
│   └── investigate_failure.j2           # The investigation prompt
└── README.md                            # This file
```

### `debugger.py` — `LiveClusterDebugger`

The core class. Key method:

```python
debugger = LiveClusterDebugger(model="sonnet", max_budget_usd=1.00, timeout=300)
result = debugger.investigate(
    test_name="test_create_pvc",
    test_nodeid="tests/functional/pv/test_pvc.py::TestPVC::test_create_pvc",
    test_source_path="/path/to/test_pvc.py",
    traceback_text="AssertionError: ...",
    markers="green_squad,tier1",
    test_start_time="2025-01-15T10:30:00Z",
    failure_phase="call",           # setup, call, or teardown
    test_log_path="/path/to/log",   # optional
    log_dir="/path/to/output",      # optional, saves JSON+HTML
)
```

Returns a dict:
```python
{
    "test_name": "test_create_pvc",
    "test_nodeid": "tests/.../test_pvc.py::TestPVC::test_create_pvc",
    "failure_phase": "call",
    "investigation": "Full narrative...",
    "category": "product_bug",      # product_bug|test_bug|infra_issue|known_issue|unknown
    "root_cause": "OSD pod crashlooping due to OOM",
    "evidence": ["OSD pod restarted 5 times", "Memory limit 2Gi exceeded"],
    "recommended_action": "File Jira bug for OSD memory limits",
    "cost_usd": 0.0312,
    "num_turns": 8,
    "duration_seconds": 45.2,
    "error": null,
    "commands_executed": ["oc get pods -n openshift-storage", ...],
    "safety_violations": []
}
```

### `safety.py` — Command Audit

Defense-in-depth layer. After Claude finishes, all executed commands are checked against allow/deny lists:

- **Allowed**: `oc get`, `oc describe`, `oc logs`, `oc events`, `oc adm top`, `oc exec -- ceph status`
- **Forbidden**: `oc delete`, `oc apply`, `oc create`, `oc patch`, `oc edit`, `oc scale`, `oc debug`, `rm`, etc.

Violations are logged as warnings and included in the report.

### `report_builder.py` — HTML Reports

Generates styled HTML reports with:
- Color-coded category badges
- Expandable investigation narratives
- Code blocks for command output
- Safety violation warnings (if any)

## Integration Point

All integration is in `ocs_ci/framework/pytest_customization/ocscilib.py`:

- **`pytest_addoption`** — registers the `--live-debug*` CLI flags
- **`process_cluster_cli_params`** — stores params in `config.RUN["cli_params"]`
- **`pytest_runtest_makereport`** — launches debugger thread on failure, waits at end
- **`pytest_sessionfinish`** — generates the aggregated session report

## Environment

The debugger subprocess inherits:
- **`KUBECONFIG`** from `config.RUN["kubeconfig"]` (same as `exec_cmd` in `utils.py`)
- **`CLAUDECODE`** env var is removed to allow nested Claude Code sessions

## Cost Control

- Default budget: $1.00 per investigation
- Typical investigation: $0.02–0.10 (5–15 turns)
- Session total logged at end with per-test breakdown
- Budget is a hard cap enforced by Claude Code CLI
