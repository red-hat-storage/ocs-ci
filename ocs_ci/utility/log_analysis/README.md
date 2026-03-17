# AI-Powered Log Analysis for OCS-CI

## What This Module Does

OCS-CI runs hundreds of tests per run, dozens of runs per day. When tests fail, engineers spend significant time manually reading tracebacks, checking logs, searching Jira for known bugs, and figuring out whether a failure is a product bug, a test bug, an infrastructure issue, or just a flaky test.

This module automates that process. Given a test run (either a remote log URL or a local log directory), it:

1. **Parses** JUnit XML results and extracts all failures
2. **Matches** failures against known issue regex patterns (instant, no cost)
3. **Classifies** remaining failures using AI (Claude) into categories: `product_bug`, `test_bug`, `infra_issue`, `flaky_test`, or `unknown` — when must-gather data is available, the pipeline pre-resolves all paths and Claude investigates Ceph status, pod logs, and cluster state directly
4. **Searches Jira** for existing bugs that match each failure
5. **Tracks history** across runs to detect flaky tests and regressions
6. **Generates reports** in Markdown, JSON, or HTML -- HTML reports feature interactive collapsible failure cards, color-coded category badges, pass rate progress bars, and Chart.js graphs for trend analysis

The result is a structured report that tells you *why* each test failed, whether it's a known issue, and what to do about it -- in seconds rather than hours.

## Quick Start

The module is invoked via `python -m ocs_ci.utility.log_analysis.cli` from within your ocs-ci virtualenv.

### Analyze a single run

```bash
# Full AI analysis (uses Claude Code CLI, no API key needed)
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --model sonnet --no-jira \
  -o analysis_report.md

# Save as JSON
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --model sonnet --no-jira \
  -f json -o analysis_report.json

# Fast mode: regex-only, no AI, no cost
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --known-issues-only --no-jira

# Local log directory works too
python -m ocs_ci.utility.log_analysis.cli /tmp/ocs-ci-logs-1234567/
```

### Track history across runs

```bash
# Record each run to the history store (add --record-history to any analysis)
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j014vu6lvt33t1/j014vu6lvt33t1_20260105T024909/logs/" \
  --known-issues-only --no-jira --record-history

python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j015vu6lvt33t1/j015vu6lvt33t1_20260113T050552/logs/" \
  --known-issues-only --no-jira --record-history

python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --model sonnet --no-jira --record-history \
  -o analysis_report.md

# After recording multiple runs, view cross-run trends
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['-o', 'trends_report.md'])
"

# Trends as JSON (for dashboards)
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['-f', 'json', '-o', 'trends_report.json'])
"
```

### Use from Python

```python
from ocs_ci.utility.log_analysis import analyze_run

result = analyze_run(
    source="http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/",
    ai_backend="claude-code",  # or "anthropic" or "none"
    model="sonnet",
    no_jira=True,
)

for fa in result.failure_analyses:
    print(f"{fa.test_result.name}: {fa.category.value} ({fa.confidence:.0%})")
    print(f"  Root cause: {fa.root_cause_summary}")
```

## How It Works

### Architecture

```
                    analyze_run()
                         |
          +--------------+--------------+
          |              |              |
     [1] PARSE     [2] CLASSIFY    [3] ENRICH
          |              |              |
    JUnit XML       Known Issues    Jira Search
    Config YAML     Cache Lookup    History Annotation
    Test Logs       AI Backend      Report Generation
    Must-Gather
```

### The Analysis Pipeline

**Step 1: Parse.** The module fetches artifacts from the log directory (supports both HTTP URLs like `magna002.ceph.redhat.com` and local paths). It parses JUnit XML for test results, the run config YAML for environment metadata (platform, OCS version, etc.), and per-test log files for additional error context. Log excerpts are filtered to remove noise lines (YAML field names like `error_count:`, `warning_count:` that falsely match ERROR/WARNING patterns).

**Step 2: Classify.** Each failure goes through a three-tier classification pipeline:

1. **Regex matching** (instant, free) -- checks the traceback against a built-in set of known issue patterns (e.g., `DFBUGS-2781` for Prometheus mgr module crashes). If matched, the failure is classified as `known_issue` and AI is skipped.

2. **Cache lookup** (instant, free) -- failures are fingerprinted by exception type and normalized traceback hash. If an identical failure was classified recently (within 30 days by default), the cached result is reused.

3. **AI classification** (costs money, takes a few seconds) -- the traceback, test log excerpt, and infrastructure context are sent to Claude, which returns a structured classification with category, confidence score, root cause summary, evidence, and recommended action.

**Must-gather investigation.** When must-gather artifacts are available on the log server (under `failed_testcase_ocs_logs_*/`), the Claude Code backend switches to **agentic mode**. Before calling Claude, the pipeline pre-resolves all paths to eliminate directory navigation overhead:

1. **Pre-resolution** — The pipeline navigates the must-gather directory structure upfront (cluster ID, `quay-io-*` hash directory), so Claude receives direct paths to the data. If only a `tar.gz` archive is available, it's automatically downloaded and extracted to `~/.ocs-ci/must_gather_cache/`.
2. **Test log URLs** — The per-test log directory URL is computed from the test classname and passed directly.
3. **UI test detection** — For UI tests, the pipeline checks if DOM snapshots and screenshots exist (under `ui_logs_dir_*/`) and passes those URLs. For non-UI tests, this section is omitted entirely.

With all paths pre-resolved, Claude goes straight to the evidence:

1. Starts with **OCS must-gather** — checks Ceph status, health details, OSD tree, operator pod logs, and namespace resources
2. If it suspects an **infrastructure issue**, it also explores the **OCP must-gather** — node conditions, kubelet logs, etcd health, network diagnostics
3. For **UI tests** — examines DOM snapshots and screenshots for error messages and UI state

Evidence entries include both the AI's interpretation and the actual log line that supports it:
```
Ceph cluster has degraded OSDs — ceph_health_detail: 'HEALTH_WARN 1 osds down; Degraded data redundancy'
```

This produces significantly more accurate classifications because Claude can correlate the traceback with the actual cluster state at the time of failure. Agentic calls are slower (3-10 minutes) and cost more (~$0.50-$1.50 per failure) but provide evidence-based root cause analysis with quoted log lines.

If must-gather data is not available, classification falls back to the standard non-agentic mode using only the traceback and test log excerpts.

**Session recording.** Every agentic investigation is automatically saved to `~/.ocs-ci/recorded_sessions/`. The transcript includes Claude's full tool call chain — every `cat`, `curl`, `grep` command and its output — so you can review exactly what data Claude examined and how it reached its conclusion. Session files are linked from the report.

**Console summary.** At the end of classification, a summary line reports total elapsed time and cost:
```
Classification complete in 12.3min, $4.56: 8 AI calls, 2 cache hits, 3 known issues
```

**Step 3: Enrich.** Classified failures are enriched with:

- **Jira search results** -- JQL queries built from must-gather evidence error messages (exact phrase match) and product/component keywords from the root cause analysis, searching DFBUGS for matching bugs
- **Cross-run context** -- if history is enabled, each failure is annotated with flakiness rate and regression status from previous runs

### Classification Categories

| Category | What it means | Examples |
|----------|--------------|---------|
| `product_bug` | Bug in ODF/OCS/Ceph/NooBaa/Rook | Unexpected API errors, wrong component status, data corruption |
| `test_bug` | Bug in the test code itself | Wrong assertions, hardcoded values, stale UI locators |
| `infra_issue` | Environment/infrastructure problem | Network timeouts, node not ready, cloud API errors |
| `flaky_test` | Intermittent timing/race condition | Timeout waiting for resource state, poll window missed |
| `known_issue` | Matched a known regex pattern | Pre-cataloged bugs like DFBUGS-2781 |
| `unknown` | Could not determine | Insufficient information to classify |

### AI Backends

| Backend | Flag | API Key? | How it works |
|---------|------|----------|-------------|
| Claude Code CLI | `--ai-backend claude-code` (default) | No | Calls `claude -p` subprocess with `--json-schema` for structured output |
| Anthropic API | `--ai-backend anthropic` | Yes (`ANTHROPIC_API_KEY`) | Direct API calls via the Anthropic SDK |
| None | `--ai-backend none` or `--known-issues-only` | No | Regex matching only, no AI |

**Model choices** (via `--model`):
- **`sonnet`** (default) -- best balance of quality and cost. Recommended for production use.
- **`haiku`** -- cheaper and faster, good for high-volume CI where cost matters more than precision.
- **`opus`** -- highest quality but slowest. Use for deep investigation of critical failures.

### Cost Control

AI classification costs money. The module has several safeguards:

- **Known issues bypass AI** -- regex-matched failures skip AI entirely (free)
- **Signature deduplication** -- failures with identical tracebacks share one AI call. A run with 40 failures but only 10 unique tracebacks makes 10 AI calls, not 40.
- **Caching** -- results cached for 30 days (configurable via `--cache-ttl`). Re-analyzing the same run is free.
- **Budget cap** -- `--max-budget-usd 0.50` (default) limits spend per AI call
- **Failure limit** -- `--max-failures 30` (default) caps total AI calls per run

A typical run with ~30 failures and ~10 unique signatures costs well under $0.10 with the `sonnet` model.

## CLI Reference

### `analyze-logs`

Analyze a single test run.

```
python -m ocs_ci.utility.log_analysis.cli <source> [options]
```

**Arguments:**

| Flag | Default | Description |
|------|---------|-------------|
| `source` | (required) | URL or local path to log directory |
| `-f`, `--format` | `markdown` | Output format: `json`, `markdown`, `html` |
| `-o`, `--output` | stdout | Write report to file |
| `--ai-backend` | `claude-code` | AI backend: `claude-code`, `anthropic`, `none` |
| `--model` | `sonnet` | AI model: `sonnet`, `haiku`, `opus` |
| `--known-issues-only` | off | Regex-only mode, no AI, no cost |
| `--no-jira` | off | Skip Jira integration |
| `--jira-config` | none | Path to Jira INI config file (with `url` and `token` in `[DEFAULT]` section) |
| `--max-budget-usd` | `0.50` | Max spend per AI classification call |
| `--max-failures` | `30` | Max unique failure signatures to classify with AI |
| `--test` | all | Only analyze failures matching these test name substrings (e.g., `--test noobaa pvc_clone`) |
| `--limit` | all | Limit total number of failures to process (for debugging) |
| `--squad` | all | Only analyze failures from a specific squad (e.g., `brown_squad`, `green_squad`) |
| `--cache-dir` | `~/.ocs-ci/analysis_cache` | Cache directory |
| `--cache-ttl` | `720` | Cache time-to-live in hours (default: 30 days) |
| `--known-issues-file` | none | Path to YAML file with additional known issue patterns |
| `--sessions-dir` | `~/.ocs-ci/recorded_sessions` | Directory for recorded session transcripts |
| `--jslave` | off | Running on Jenkins slave: translate session paths to magna002 HTTP URLs |
| `--record-history` | off | Save results to history store for cross-run analysis |
| `--history-dir` | `~/.ocs-ci/analysis_history` | History store directory |
| `--save-prompts` | off | Save AI prompts to `~/.ocs-ci/prompts/<run_id>/` for debugging |
| `-v`, `--verbose` | off | Enable debug logging |

**Examples:**

```bash
# Quick triage -- regex only, no cost
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --known-issues-only --no-jira

# Full analysis with AI, save Markdown report
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --model sonnet --no-jira \
  -o analysis_report.md

# Full analysis with AI, save JSON report
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --model sonnet --no-jira \
  -f json -o analysis_report.json

# Analyze and record for trend tracking
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --model sonnet --no-jira --record-history \
  -o analysis_report.md

# Use cheaper model for bulk CI analysis
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --model haiku --no-jira

# Add custom known issue patterns
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --known-issues-file my_patterns.yaml --no-jira

# Use a custom Jira config file (for standalone CLI usage without framework config)
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --jira-config ~/jira.cfg --known-issues-only
```

The Jira config file is a simple INI file:

```ini
[DEFAULT]
url = https://redhat.atlassian.net
username = you@redhat.com
token = YOUR_JIRA_API_TOKEN
```

For Atlassian Cloud (`atlassian.net`), both `username` and `token` are required (Basic auth). For on-prem Jira, only `token` is needed (Bearer auth).

### `analyze-trends`

Analyze patterns across multiple recorded runs. Requires at least 2 runs recorded with `--record-history`.

```bash
python3 -c "from ocs_ci.utility.log_analysis.cli import trends_main; trends_main([OPTIONS])"
```

| Flag | Default | Description |
|------|---------|-------------|
| `--history-dir` | `~/.ocs-ci/analysis_history` | History store directory |
| `--max-runs` | `100` | Maximum runs to analyze |
| `--platform` | all | Filter by platform (e.g., `aws`, `baremetal`) |
| `--ocs-version` | all | Filter by OCS version (e.g., `4.21`) |
| `--flavour` | all | Filter by launch name substrings (e.g., `--flavour tier1`, `--flavour BAREMETAL tier1`) |
| `-f`, `--format` | `markdown` | Output format: `json`, `markdown`, `html` |
| `-o`, `--output` | stdout | Write report to file |
| `-v`, `--verbose` | off | Enable debug logging |

**Examples:**

```bash
# View trends across all recorded runs
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['-o', 'trends_report.md'])
"

# Filter to a specific platform and version
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['--platform', 'baremetal', '--ocs-version', '4.21', '-o', 'trends.md'])
"

# Filter by run flavour (e.g., only tier1 runs)
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['--flavour', 'tier1', '-o', 'trends_tier1.md'])
"

# Combine filters: tier1 runs on baremetal only
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['--flavour', 'tier1', 'BAREMETAL', '-o', 'trends_bm_tier1.md'])
"

# Export as JSON for dashboard consumption
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['-f', 'json', '-o', 'trends.json'])
"
```

**The trend report includes:**

- **Analyzed runs table** -- each run with flavour (launch name), platform, OCS version, pass rate, and links to Jenkins and logs
- **Pass rate trend** -- pass rate per run over time
- **Regressions** -- tests that recently started failing after previously passing, with the date they broke
- **Flaky tests** -- tests that intermittently pass and fail, with flakiness rate and recent result pattern (e.g., `P F P F P`)
- **Most failing tests** -- ranked by total failure count
- **Squad health** -- per-squad pass rate, failure count, and flaky test count
- **Category distribution** -- how failure categories trend over time

## CI/CD Integration

The module can run automatically after every test run as a pytest plugin.

### Enable in config

Add to your cluster config YAML (or it will use defaults from `default_config.yaml`):

```yaml
LOG_ANALYSIS:
  ci_post_hook_enabled: true    # Enable automatic post-run analysis
  ai_backend: "claude-code"     # or "anthropic" or "none"
  model: "sonnet"
  ci_report_format: "all"       # "json", "markdown", "html", "both", or "all"
```

When enabled, after `pytest` finishes (and there are failures), the hook will:

1. Find the JUnit XML in the log directory
2. Run `analyze_run()` with `record_history=True`
3. Save reports to the log directory: `ai_analysis_report.json`, `ai_analysis_report.md`, and `ai_analysis_report.html`
4. Record the run to history for cross-run trend tracking (including run flavour, run_id, and Jenkins URL)

The hook is fully non-fatal -- if analysis fails for any reason, the test run result is unaffected.

### Framework config reference

All settings under `LOG_ANALYSIS:` in the ocs-ci config:

| Key | Default | Description |
|-----|---------|-------------|
| `ai_backend` | `"claude-code"` | AI backend |
| `model` | `"sonnet"` | AI model |
| `max_budget_usd` | `0.50` | Max spend per AI call |
| `max_failures_to_analyze` | `30` | Max unique failures for AI |
| `skip_ai_for_known_issues` | `true` | Bypass AI for regex matches |
| `cache_enabled` | `true` | Enable analysis caching |
| `cache_dir` | `"~/.ocs-ci/analysis_cache"` | Cache directory |
| `jira_search_enabled` | `true` | Search Jira for matching bugs |
| `jira_projects` | `["DFBUGS"]` | Jira projects to search |
| `sessions_dir` | `"~/.ocs-ci/recorded_sessions"` | Recorded session transcripts directory |
| `sessions_url` | `""` | HTTP base URL for session links (auto-derived from `sessions_dir` on Jenkins) |
| `history_dir` | `"~/.ocs-ci/analysis_history"` | Run history directory |
| `ci_post_hook_enabled` | `false` | Enable CI post-session hook |
| `ci_report_format` | `"all"` | CI report format: `json`, `markdown`, `html`, `both`, or `all` |

## Adding Custom Known Issue Patterns

Create a YAML file with your patterns:

```yaml
# my_known_issues.yaml
known_issues:
  - issue: "DFBUGS-9999"
    pattern: "NullPointerException.*rgw_bucket_sync"
    description: "RGW bucket sync NPE"

  - issue: "OCSQE-1234"
    pattern: "test_my_feature.*AssertionError"
    description: "Known assertion bug in test_my_feature"
```

Use with:

```bash
analyze-logs http://magna002.../logs/ --known-issues-file my_known_issues.yaml
```

Patterns are Python regexes matched against the full traceback text (case-insensitive). When a pattern matches, the failure is classified as `known_issue` immediately, skipping AI.

## Module Structure

```
ocs_ci/utility/log_analysis/
|-- __init__.py                  # analyze_run() entry point
|-- cli.py                       # analyze-logs and analyze-trends commands
|-- models.py                    # TestResult, FailureAnalysis, RunAnalysis dataclasses
|-- cache.py                     # File-based analysis result caching
|-- exceptions.py                # LogAnalysisError, AIBackendError, etc.
|
|-- ai/                          # AI classification backends
|   |-- base.py                  # AIBackend abstract class + get_backend() factory
|   |-- claude_code_backend.py   # Claude Code CLI backend (no API key)
|   |-- anthropic_backend.py     # Anthropic API backend (needs API key)
|   +-- prompt_templates/
|       |-- classify_failure.j2          # Failure classification prompt
|       |-- classify_failure_agentic.j2  # Agentic prompt with must-gather investigation
|       +-- run_summary.j2              # Run summary prompt
|
|-- analysis/                    # Analysis engines
|   |-- failure_classifier.py    # Classification pipeline orchestrator
|   |-- known_issues.py          # Regex-based known issue matching
|   |-- history_store.py         # Cross-run history persistence (JSON files)
|   +-- pattern_detector.py      # Flakiness, regression, and trend detection
|
|-- parsers/                     # Input parsing
|   |-- artifact_fetcher.py      # Fetch logs from URLs or local paths
|   |-- junit_parser.py          # Parse JUnit XML test results
|   |-- config_parser.py         # Parse run config YAML for metadata
|   |-- test_log_parser.py       # Extract errors/warnings from test logs
|   +-- must_gather_parser.py    # Extract Ceph/OSD context from must-gather
|
|-- integrations/                # External integrations
|   |-- jira_search.py           # Search Jira for matching bugs
|   +-- ci_hook.py               # pytest plugin for CI post-session analysis
|
+-- reporting/                   # Report generation
    |-- report_builder.py        # JSON, Markdown, HTML report builder
    +-- templates/
        |-- analysis_report.md.j2     # Single-run Markdown report
        |-- analysis_report.html.j2   # Single-run HTML report (styled, interactive)
        |-- trends_report.md.j2       # Cross-run Markdown trend report
        |-- trends_report.html.j2     # Cross-run HTML trend report (with Chart.js graphs)
        +-- jira_comment.j2           # Jira comment format
```

## Python API

### `analyze_run()`

The main entry point. Runs the full pipeline and returns a `RunAnalysis` object.

```python
from ocs_ci.utility.log_analysis import analyze_run

result = analyze_run(
    source="http://magna002.../logs/",
    ai_backend="claude-code",      # "claude-code" | "anthropic" | "none"
    known_issues_only=False,       # True = regex only, no AI
    model="sonnet",                # "sonnet" | "haiku" | "opus"
    max_budget_usd=0.50,           # Max USD per AI call
    max_failures=30,               # Max unique signatures for AI
    cache_dir="~/.ocs-ci/analysis_cache",
    cache_enabled=True,
    no_jira=False,                 # True = skip Jira search
    jira_projects=["DFBUGS"],
    record_history=False,          # True = save to history store
    history_dir="~/.ocs-ci/analysis_history",
)

# result.total_tests, result.passed, result.failed, result.error, result.skipped
# result.summary -- AI-generated or fallback summary string
# result.failure_analyses -- list of FailureAnalysis objects
# result.run_metadata -- RunMetadata (platform, ocs_version, etc.)
```

### `FailureAnalysis` fields

```python
fa = result.failure_analyses[0]

fa.test_result.full_name     # "tests.functional.pv...::test_create_pvc"
fa.test_result.squad         # "Green"
fa.test_result.status        # TestStatus.FAILED
fa.test_result.traceback     # Full traceback string

fa.category                  # FailureCategory.PRODUCT_BUG
fa.confidence                # 0.85
fa.root_cause_summary        # "The RBD space reclaim job timed out because..."
fa.evidence                  # ["AI explanation — source: 'quoted log line'", ...]
fa.recommended_action        # "Check Ceph cluster health and OSD status"
fa.matched_known_issues      # ["DFBUGS-2781"] if regex-matched
fa.suggested_jira_issues     # [{"key": "DFBUGS-5678", "summary": "...", ...}]
fa.session_id                # "abc123..." Claude session ID (agentic mode)
fa.session_file              # "/home/user/.ocs-ci/recorded_sessions/run_session_test.txt"
```

### Report generation

```python
from ocs_ci.utility.log_analysis.reporting.report_builder import ReportBuilder

builder = ReportBuilder()

# Single-run reports
md_report = builder.build(result, fmt="markdown")
json_report = builder.build(result, fmt="json")
html_report = builder.build(result, fmt="html")

# Trend reports (from PatternDetector)
from ocs_ci.utility.log_analysis.analysis.history_store import RunHistoryStore
from ocs_ci.utility.log_analysis.analysis.pattern_detector import PatternDetector

store = RunHistoryStore()
history = store.get_history()
detector = PatternDetector(history)
trend = detector.build_trend_report()

trend_md = builder.build_trends_report(trend, fmt="markdown")
trend_json = builder.build_trends_report(trend, fmt="json")
```

### Cross-run analysis

```python
from ocs_ci.utility.log_analysis.analysis.history_store import RunHistoryStore
from ocs_ci.utility.log_analysis.analysis.pattern_detector import PatternDetector

store = RunHistoryStore(history_dir="~/.ocs-ci/analysis_history")

# Load history (optionally filtered)
history = store.get_history(max_runs=50, platform="aws", ocs_version="4.21")

# Filter by flavour (launch name substrings, all must match)
history = store.get_history(flavour=["tier1", "BAREMETAL"])

detector = PatternDetector(history)

# Find flaky tests
flaky = detector.detect_flaky_tests(min_runs=3, flakiness_threshold=0.1)
for t in flaky:
    print(f"{t.test_name}: {t.flakiness_rate:.0%} flaky, recent: {t.recent_results[-5:]}")

# Find regressions
regressions = detector.detect_regressions(min_consecutive_failures=2)
for r in regressions:
    print(f"{r.test_name}: failing since {r.first_failure_timestamp[:10]}, "
          f"{r.consecutive_failures} runs in a row")

# Full trend report
trend = detector.build_trend_report()
print(f"Period: {trend.period}, Runs: {trend.runs_analyzed}")
print(f"Flaky: {len(trend.top_flaky_tests)}, Regressions: {len(trend.regressions)}")
```

## End-to-End Demo Walkthrough

A step-by-step demo showing the full capabilities. Run from the ocs-ci virtualenv.

### Prep: clear old cache and history

```bash
rm -rf ~/.ocs-ci/analysis_cache ~/.ocs-ci/analysis_history
```

### Step 1: Regex-only baseline (instant, free)

This shows what you get without AI -- almost everything is `UNKNOWN`:

```bash
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --known-issues-only --no-jira \
  -o demo_regex_only.md
```

Open `demo_regex_only.md` -- you'll see ~40 failures, nearly all marked `UNKNOWN` with raw tracebacks. This is the status quo: you'd have to read every traceback manually.

### Step 2: Full AI analysis (the main event)

Now run with AI classification:

```bash
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j043vu6mlvt33t1/j043vu6mlvt33t1_20260122T042132/logs/" \
  --model sonnet --no-jira \
  --record-history \
  -o demo_ai_analysis.md
```

Open `demo_ai_analysis.md` -- each failure now has:
- **Category**: `product_bug`, `test_bug`, `infra_issue`, or `flaky_test`
- **Confidence score**: how sure the AI is (e.g., 85%)
- **Root cause summary**: plain-English explanation of why it failed
- **Evidence**: key log lines and observations supporting the classification
- **Recommended action**: what to do about it
- **AI-generated run summary** at the top of the report

### Step 3: Record more runs for cross-run analysis

Record two older runs (regex-only is fast, we just need the test outcomes for history):

```bash
python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j014vu6lvt33t1/j014vu6lvt33t1_20260105T024909/logs/" \
  --known-issues-only --no-jira --record-history

python -m ocs_ci.utility.log_analysis.cli \
  "http://magna002.ceph.redhat.com/ocsci-jenkins/openshift-clusters/j015vu6lvt33t1/j015vu6lvt33t1_20260113T050552/logs/" \
  --known-issues-only --no-jira --record-history
```

### Step 4: Cross-run trend analysis

With 3 runs recorded, generate the trend report:

```bash
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['-o', 'demo_trends.md'])
"
```

Open `demo_trends.md` -- you'll see:
- **Pass rate trend**: 81.2% -> 80.1% -> 80.1% across Jan 5, Jan 13, Jan 22
- **Regressions**: tests that started failing mid-January (e.g., `test_bucket_logs_integrity`, `test_bucket_notifications`)
- **Flaky tests**: 21 tests with intermittent pass/fail, showing patterns like `P F P F P`
- **Squad health**: per-squad breakdown (Red squad: 82 tests, 83.4% pass rate, 9 flaky)
- **Most failing tests**: ranked by total failures across all runs

### Step 5: JSON output for automation

```bash
python3 -c "
from ocs_ci.utility.log_analysis.cli import trends_main
trends_main(['-f', 'json', '-o', 'demo_trends.json'])
"
```

Structured JSON that can feed into dashboards, Slack bots, or CI gates.
