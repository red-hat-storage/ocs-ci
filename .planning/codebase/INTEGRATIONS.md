# External Integrations

**Analysis Date:** 2026-04-23

## APIs & External Services

### Anthropic API (Claude Models)
- **Purpose:** AI-powered failure classification and run summaries
- **Implementation:** `ocs_ci/utility/log_analysis/ai/anthropic_backend.py`
- **SDK/Client:** `anthropic` Python SDK (v latest, optional dependency)
- **Auth:** `ANTHROPIC_API_KEY` environment variable
- **Models Supported:**
  - `claude-sonnet-4-20250514` (default, alias: "sonnet")
  - `claude-opus-4-20250514` (alias: "opus")
  - `claude-haiku-4-20250414` (alias: "haiku")
- **Features:**
  - Failure classification into categories (product_bug, test_bug, infra_issue, flaky_test, unknown)
  - Run summary generation across all failures
  - JSON schema output validation
  - Cost tracking and budget limiting (`--max-budget-usd` flag)
- **Fallback:** Can be disabled with `--ai-backend none` for regex-only analysis

### Claude Code CLI Backend
- **Purpose:** AI-powered analysis using Claude Code command-line tool
- **Implementation:** `ocs_ci/utility/log_analysis/ai/claude_code_backend.py`
- **Invocation:** Subprocess call to `claude -p` with `--output-format json` and `--json-schema`
- **Auth:** Uses Claude Code's existing authentication (no additional setup)
- **Default Backend:** `--ai-backend claude-code` (CLI default)
- **Features:**
  - Same classification and summary functionality as Anthropic backend
  - No ANTHROPIC_API_KEY required
  - Uses Claude Code's own authentication system
  - JSON schema validation of responses

### Log Artifact Fetching
- **Purpose:** Discover and fetch test logs from local or remote sources
- **Implementation:** `ocs_ci/utility/log_analysis/parsers/artifact_fetcher.py`
- **Protocol:** HTTP/HTTPS for remote, filesystem for local
- **Client:** Python `requests` library (v2.32.2) with BeautifulSoup (v0.0.1) for HTML parsing
- **Features:**
  - Parses Apache/nginx directory listing HTML for remote sources
  - Discovers JUnit XML, config YAML, test logs, UI logs, must-gather archives
  - Self-signed certificate support (urllib3.disable_warnings for InsecureRequestWarning)
  - Request timeout: 30 seconds

### ReportPortal Integration
- **Purpose:** Upload test execution reports to ReportPortal for tracking
- **Client:** `reportportal-client` v3.2.3
- **Configuration:** Integrated with OCS-CI framework config
- **Use:** Provides additional integration point for test reporting

## Jira Integration

### Jira API (Bug Tracking)
- **Purpose:** Search for existing bugs matching failure signatures, enrich analysis results
- **Implementation:** `ocs_ci/utility/log_analysis/integrations/jira_search.py`
- **Client:** `atlassian-python-api` v4.0.7 via OCS-CI's `JiraHelper` wrapper
- **Auth:** Jira credentials from OCS-CI config or via `--jira-config` INI file with:
  - `url`: Jira instance URL
  - `username`: Jira user
  - `password`: API token or password
- **Configuration Options:**
  - `--no-jira`: Skip Jira integration entirely
  - `--jira-config`: Path to INI file with credentials
- **Default Projects Searched:** `DFBUGS` (can be overridden with `--jira-projects`)
- **Features:**
  - JQL-based bug search matching failure root causes
  - Caches search results to avoid duplicate queries
  - Gracefully degrades if credentials unavailable (non-fatal)
  - Max results per query: 5 (configurable)
  - Categorizes failures as searchable: PRODUCT_BUG, UNKNOWN, INFRA_ISSUE, FLAKY_TEST

## Message Queues / Events

None detected. No Kafka, RabbitMQ, or event streaming systems are used.

## Databases

None detected. Log analysis module does not use traditional databases.

**Local Storage Used:**
- **Analysis Cache:** JSON files in `~/.ocs-ci/analysis_cache/` with TTL (default: 720 hours)
- **History Store:** JSON files in `~/.ocs-ci/analysis_history/` for cross-run pattern detection
- **Session Transcripts:** Optional recording in `~/.ocs-ci/recorded_sessions/`
- **Prompts:** Optional saving in `~/.ocs-ci/prompts/{run_id}/` (debug mode)
- **Bug Details:** Optional per-failure JSON in `--bug-details-dir` (default: disabled)

## Authentication & Identity

**Authentication Methods:**
1. **API Keys:**
   - `ANTHROPIC_API_KEY`: For Anthropic API backend (environment variable or AnthropicBackend constructor)

2. **Jira Credentials:**
   - Format: INI file with `[DEFAULT]` section containing `url`, `username`, `password`
   - Location: Passed via `--jira-config` CLI flag or configured in OCS-CI framework config
   - Env vars: Stored in `ocs_ci.framework.AUTH["jira"]` dictionary

3. **Claude Code CLI:**
   - Uses Claude Code's built-in authentication
   - No additional credentials required

## Monitoring & Observability

### Error Tracking
- No external error tracking service (Sentry, DataDog, etc.)
- Errors logged to stdout/stderr via Python logging module

### Logging
- **Framework:** Python standard `logging` module
- **Configuration:** Controlled by `--verbose` CLI flag (sets DEBUG level)
- **Loggers Suppressed:**
  - `atlassian` (Jira client): set to WARNING level
  - `urllib3` (HTTP client): set to WARNING level
  - `ocs_ci.utility.log_analysis`: inherits INFO level (or DEBUG if `--verbose`)
- **Format:** `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

### Debug Features
- **Prompt Saving:** `--save-prompts` saves AI prompts to `~/.ocs-ci/prompts/{run_id}/`
- **Session Recording:** `--record-history` stores run results for cross-run pattern analysis
- **Verbose Logging:** `--verbose` enables DEBUG-level output

## CI/CD & Deployment

### Jenkins Integration
- **Location:** `ocs_ci/utility/log_analysis/integrations/ci_hook.py`
- **Mechanism:** Pytest plugin registered conditionally in OCS-CI framework
- **Trigger:** `pytest_sessionfinish` hook after test suite completes
- **Features:**
  - Runs only on test runs with failures (skips deployment, skips all-pass)
  - Auto-detects log directory from `ocs_ci.framework.config.RUN.log_dir`
  - Generates reports (JSON, Markdown, HTML) in log directory
  - Converts local `/mnt/ocsci-jenkins/` paths to `http://magna002.ceph.redhat.com/` URLs for Jenkins slave environments
  - Configuration: Via `LOG_ANALYSIS` section in OCS-CI config
- **Configuration Options:**
  - `ci_post_hook_enabled`: Enable/disable the hook
  - `ai_backend`: "claude-code", "anthropic", or "none"
  - `model`: AI model alias
  - `max_budget_usd`: Max spend per analysis
  - `jira_search_enabled`: Enable Jira lookups
  - `ci_report_format`: "json", "markdown", "html", or "all"

### Hosting / Log Server
- **Log Server:** HTTP server (Apache/nginx style directory listing)
- **Default URL Pattern:** `http://magna002.ceph.redhat.com/ocsci-jenkins/`
- **Local Mount:** `/mnt/ocsci-jenkins/` (on Jenkins slaves)
- **Features:**
  - Standard HTTP GET for artifact discovery
  - Supports both HTTP and HTTPS sources
  - BeautifulSoup parsing of directory listings

## Environment Configuration

### Required Environment Variables
- `ANTHROPIC_API_KEY` (optional): For `--ai-backend anthropic` backend

### Configuration Files
- **Jira Config:** INI format, passed via `--jira-config` flag
  - Sections: `[DEFAULT]`
  - Keys: `url`, `username`, `password`

### CLI Flags for Configuration
- `--ai-backend`: "claude-code" (default), "anthropic", or "none"
- `--model`: AI model alias (default: "sonnet")
- `--cache-dir`: Analysis cache location (default: `~/.ocs-ci/analysis_cache`)
- `--cache-ttl`: Cache TTL in hours (default: 720)
- `--history-dir`: Run history location (default: `~/.ocs-ci/analysis_history`)
- `--sessions-dir`: Session transcripts location
- `--jira-config`: Path to Jira credentials INI file
- `--known-issues-file`: Path to YAML file with custom failure patterns
- `--jslave`: Convert local paths to magna002 HTTP URLs (Jenkins slave mode)

### OCS-CI Framework Config Integration
Module integrates with OCS-CI's `ocs_ci.framework.config` for:
- `LOG_ANALYSIS`: Log analysis configuration section
- `RUN.log_dir`: Test run log directory
- `AUTH["jira"]`: Jira authentication dictionary
- `REPORTING`: Test run metadata (versions, flavour, etc.)

## Webhooks & Callbacks

### Incoming Webhooks
None detected. Module does not listen for webhooks.

### Outgoing Webhooks
- **Jira API:** Queries via JQL (read-only, no write)
- **HTTP Log Fetching:** GET requests to log server
- **Anthropic API:** POST requests for classification and summary (streaming supported)

## Cross-Run Analysis & Trend Detection

### History Store
- **Purpose:** Record and analyze patterns across multiple test runs
- **Storage:** JSON files in `~/.ocs-ci/analysis_history/`
- **Functionality:**
  - `RunHistoryStore`: Stores run analysis results with TTL-based cleanup
  - `PatternDetector`: Analyzes failure patterns across runs
  - Generates cross-run trend reports (markdown, JSON, HTML)
- **Entry Point:** `analyze-trends` CLI command
- **Features:**
  - Filter by platform, OCS version, flavour
  - Identify recurring failures, new failures, flaky patterns
  - Annotate current run with historical context

## Testing & Quality Tools

### Test Parsing
- **JUnit XML:** `junitparser` v3.1.0
- **Must-Gather:** `must_gather_parser.py` extracts cluster diagnostics
- **Config YAML:** `config_parser.py` parses run configuration

### Test Metadata
- Extracts from JUnit XML suite properties (e.g., `rp_ocp_version`, `rp_ocs_build`, `rp_launch_name`)
- Enriches with ReportPortal metadata if available

---

*Integration audit: 2026-04-23*
