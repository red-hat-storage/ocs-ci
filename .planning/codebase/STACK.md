# Technology Stack

**Analysis Date:** 2026-04-23

## Language & Runtime

**Primary Language:**
- Python 3.10-3.11 (specified in `pyproject.toml`: `requires-python = ">=3.10,<3.12"`)

**Runtime:**
- CPython standard interpreter

## Frameworks & Libraries

**AI/LLM Integration:**
| Package | Version | Purpose |
|---------|---------|---------|
| anthropic | Latest | Direct Anthropic API backend for Claude models |
| (none in base dependencies) | - | Claude Code CLI backend (subprocess-based) |

**HTTP & API Client:**
| Package | Version | Purpose |
|---------|---------|---------|
| requests | 2.32.2 | HTTP requests for artifact fetching and remote log access |
| urllib3 | 2.6.3 | Low-level HTTP client, indirect dependency of requests |
| bs4 | 0.0.1 | BeautifulSoup for parsing Apache/nginx directory listings |

**Jira Integration:**
| Package | Version | Purpose |
|---------|---------|---------|
| atlassian-python-api | 4.0.7 | Jira API client (via JiraHelper wrapper) |

**Testing & Reporting:**
| Package | Version | Purpose |
|---------|---------|---------|
| pytest | 6.2.5 | Test runner framework |
| pytest-html | 3.1.1 | HTML report generation |
| pytest-logger | 0.5.1 | Test logging plugin |
| pytest-metadata | 1.11.0 | Test metadata collection |
| pytest-order | 1.2.0 | Test ordering control |
| pytest-progress | 1.2.5 | Progress reporting |
| pytest-repeat | 0.9.3 | Test repetition plugin |
| pytest-jira | 0.3.22 | Jira integration with pytest |
| junitparser | 3.1.0 | JUnit XML parsing |

**Data Processing & Parsing:**
| Package | Version | Purpose |
|---------|---------|---------|
| pyyaml | 6.0.3 | YAML configuration and artifact parsing |
| jinja2 | 3.1.6 | Template engine for AI prompts |
| pandas | 1.5.2 | Data analysis and log processing |
| numpy | 1.23.2 | Numerical operations |
| jsonschema | >=3.2.0 | JSON schema validation for AI output |
| marshmallow | 3.26.2 | Object serialization/deserialization |
| markdown | 3.7 | Markdown report generation |

**Reporting:**
| Package | Version | Purpose |
|---------|---------|---------|
| prettytable | 0.7.2 | Formatted table display in reports |
| tabulate | 0.9.0 | Alternative table formatting |
| reportportal-client | 3.2.3 | ReportPortal integration for test reporting |

**Cloud & Infrastructure:**
| Package | Version | Purpose |
|---------|---------|---------|
| boto3 | 1.38.31 | AWS SDK (S3, EC2, etc.) |
| google-cloud-storage | 3.1.0 | Google Cloud Storage client |
| google-api-python-client | 2.171.0 | Google Workspace/Drive APIs |
| google-auth | 2.38.0 | Google authentication |
| azure-mgmt-compute | 33.0.0 | Azure compute resources |
| azure-mgmt-network | 28.0.0 | Azure networking |
| azure-mgmt-storage | 21.0.0 | Azure storage management |
| azure-storage-blob | 12.23.1 | Azure Blob Storage client |

**Utilities:**
| Package | Version | Purpose |
|---------|---------|---------|
| docopt | 0.6.2 | CLI argument parsing |
| python-dateutil | 2.9.0 | Date/time utilities |
| requests | 2.32.2 | HTTP requests |

## Build & Tooling

**Build System:**
- setuptools (via `build-system` in `pyproject.toml`)
- Python packaging standard: `[build-system]` declares `setuptools>=61.0`

**Package Manager:**
- uv (project uses `[tool.uv.sources]` for custom sources)
- Fallback: pip

**Lockfile:**
- Not present in repository (dependencies managed via `pyproject.toml`)

**Code Quality & Formatting:**
| Tool | Version | Purpose |
|------|---------|---------|
| black | 24.3.0 | Code formatter (in `pyproject.toml` dev dependencies) |
| pre-commit | 2.15.0 | Git hooks framework (in dev dependencies) |
| detect-secrets | (Git source) | Secret scanning (custom fork from `https://github.com/ibm/detect-secrets.git`) |

**Testing Runners:**
| Tool | Version | Purpose |
|------|---------|---------|
| tox | 3.25.1 | Test automation (in dev dependencies) |

**Documentation:**
| Tool | Version | Purpose |
|------|---------|---------|
| sphinx | 7.0.0 | Documentation generation (in docs group) |
| myst-parser | >=4.0.1 | Markdown support for Sphinx |
| sphinx-rtd-theme | 2.0.0 | ReadTheDocs Sphinx theme |

## Configuration

**Entry Points (CLI Commands):**
- `analyze-logs` → `ocs_ci.utility.log_analysis.cli:main`
- `analyze-trends` → `ocs_ci.utility.log_analysis.cli:trends_main`

Defined in `pyproject.toml` under `[project.scripts]`

**Core Configuration Files:**
- `pyproject.toml`: Python project metadata, dependencies, build config
- `requirements.txt`: Pinned dependencies for reproducible installs
- `requirements-dev.txt`: Development-only dependencies (black, pre-commit, tox, detect-secrets)
- `requirements-docs.txt`: Documentation dependencies (not read)
- `pytest.ini`: Pytest configuration
- `tox.ini`: Tox test automation config

**Environment Variables (Required):**
- `ANTHROPIC_API_KEY`: For `AnthropicBackend` AI model access (fallback to Claude Code CLI)
- Jira credentials: Configured via OCS-CI framework config or passed via CLI `--jira-config`

**Cache Configuration:**
- Default cache directory: `~/.ocs-ci/analysis_cache`
- Cache TTL: 720 hours (30 days) default

**Log Directory Configuration:**
- Sessions directory: `~/.ocs-ci/recorded_sessions`
- History directory: `~/.ocs-ci/analysis_history`
- Prompts directory: `~/.ocs-ci/prompts/{run_id}/` (when `--save-prompts` enabled)

**Custom Dependencies:**
- `ocp-network-split`: Git source `https://github.com/red-hat-storage/ocp-network-split.git` (in `[tool.uv.sources]`)
- `detect-secrets`: Git source `https://github.com/ibm/detect-secrets.git` with `rev = "master"` (custom fork)

## Platform Requirements

**Development:**
- Python 3.10 or 3.11
- pip, uv, or setuptools for package management
- git (for custom source dependencies)
- Optional: Claude Code CLI (for claude-code AI backend)
- Optional: ANTHROPIC_API_KEY environment variable (for anthropic backend)

**Production:**
- Python 3.10 or 3.11
- All dependencies from `requirements.txt`
- Jira credentials (optional, for Jira integration)
- ANTHROPIC_API_KEY (optional, for Anthropic API backend)
- Network access to:
  - Log servers (HTTP/HTTPS)
  - Jira instance (HTTP/HTTPS)
  - Anthropic API (for anthropic backend)
  - OpenShift/Kubernetes clusters (via openshift SDK)

**Deployment Target:**
- Linux-based systems (Docker-compatible)
- Can run as standalone CLI tool or pytest plugin
- Integrates with OCS-CI framework (larger test automation platform)

## Notable Patterns

- **AI Backend Abstraction**: Multiple AI backends (Claude Code CLI, Anthropic SDK, "none" for regex-only) all implement `AIBackend` protocol in `ocs_ci/utility/log_analysis/ai/base.py`
- **Plugin Architecture**: Pytest plugin via `ci_hook.py` registers conditionally in framework main
- **Lazy Initialization**: Jira integration and Anthropic SDK use lazy property pattern to avoid credential errors at import time
- **Template-Based Prompts**: AI backends use Jinja2 templates from `ocs_ci/utility/log_analysis/ai/prompt_templates/` for structured prompts
- **Caching Layer**: Analysis results cached with TTL to avoid redundant AI calls
- **Known Issues Matcher**: Regex-based pattern matching as fallback for AI
- **URL Translation**: Support for converting `/mnt/ocsci-jenkins/` paths to `http://magna002.ceph.redhat.com/ocsci-jenkins/` HTTP URLs

---

*Stack analysis: 2026-04-23*
