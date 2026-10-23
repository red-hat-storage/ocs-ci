"""
Configuration for the z-stream test generator.

Configuration is loaded from environment variables and/or a config file
at ~/.zstream_test_gen.yaml.
"""

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)

CONFIG_FILE = Path.home() / ".zstream_test_gen.yaml"


@dataclass
class JiraConfig:
    """Jira connection configuration."""

    url: str = "https://redhat.atlassian.net"
    username: str = ""
    api_token: str = ""
    project_key: str = "DFBUGS"


@dataclass
class GitHubConfig:
    """GitHub API configuration."""

    token: str = ""
    # Upstream repos where fix PRs are merged
    upstream_repos: list = field(
        default_factory=lambda: [
            "ceph/ceph-csi",
            "red-hat-storage/ceph-csi",
            "noobaa/noobaa-operator",
            "red-hat-storage/noobaa-operator",
            "RamenDR/ramen",
            "red-hat-storage/ramen",
        ]
    )
    # ocs-ci repo for PR creation
    ocsci_repo: str = "red-hat-storage/ocs-ci"
    # Fork repo for pushing branches (PRs open from fork -> upstream)
    fork_repo: str = ""


@dataclass
class ClaudeConfig:
    """Claude AI configuration for test generation."""

    # Vertex AI settings
    project_id: str = ""
    location: str = "us-east5"
    model: str = "claude-opus-4-6"
    max_tokens: int = 8192
    temperature: float = 0.2
    # Direct Anthropic API (alternative to Vertex)
    api_key: str = ""
    use_vertex: bool = True


@dataclass
class RovoConfig:
    """Rovo MCP Server configuration."""

    mcp_url: str = "https://mcp.atlassian.com/v2/mcp"
    api_token: str = ""
    enabled: bool = False


@dataclass
class GeneratorConfig:
    """Overall generator configuration."""

    jira: JiraConfig = field(default_factory=JiraConfig)
    github: GitHubConfig = field(default_factory=GitHubConfig)
    claude: ClaudeConfig = field(default_factory=ClaudeConfig)
    rovo: RovoConfig = field(default_factory=RovoConfig)

    # ocs-ci repo root (auto-detected)
    ocsci_root: str = ""

    # Generation settings
    max_retries: int = 2
    dry_run: bool = False
    output_dir: str = ""

    # PR settings
    create_prs: bool = True
    draft_prs: bool = True
    backport: bool = True
    min_confidence: str = ""
    pr_labels: list = field(
        default_factory=lambda: [
            "zstream-verification",
            "ai-generated",
        ]
    )

    def __post_init__(self):
        if not self.ocsci_root:
            # Auto-detect ocs-ci root by walking up from this file
            current = Path(__file__).resolve()
            for parent in current.parents:
                if (parent / "ocs_ci").is_dir() and (parent / "tests").is_dir():
                    self.ocsci_root = str(parent)
                    break

        if not self.output_dir:
            self.output_dir = str(Path(self.ocsci_root) / ".zstream_gen_output")


def load_config() -> GeneratorConfig:
    """
    Load configuration from environment variables and config file.

    Environment variables take precedence over config file values.

    Returns:
        GeneratorConfig: The loaded configuration.

    """
    cfg = GeneratorConfig()

    # Load from config file if it exists
    if CONFIG_FILE.exists():
        import yaml

        log.info("Loading config from %s", CONFIG_FILE)
        with open(CONFIG_FILE) as f:
            data = yaml.safe_load(f) or {}

        jira_data = data.get("jira", {})
        if jira_data:
            cfg.jira.url = jira_data.get("url", cfg.jira.url)
            cfg.jira.username = jira_data.get("username", cfg.jira.username)
            cfg.jira.api_token = jira_data.get("api_token", cfg.jira.api_token)
            cfg.jira.project_key = jira_data.get("project_key", cfg.jira.project_key)

        github_data = data.get("github", {})
        if github_data:
            cfg.github.token = github_data.get("token", cfg.github.token)
            cfg.github.ocsci_repo = github_data.get("ocsci_repo", cfg.github.ocsci_repo)
            cfg.github.fork_repo = github_data.get("fork_repo", cfg.github.fork_repo)

        claude_data = data.get("claude", {})
        if claude_data:
            cfg.claude.project_id = claude_data.get("project_id", cfg.claude.project_id)
            cfg.claude.location = claude_data.get("location", cfg.claude.location)
            cfg.claude.model = claude_data.get("model", cfg.claude.model)
            cfg.claude.api_key = claude_data.get("api_key", cfg.claude.api_key)
            cfg.claude.use_vertex = claude_data.get("use_vertex", cfg.claude.use_vertex)

        rovo_data = data.get("rovo", {})
        if rovo_data:
            cfg.rovo.api_token = rovo_data.get("api_token", cfg.rovo.api_token)
            cfg.rovo.enabled = rovo_data.get("enabled", cfg.rovo.enabled)

        gen_data = data.get("generator", {})
        if gen_data:
            cfg.max_retries = gen_data.get("max_retries", cfg.max_retries)
            cfg.dry_run = gen_data.get("dry_run", cfg.dry_run)
            cfg.create_prs = gen_data.get("create_prs", cfg.create_prs)
            cfg.draft_prs = gen_data.get("draft_prs", cfg.draft_prs)
            cfg.backport = gen_data.get("backport", cfg.backport)
            cfg.min_confidence = gen_data.get("min_confidence", cfg.min_confidence)

    # Override with environment variables
    cfg.jira.username = os.environ.get("JIRA_USERNAME", cfg.jira.username)
    cfg.jira.api_token = os.environ.get("JIRA_API_TOKEN", cfg.jira.api_token)
    cfg.github.token = os.environ.get("GITHUB_TOKEN", cfg.github.token)
    cfg.claude.project_id = os.environ.get("VERTEX_PROJECT_ID", cfg.claude.project_id)
    cfg.claude.api_key = os.environ.get("ANTHROPIC_API_KEY", cfg.claude.api_key)
    cfg.rovo.api_token = os.environ.get("ROVO_API_TOKEN", cfg.rovo.api_token)

    if cfg.claude.api_key and not cfg.claude.project_id:
        cfg.claude.use_vertex = False

    if cfg.rovo.api_token:
        cfg.rovo.enabled = True

    return cfg


def create_sample_config():
    """
    Create a sample configuration file at ~/.zstream_test_gen.yaml.

    Returns:
        str: Path to the created sample config file.

    """
    import yaml

    sample = {
        "jira": {
            "url": "https://redhat.atlassian.net",
            "username": "your-email@redhat.com",
            "api_token": "your-jira-api-token",
            "project_key": "DFBUGS",
        },
        "github": {
            "token": "ghp_your-github-token",
            "ocsci_repo": "red-hat-storage/ocs-ci",
            "fork_repo": "your-username/ocs-ci",
        },
        "claude": {
            "project_id": "your-gcp-project-id",
            "location": "us-east5",
            "model": "claude-opus-4-6",
            "api_key": "",
            "use_vertex": True,
        },
        "rovo": {
            "api_token": "",
            "enabled": False,
        },
        "generator": {
            "max_retries": 2,
            "dry_run": False,
            "create_prs": True,
            "draft_prs": True,
            "backport": True,
        },
    }

    with open(CONFIG_FILE, "w") as f:
        yaml.dump(sample, f, default_flow_style=False, sort_keys=False)

    return str(CONFIG_FILE)
