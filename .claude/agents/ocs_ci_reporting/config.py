"""Configuration for the OCS-CI reporting agent."""

from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
AGENTS_DIR = MODULE_DIR.parent
REPO_ROOT = AGENTS_DIR.parent.parent

TEMPLATES_DIR = MODULE_DIR / "templates"

AUTH_YAML_CANDIDATES: tuple[Path, ...] = (
    REPO_ROOT / "data" / "auth.yaml",
    REPO_ROOT / "data" / "auth.yml",
)

DEFAULT_TEMPLATE = "plain.md.j2"
SUPPORTED_FORMATS = frozenset({"markdown", "html", "text"})
SUPPORTED_CHANNEL_TYPES = frozenset({"file", "slack", "email"})
