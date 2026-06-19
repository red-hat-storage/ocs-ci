"""Configuration for the ocs-ci code vector database."""

from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
CLAUDE_DIR = MODULE_DIR.parent
REPO_ROOT = CLAUDE_DIR.parent
AGENTS_DIR = CLAUDE_DIR / "agents"

DATA_DIR = MODULE_DIR / "data"
QDRANT_PATH = DATA_DIR / "qdrant"
MANIFEST_PATH = DATA_DIR / "manifest.json"

# Only these top-level repo directories (and their subdirs) are indexed.
INDEX_DIR_NAMES: tuple[str, ...] = (
    "conf",
    "Docker_files",
    "docs",
    "examples",
    "external",
    "ocs-ci",
    "scripts",
    "src",
    "template_test",
    "terraform",
    "tests",
)

# Resolve user-facing names to on-disk paths (ocs-ci/ → ocs_ci/).
INDEX_DIR_ALIASES: dict[str, str] = {
    "ocs-ci": "ocs_ci",
}

INDEX_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".py",
        ".md",
        ".rst",
        ".yaml",
        ".yml",
        ".json",
        ".tf",
        ".hcl",
        ".sh",
        ".bash",
        ".ini",
        ".cfg",
        ".toml",
        ".j2",
        ".jinja2",
        ".xml",
        ".properties",
        ".txt",
        ".csv",
    }
)

INDEX_FILENAMES: frozenset[str] = frozenset(
    {
        "dockerfile",
        "makefile",
        "jenkinsfile",
        "vagrantfile",
    }
)

SKIP_DIR_NAMES: frozenset[str] = frozenset(
    {
        "__pycache__",
        ".git",
        ".tox",
        ".venv",
        "node_modules",
        ".pytest_cache",
        ".mypy_cache",
        "egg-info",
    }
)

MAX_EMBED_CHARS = 8000
MAX_FILE_BYTES = 512_000

DEFAULT_COLLECTION = "ocs_ci_code"
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_VECTOR_SIZE = 384
DEFAULT_BATCH_SIZE = 64
DEFAULT_TOP_K = 10

DEFAULT_QDRANT_URL: str | None = None


def resolve_index_dirs(repo_root: Path | None = None) -> list[Path]:
    """Return existing absolute paths for configured index directories."""
    root = repo_root or REPO_ROOT
    dirs: list[Path] = []
    for name in INDEX_DIR_NAMES:
        disk_name = INDEX_DIR_ALIASES.get(name, name)
        path = (root / disk_name).resolve()
        if path.is_dir():
            dirs.append(path)
    return dirs
