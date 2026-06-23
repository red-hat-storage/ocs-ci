"""Z-stream workflow path configuration."""

from pathlib import Path

from workflow_lib.config import WorkflowPaths

ZSTREAM_DIR = Path(__file__).resolve().parent
_WORKFLOW_DIR = ZSTREAM_DIR.parent
_CLAUDE_DIR = _WORKFLOW_DIR.parent
WORKFLOW_PATHS = WorkflowPaths(
    workflows_dir=ZSTREAM_DIR / "pipelines",
    registry_file=ZSTREAM_DIR / "agents" / "registry.yaml",
    repo_root=_CLAUDE_DIR.parent,
)
