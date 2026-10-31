"""Issue verification workflow path configuration."""

from pathlib import Path

from workflow_lib.config import WorkflowPaths

ISSUE_VERIFICATION_DIR = Path(__file__).resolve().parent
_WORKFLOW_DIR = ISSUE_VERIFICATION_DIR.parent
_CLAUDE_DIR = _WORKFLOW_DIR.parent
WORKFLOW_PATHS = WorkflowPaths(
    workflows_dir=ISSUE_VERIFICATION_DIR / "pipelines",
    registry_file=ISSUE_VERIFICATION_DIR / "agents" / "registry.yaml",
    repo_root=_CLAUDE_DIR.parent,
)
