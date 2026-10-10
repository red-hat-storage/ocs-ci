"""Workflow path configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WorkflowPaths:
    """Directories and files for a workflow family (e.g. z-stream)."""

    workflows_dir: Path
    registry_file: Path
    repo_root: Path

    @property
    def agents_parent(self) -> Path:
        return self.repo_root / ".claude" / "agents"
