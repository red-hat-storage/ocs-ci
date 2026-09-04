"""Run context protocol for workflow execution."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class RunContext(Protocol):
    """Workflow run state passed to stage executors."""

    @property
    def run_id(self) -> str: ...  # noqa: E704

    @property
    def run_dir(self) -> Path: ...  # noqa: E704

    def stages_completed(self) -> list[str]: ...  # noqa: E704

    def setup_logging(self) -> None: ...  # noqa: E704

    def to_ref_dict(self, parameters: dict[str, Any]) -> dict[str, Any]:
        """Values for ``$run.*`` reference resolution."""
        ...


class RunContextFactory(Protocol):
    """Create or load run context for a workflow family."""

    @property
    def create_run_stage(self) -> str:
        """Pipeline stage name that may create a new run when no run_id is set."""
        ...

    def load(self, run_id: str) -> RunContext: ...  # noqa: E704

    def create(self, parameters: dict[str, Any]) -> RunContext: ...  # noqa: E704
