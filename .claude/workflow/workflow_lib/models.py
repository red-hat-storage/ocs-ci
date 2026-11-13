"""Workflow run result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StageRunResult:
    name: str
    agent: str
    status: str
    outputs: dict[str, Any] = field(default_factory=dict)
    skipped: bool = False
    reason: str | None = None


@dataclass
class WorkflowRunResult:
    pipeline_name: str
    run_id: str
    run_dir: str
    artifacts: dict[str, str] = field(default_factory=dict)
    stages: list[StageRunResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "run_dir": self.run_dir,
            "artifacts": self.artifacts,
            "issues_file": self.artifacts.get("issues_file"),
            "stages": [
                {
                    "name": stage.name,
                    "agent": stage.agent,
                    "status": stage.status,
                    "skipped": stage.skipped,
                    "reason": stage.reason,
                    "outputs": {
                        key: (val if key != "issues" else f"<{len(val)} issues>")
                        for key, val in stage.outputs.items()
                    },
                }
                for stage in self.stages
            ],
        }
