"""Load per-run YAML config files for workflow execution."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from workflow_lib.loader import load_yaml

_RESERVED_TOP_LEVEL_KEYS = frozenset(
    {"pipeline", "parameters", "defaults", "run", "description", "name"}
)


def load_run_config(
    path: Path | str,
    *,
    pipeline_name: str | None = None,
) -> dict[str, Any]:
    """Load a run config YAML file (parameters, defaults, run options)."""
    config_path = Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Run config not found: {config_path}")

    data = load_yaml(config_path)
    file_pipeline = data.get("pipeline")
    if pipeline_name and file_pipeline and file_pipeline != pipeline_name:
        raise ValueError(
            f"Config pipeline '{file_pipeline}' does not match "
            f"--pipeline {pipeline_name}"
        )

    parameters: dict[str, Any] = dict(data.get("parameters") or {})
    for key, value in data.items():
        if key not in _RESERVED_TOP_LEVEL_KEYS:
            parameters[key] = value

    return {
        "pipeline": file_pipeline or pipeline_name,
        "description": data.get("description"),
        "parameters": parameters,
        "defaults": dict(data.get("defaults") or {}),
        "run": dict(data.get("run") or {}),
    }
