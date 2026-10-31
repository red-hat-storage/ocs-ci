"""Load workflow and agent registry YAML files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from workflow_lib.config import WorkflowPaths

log = logging.getLogger(__name__)


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return data


def list_workflows(paths: WorkflowPaths) -> list[str]:
    if not paths.workflows_dir.is_dir():
        return []
    return sorted(p.stem for p in paths.workflows_dir.glob("*.yaml"))


def load_workflow(name: str, paths: WorkflowPaths) -> dict[str, Any]:
    workflow_path = paths.workflows_dir / f"{name}.yaml"
    if not workflow_path.is_file():
        raise FileNotFoundError(f"Workflow not found: {workflow_path}")
    config = load_yaml(workflow_path)
    config["_workflow_file"] = str(workflow_path)
    return config


def load_agent_registry(paths: WorkflowPaths) -> dict[str, Any]:
    return load_yaml(paths.registry_file)


def get_agent_record_stage(
    agent_name: str,
    paths: WorkflowPaths,
) -> str | None:
    registry = load_agent_registry(paths)
    agent = registry.get("agents", {}).get(agent_name, {})
    return agent.get("record_stage")
