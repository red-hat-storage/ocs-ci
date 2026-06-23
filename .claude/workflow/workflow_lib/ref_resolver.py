"""Resolve $pipeline.* and $stages.* references in pipeline parameters."""

from __future__ import annotations

import re
from typing import Any

_REF_PATTERN = re.compile(r"^\$(pipeline|stages|run)\.(.+)$")


def _traverse(root: Any, path: str) -> Any:
    """Walk a dotted path; supports ``[*]`` for list projection."""
    current = root
    for segment in path.split("."):
        if segment.endswith("[*]"):
            key = segment[:-3]
            if key:
                current = current.get(key) if isinstance(current, dict) else None
            if not isinstance(current, list):
                return []
            return current
        if isinstance(current, dict):
            current = current.get(segment)
        else:
            return None
    return current


def resolve_value(
    value: Any,
    *,
    pipeline: dict[str, Any],
    parameters: dict[str, Any],
    stage_outputs: dict[str, dict[str, Any]],
    run_context: dict[str, Any],
) -> Any:
    """
    Resolve a parameter value, including ``$pipeline`` / ``$stages`` / ``$run`` refs.

    Non-string values and plain strings without a leading ``$`` pass through unchanged.
    """
    if isinstance(value, list):
        return [
            resolve_value(
                item,
                pipeline=pipeline,
                parameters=parameters,
                stage_outputs=stage_outputs,
                run_context=run_context,
            )
            for item in value
        ]

    if not isinstance(value, str) or not value.startswith("$"):
        return value

    match = _REF_PATTERN.match(value)
    if not match:
        return value

    namespace, path = match.group(1), match.group(2)

    if namespace == "pipeline":
        root: Any = {
            "parameters": parameters,
            "defaults": pipeline.get("defaults", {}),
        }
        return _traverse(root, path)

    if namespace == "stages":
        return _traverse(stage_outputs, path)

    if namespace == "run":
        return _traverse(run_context, path)

    return value


def resolve_parameters(
    raw_parameters: dict[str, Any],
    *,
    pipeline: dict[str, Any],
    parameters: dict[str, Any],
    stage_outputs: dict[str, dict[str, Any]],
    run_context: dict[str, Any],
) -> dict[str, Any]:
    """Resolve all parameters for a pipeline stage."""
    return {
        key: resolve_value(
            val,
            pipeline=pipeline,
            parameters=parameters,
            stage_outputs=stage_outputs,
            run_context=run_context,
        )
        for key, val in raw_parameters.items()
    }


def evaluate_when(
    expression: Any,
    *,
    pipeline: dict[str, Any],
    parameters: dict[str, Any],
    stage_outputs: dict[str, dict[str, Any]],
    run_context: dict[str, Any],
) -> bool:
    """Evaluate a stage ``when`` condition (truthy after reference resolution)."""
    if expression is None:
        return True
    resolved = resolve_value(
        expression,
        pipeline=pipeline,
        parameters=parameters,
        stage_outputs=stage_outputs,
        run_context=run_context,
    )
    return bool(resolved)
