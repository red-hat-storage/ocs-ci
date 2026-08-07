#!/usr/bin/env python3
"""
Generic workflow orchestrator CLI.

Usage (from ocs-ci repo root):

  python .claude/workflow/workflow_lib/workflow_cli.py run \\
    --workflows-dir .claude/workflow/issue_verification_workflow/pipelines \\
    --registry .claude/workflow/issue_verification_workflow/agents/registry.yaml \\
    --executors-module executors \\
    --context-factory workflow_context:IssueVerificationContextFactory \\
    --pipeline issue_verification \\
    --param odf_version=4.22
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any

_WORKFLOW_LIB_DIR = Path(__file__).resolve().parent
_WORKFLOW_DIR = _WORKFLOW_LIB_DIR.parent
_CLAUDE_DIR = _WORKFLOW_DIR.parent
_REPO_ROOT = _CLAUDE_DIR.parent

for _path in (_WORKFLOW_DIR, _CLAUDE_DIR, _REPO_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from workflow_lib.config import WorkflowPaths
from workflow_lib.loader import list_workflows, load_agent_registry, load_workflow
from workflow_lib.run_config import load_run_config
from workflow_lib.runner import WorkflowRunner

log = logging.getLogger("workflow_lib")


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )


def _parse_param_pairs(pairs: list[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"Invalid --param (expected key=value): {pair}")
        key, value = pair.split("=", 1)
        params[key.strip()] = value.strip()
    return params


def _coerce_param(value: str) -> str | bool | int | None:
    lowered = value.lower()
    if lowered in ("true", "yes", "1"):
        return True
    if lowered in ("false", "no", "0"):
        return False
    if lowered in ("null", "none", ""):
        return None
    if value.isdigit():
        return int(value)
    return value


def _load_executors(module_path: str) -> dict[str, Any]:
    module = importlib.import_module(module_path)
    executors = getattr(module, "AGENT_EXECUTORS", None)
    if not isinstance(executors, dict):
        raise ValueError(f"{module_path} has no AGENT_EXECUTORS dict")
    return executors


def _load_context_factory(factory_path: str) -> Any:
    if ":" not in factory_path:
        raise ValueError(
            "context factory must be module:Class, e.g. workflow_context:IssueVerificationContextFactory"
        )
    module_name, class_name = factory_path.split(":", 1)
    module = importlib.import_module(module_name)
    factory_cls = getattr(module, class_name)
    return factory_cls()


def _merge_run_parameters(
    args: argparse.Namespace,
    paths: WorkflowPaths,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    pipeline = load_workflow(args.pipeline, paths)
    default_keys = set(pipeline.get("defaults", {}).keys())

    parameters: dict[str, Any] = {}
    defaults_override: dict[str, Any] = {}
    run_opts: dict[str, Any] = {}

    if args.config:
        cfg = load_run_config(args.config, pipeline_name=args.pipeline)
        parameters.update(cfg.get("parameters") or {})
        defaults_override.update(cfg.get("defaults") or {})
        run_opts.update(cfg.get("run") or {})
        if cfg.get("description"):
            log.info("Run config: %s", cfg["description"])

    for key, val in _parse_param_pairs(args.param or []).items():
        coerced = _coerce_param(val)
        if key in default_keys:
            defaults_override[key] = coerced
        else:
            parameters[key] = coerced

    return parameters, defaults_override, run_opts


def cmd_list(args: argparse.Namespace) -> int:
    paths = WorkflowPaths(
        workflows_dir=Path(args.workflows_dir),
        registry_file=Path(args.registry),
        repo_root=Path(args.repo_root) if args.repo_root else _REPO_ROOT,
    )
    registry = load_agent_registry(paths).get("agents", {})
    print(
        json.dumps(
            {"workflows": list_workflows(paths), "agents": list(registry)},
            indent=2,
        )
    )
    return 0


def cmd_describe(args: argparse.Namespace) -> int:
    paths = WorkflowPaths(
        workflows_dir=Path(args.workflows_dir),
        registry_file=Path(args.registry),
        repo_root=Path(args.repo_root) if args.repo_root else _REPO_ROOT,
    )
    print(
        json.dumps(
            {
                "workflow": load_workflow(args.pipeline, paths),
                "agents": load_agent_registry(paths).get("agents", {}),
            },
            indent=2,
        )
    )
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    paths = WorkflowPaths(
        workflows_dir=Path(args.workflows_dir),
        registry_file=Path(args.registry),
        repo_root=Path(args.repo_root) if args.repo_root else _REPO_ROOT,
    )
    parameters, defaults_override, run_opts = _merge_run_parameters(args, paths)

    run_id = args.run_id if args.run_id is not None else run_opts.get("run_id")
    from_stage = (
        args.from_stage if args.from_stage is not None else run_opts.get("from_stage")
    )
    until_stage = (
        args.until_stage
        if args.until_stage is not None
        else run_opts.get("until_stage")
    )
    force = args.force or bool(run_opts.get("force", False))
    skip_if_completed = not args.no_skip_completed
    if "skip_if_completed" in run_opts and not args.no_skip_completed:
        skip_if_completed = bool(run_opts["skip_if_completed"])

    runner = WorkflowRunner(
        args.pipeline,
        paths,
        _load_executors(args.executors_module),
        _load_context_factory(args.context_factory),
        parameters=parameters,
        defaults_override=defaults_override or None,
        run_id=run_id,
        from_stage=from_stage,
        until_stage=until_stage,
        force=force,
        skip_if_completed=skip_if_completed,
    )
    result = runner.run()
    print(json.dumps(result.to_dict(), indent=2))
    log.info("Run id: %s", result.run_id)
    if result.artifacts.get("issues_file"):
        log.info("Issues file: %s", result.artifacts["issues_file"])
    return 0


def _add_workflow_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--workflows-dir",
        required=True,
        help="Directory containing workflow *.yaml files",
    )
    parser.add_argument(
        "--registry",
        required=True,
        help="Agent registry YAML path",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="OCS-CI repo root (default: auto-detect)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generic YAML workflow orchestrator")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    list_cmd = sub.add_parser("list", help="List workflows and agents")
    _add_workflow_args(list_cmd)
    list_cmd.set_defaults(func=cmd_list)

    describe = sub.add_parser("describe", help="Show workflow definition")
    _add_workflow_args(describe)
    describe.add_argument("--pipeline", required=True)
    describe.set_defaults(func=cmd_describe)

    run = sub.add_parser("run", help="Execute a workflow")
    _add_workflow_args(run)
    run.add_argument("--executors-module", required=True)
    run.add_argument("--context-factory", required=True)
    run.add_argument("--pipeline", required=True)
    run.add_argument("--config", default=None, metavar="PATH")
    run.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
    )
    run.add_argument("--run-id", default=None)
    run.add_argument("--from-stage", default=None)
    run.add_argument("--until-stage", default=None)
    run.add_argument("--force", action="store_true")
    run.add_argument("--no-skip-completed", action="store_true")
    run.set_defaults(func=cmd_run)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
