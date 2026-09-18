#!/usr/bin/env python3
"""
Issue verification workflow CLI (wrapper around generic workflow engine).

Usage (from ocs-ci repo root):

  python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \\
    --pipeline issue_verification \\
    --param odf_version=4.22

  python .claude/workflow/issue_verification_workflow/pipeline_cli.py run \\
    --pipeline issue_verification \\
    --config .claude/workflow/issue_verification_workflow/pipelines/configs/my-run.yaml
"""

from __future__ import annotations

import sys
from pathlib import Path

_ISSUE_VERIFICATION_DIR = Path(__file__).resolve().parent
_WORKFLOW_DIR = _ISSUE_VERIFICATION_DIR.parent
_CLAUDE_DIR = _WORKFLOW_DIR.parent
_REPO_ROOT = _CLAUDE_DIR.parent

for _path in (_ISSUE_VERIFICATION_DIR, _WORKFLOW_DIR, _CLAUDE_DIR, _REPO_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from workflow_paths import WORKFLOW_PATHS
from workflow_lib.workflow_cli import main as workflow_main
from workflow_config import DEFAULT_CONFIG_PATH, resolve_config_path

_WORKFLOW_PATH_ARGS = [
    "--workflows-dir",
    str(WORKFLOW_PATHS.workflows_dir),
    "--registry",
    str(WORKFLOW_PATHS.registry_file),
]

_RUN_DEFAULTS = [
    *_WORKFLOW_PATH_ARGS,
    "--executors-module",
    "executors",
    "--context-factory",
    "workflow_context:IssueVerificationContextFactory",
]


def main(argv: list[str] | None = None) -> int:
    user_argv = list(argv if argv is not None else sys.argv[1:])
    if not user_argv:
        return workflow_main(user_argv)

    command = user_argv[0]
    rest = user_argv[1:]

    if command == "run":
        has_config = "--config" in rest or "-c" in rest
        if not has_config and resolve_config_path() is not None:
            rest = ["--config", str(DEFAULT_CONFIG_PATH), *rest]
        return workflow_main(["run", *_RUN_DEFAULTS, *rest])

    if command in ("list", "describe"):
        return workflow_main([command, *_WORKFLOW_PATH_ARGS, *rest])

    return workflow_main(user_argv)


if __name__ == "__main__":
    sys.exit(main())
