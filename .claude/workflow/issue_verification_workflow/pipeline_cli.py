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

_ISSUE_VERIFICATION_DEFAULTS = [
    "--workflows-dir",
    str(WORKFLOW_PATHS.workflows_dir),
    "--registry",
    str(WORKFLOW_PATHS.registry_file),
    "--executors-module",
    "executors",
    "--context-factory",
    "workflow_context:IssueVerificationContextFactory",
]


def main(argv: list[str] | None = None) -> int:
    user_argv = list(argv if argv is not None else sys.argv[1:])
    if user_argv and user_argv[0] == "run":
        has_config = "--config" in user_argv or "-c" in user_argv
        if not has_config and resolve_config_path() is not None:
            user_argv = ["run", "--config", str(DEFAULT_CONFIG_PATH), *user_argv[1:]]
    return workflow_main(_ISSUE_VERIFICATION_DEFAULTS + user_argv)


if __name__ == "__main__":
    sys.exit(main())
