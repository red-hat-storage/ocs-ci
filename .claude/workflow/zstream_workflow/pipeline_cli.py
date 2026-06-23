#!/usr/bin/env python3
"""
Z-stream workflow CLI (wrapper around generic workflow engine).

Usage (from ocs-ci repo root):

  python .claude/workflow/zstream_workflow/pipeline_cli.py run \\
    --pipeline zstream_verification \\
    --param odf_version=4.22

  python .claude/workflow/zstream_workflow/pipeline_cli.py run \\
    --pipeline zstream_verification \\
    --config .claude/workflow/zstream_workflow/pipelines/configs/my-run.yaml
"""

from __future__ import annotations

import sys
from pathlib import Path

_ZSTREAM_DIR = Path(__file__).resolve().parent
_WORKFLOW_DIR = _ZSTREAM_DIR.parent
_CLAUDE_DIR = _WORKFLOW_DIR.parent
_REPO_ROOT = _CLAUDE_DIR.parent

for _path in (_ZSTREAM_DIR, _WORKFLOW_DIR, _CLAUDE_DIR, _REPO_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from workflow_paths import WORKFLOW_PATHS
from workflow_lib.workflow_cli import main as workflow_main

_ZSTREAM_DEFAULTS = [
    "--workflows-dir",
    str(WORKFLOW_PATHS.workflows_dir),
    "--registry",
    str(WORKFLOW_PATHS.registry_file),
    "--executors-module",
    "executors",
    "--context-factory",
    "workflow_context:ZstreamContextFactory",
]


def main(argv: list[str] | None = None) -> int:
    user_argv = list(argv if argv is not None else sys.argv[1:])
    return workflow_main(_ZSTREAM_DEFAULTS + user_argv)


if __name__ == "__main__":
    sys.exit(main())
