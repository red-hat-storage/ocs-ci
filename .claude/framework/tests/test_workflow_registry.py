"""Workflow registry tests."""

import sys
from pathlib import Path

LIB = Path(__file__).resolve().parents[1] / "lib"
sys.path.insert(0, str(LIB))
from workflow_registry import (
    DEFAULT_WORKFLOW,
    list_workflows,
    load_workflow,
    prompt_filename,
)  # noqa: E402


def test_default_workflow_exists():
    wfs = list_workflows()
    ids = [w["id"] for w in wfs]
    assert DEFAULT_WORKFLOW in ids


def test_load_zstream():
    wf = load_workflow(DEFAULT_WORKFLOW)
    assert wf["id"] == DEFAULT_WORKFLOW
    assert wf.get("coordinator_agent") == "orchestrator-coordinator"


def test_prompt_filename():
    assert prompt_filename("zstream-issue-verification") == (
        "workflow-zstream-issue-verification-prompt.md"
    )
