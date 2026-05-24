"""Workflow registry tests."""

import sys
from pathlib import Path

LIB = Path(__file__).resolve().parents[1] / "lib"
sys.path.insert(0, str(LIB))
from workflow_registry import (
    DEFAULT_WORKFLOW,
    get_default_workflow,
    list_workflows,
    load_workflow,
    prompt_filename,
    workflow_custom_field,
    workflow_param,
)  # noqa: E402


def test_default_workflow_exists():
    wfs = list_workflows()
    ids = [w["id"] for w in wfs]
    assert DEFAULT_WORKFLOW in ids


def test_get_default_workflow_matches_flag():
    wf_id = get_default_workflow()
    wf = load_workflow(wf_id)
    assert wf.get("default", False) is True


def test_load_zstream():
    wf = load_workflow(DEFAULT_WORKFLOW)
    assert wf["id"] == DEFAULT_WORKFLOW
    assert wf.get("coordinator_agent") == "orchestrator-coordinator"


def test_prompt_filename():
    assert prompt_filename("zstream-issue-verification") == (
        "workflow-zstream-issue-verification-prompt.md"
    )


def test_workflow_param_dict():
    wf = {"params": {"jira_status": {"default": "ON_QA", "description": "..."}}}
    assert workflow_param(wf, "jira_status") == "ON_QA"


def test_workflow_param_scalar():
    wf = {"params": {"jira_project": "DFBUGS"}}
    assert workflow_param(wf, "jira_project") == "DFBUGS"


def test_workflow_param_missing():
    wf = {"params": {}}
    assert workflow_param(wf, "nonexistent", "fallback") == "fallback"


def test_workflow_param_from_defaults():
    wf = {"params": {}, "defaults": {"skip_label": "skip-ocsci-agent"}}
    assert workflow_param(wf, "skip_label") == "skip-ocsci-agent"


def test_workflow_custom_field():
    wf = load_workflow(DEFAULT_WORKFLOW)
    assert workflow_custom_field(wf, "target_release") == "customfield_10886"
    assert workflow_custom_field(wf, "prod_build_version") == "customfield_10566"


def test_workflow_custom_field_missing():
    wf = {"custom_fields": {}}
    assert workflow_custom_field(wf, "nonexistent") is None


def test_list_workflows_default_flag():
    wfs = list_workflows()
    defaults = [w for w in wfs if w["default"]]
    assert len(defaults) >= 1
    assert defaults[0]["id"] == DEFAULT_WORKFLOW
