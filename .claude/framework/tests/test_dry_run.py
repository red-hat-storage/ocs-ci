"""Dry-run helper tests."""

import json
from pathlib import Path

import sys

LIB = Path(__file__).resolve().parents[1] / "lib"
sys.path.insert(0, str(LIB))
from dry_run import (
    disable_dry_run,
    enable_dry_run,
    is_dry_run,
    jira_github_writes_allowed,
)  # noqa: E402


def test_dry_run_marker(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DFBUGS_DRY_RUN", raising=False)
    monkeypatch.setenv("JIRA_AGENT_WORKSPACE", str(tmp_path))
    assert not is_dry_run()
    enable_dry_run(tmp_path)
    assert is_dry_run()
    assert not jira_github_writes_allowed()
    disable_dry_run(tmp_path)
    assert not is_dry_run()


def test_dry_run_env(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("JIRA_AGENT_WORKSPACE", str(tmp_path))
    monkeypatch.setenv("DFBUGS_DRY_RUN", "1")
    assert is_dry_run()


def test_run_config_json(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DFBUGS_DRY_RUN", raising=False)
    monkeypatch.setenv("JIRA_AGENT_WORKSPACE", str(tmp_path))
    (tmp_path / "run-config.json").write_text(json.dumps({"dry_run": True}))
    assert is_dry_run()
