"""JIRA context extraction (no hardcoded scenarios)."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jira-repro"))
from build_repro_context import build_context  # noqa: E402
from check_script_generated import is_placeholder  # noqa: E402


def test_context_has_ai_flag():
    analysis = {"summary": "test bug", "description_excerpt": "something broke"}
    ctx = build_context(analysis, None, issue_key="DFBUGS-1", odf_target="4.19")
    assert ctx["ai_generation_required"] is True
    assert "md_blow" not in json.dumps(ctx)  # no hardcoded scenario names required


def test_placeholder_detected():
    art = Path(__file__).parent / "_tmp_art"
    art.mkdir(exist_ok=True)
    (art / "reproduce.py").write_text("def test_verify_fix():\n    assert True\n")
    bad, _ = is_placeholder(art / "reproduce.py")
    assert bad
    (art / "reproduce.py").unlink()
    art.rmdir()
