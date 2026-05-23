"""Run context loader tests."""

import json
import sys
from pathlib import Path

LIB = Path(__file__).resolve().parents[1] / "lib"
sys.path.insert(0, str(LIB))
from load_run_context import load_context, shell_exports  # noqa: E402


def test_load_from_active_run(tmp_path: Path):
    (tmp_path / "active-run.json").write_text(
        json.dumps(
            {
                "odf_version": "4.18",
                "workflow_id": "zstream-issue-verification",
                "run_id": "test-run",
                "dry_run": True,
            }
        )
    )
    ctx = load_context(tmp_path)
    assert ctx["odf_version"] == "4.18"
    exports = shell_exports(ctx)
    assert 'export ODF_VERSION="4.18"' in exports
    assert "DFBUGS_DRY_RUN=1" in exports
