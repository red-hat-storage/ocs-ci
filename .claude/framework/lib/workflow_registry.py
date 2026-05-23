"""Load and list registered verification workflows."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = ROOT / ".claude" / "framework" / "registry" / "workflows"
DEFAULT_WORKFLOW = "zstream-issue-verification"


def list_workflows() -> list[dict]:
    items: list[dict] = []
    for path in sorted(WORKFLOWS_DIR.glob("*.yaml")):
        with path.open() as f:
            wf = yaml.safe_load(f)
        items.append(
            {
                "id": wf.get("id", path.stem),
                "name": wf.get("name", path.stem),
                "description": (wf.get("description") or "").strip().split("\n")[0],
                "file": str(path.relative_to(ROOT)),
                "default": wf.get("id", path.stem) == DEFAULT_WORKFLOW,
            }
        )
    return items


def load_workflow(workflow_id: str) -> dict:
    path = WORKFLOWS_DIR / f"{workflow_id}.yaml"
    if not path.is_file():
        known = [w["id"] for w in list_workflows()]
        raise FileNotFoundError(
            f"Unknown workflow '{workflow_id}'. Known: {', '.join(known) or '(none)'}"
        )
    with path.open() as f:
        data = yaml.safe_load(f)
    data.setdefault("id", workflow_id)
    return data


def prompt_filename(workflow_id: str) -> str:
    return f"workflow-{workflow_id}-prompt.md"
