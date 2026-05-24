"""Load and list registered verification workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = ROOT / ".claude" / "framework" / "registry" / "workflows"


def get_default_workflow() -> str:
    """Return the ID of the workflow marked ``default: true``, or first available."""
    for path in sorted(WORKFLOWS_DIR.glob("*.yaml")):
        with path.open() as f:
            wf = yaml.safe_load(f)
        if wf.get("default", False):
            return wf.get("id", path.stem)
    items = sorted(WORKFLOWS_DIR.glob("*.yaml"))
    if items:
        with items[0].open() as f:
            wf = yaml.safe_load(f)
        return wf.get("id", items[0].stem)
    return "zstream-issue-verification"


DEFAULT_WORKFLOW = get_default_workflow()


def workflow_param(wf: dict, key: str, default: Any = None) -> Any:
    """Read a parameter from ``wf["params"]``, handling dict-with-default or scalar."""
    val = wf.get("params", {}).get(key)
    if val is None:
        val = wf.get("defaults", {}).get(key)
    if val is None:
        return default
    if isinstance(val, dict):
        return val.get("default", default)
    return val


def workflow_custom_field(wf: dict, field_name: str) -> str | None:
    """Read a custom JIRA field ID from ``wf["custom_fields"]``."""
    return wf.get("custom_fields", {}).get(field_name)


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
                "default": bool(wf.get("default", False)),
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


if __name__ == "__main__":
    print(get_default_workflow())
