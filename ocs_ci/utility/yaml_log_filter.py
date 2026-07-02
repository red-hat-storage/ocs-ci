"""
YAML Log Filter - Reduces verbose YAML output in logs.

Filters verbose fields from `oc get -o yaml` outputs BEFORE logging,
while keeping full data available for actual use.
"""

import logging
import yaml

log = logging.getLogger(__name__)

# Fields to filter: {ResourceKind: {path.to.parent: [fields_to_remove]}}
VERBOSE_FIELDS: dict[str, dict[str, list[str]]] = {
    "PackageManifest": {
        "status.channels": ["currentCSVDesc", "entries"],
    },
}

MIN_SIZE_FOR_FILTERING = 5000


def filter_verbose_yaml(yaml_str: str, min_size: int = MIN_SIZE_FOR_FILTERING) -> str:
    """
    Filter verbose fields from YAML string for logging purposes only.

    Args:
        yaml_str (str): Raw YAML string from oc command output.
        min_size (int): Minimum size to trigger filtering.

    Returns:
        str: Summary string for logging.

    """
    if not yaml_str or len(yaml_str) < min_size:
        return yaml_str

    try:
        data = yaml.safe_load(yaml_str)
    except yaml.YAMLError:
        return yaml_str

    if not isinstance(data, dict):
        return yaml_str

    kind = data.get("kind", "")

    # Check if this resource type should be filtered
    if kind == "List":
        item_kinds = {item.get("kind", "") for item in data.get("items", [])}
        should_filter = any(k in VERBOSE_FIELDS for k in item_kinds)
    else:
        should_filter = kind in VERBOSE_FIELDS

    if not should_filter:
        return yaml_str

    # Apply filtering
    if kind == "List":
        for item in data.get("items", []):
            _filter_item(item)
    else:
        _filter_item(data)

    return _format_summary(data, len(yaml_str))


def _filter_item(item: dict) -> None:
    """Remove verbose fields from a single openshift resource."""
    kind = item.get("kind", "")
    if kind not in VERBOSE_FIELDS:
        return

    for path, fields_to_remove in VERBOSE_FIELDS[kind].items():
        target = _navigate_path(item, path)
        if target is not None:
            _remove_fields(target, fields_to_remove)


def _navigate_path(data: dict, path: str):
    """Navigate to a nested location using dot notation."""
    current = data
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _remove_fields(target, fields: list[str]) -> None:
    """Remove specified fields from target (list of dicts or dict)."""
    if isinstance(target, list):
        for entry in target:
            if isinstance(entry, dict):
                for field in fields:
                    entry.pop(field, None)
    elif isinstance(target, dict):
        for field in fields:
            target.pop(field, None)


def _format_summary(data: dict, original_size: int) -> str:
    """Create a concise summary string for logging."""
    kind = data.get("kind", "Unknown")

    if kind == "List":
        items = data.get("items", [])
        item_kinds: dict[str, int] = {}
        for item in items:
            k = item.get("kind", "Unknown")
            item_kinds[k] = item_kinds.get(k, 0) + 1

        summary = ", ".join(f"{count} {k}" for k, count in item_kinds.items())
        return f"[List: {summary}] (filtered from {original_size:,} chars)"
    else:
        name = data.get("metadata", {}).get("name", "?")
        return f"[{kind}: {name}] (filtered from {original_size:,} chars)"
