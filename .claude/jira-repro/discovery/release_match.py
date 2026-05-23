"""Match JIRA Target Release field values to CLI ODF version argument."""

from __future__ import annotations

import re
from typing import Any

_VERSION_RE = re.compile(r"\d+\.\d+(?:\.\d+)?")
_TARGET_RELEASE_RE = re.compile(r"^odf-(\d+\.\d+)\.z$", re.IGNORECASE)


def extract_versions(text: str) -> list[str]:
    return _VERSION_RE.findall(text or "")


def zstream_prefix(version: str) -> str | None:
    parts = version.split(".")
    if len(parts) >= 2:
        return f"{parts[0]}.{parts[1]}"
    return None


def cli_to_target_release_value(cli_version: str) -> str:
    """Map CLI ODF version to JIRA Target Release value.

    Examples:
        4.19   -> odf-4.19.z
        4.22.1 -> odf-4.22.z  (z-stream uses major.minor)
    """
    versions = extract_versions(cli_version.strip())
    if versions:
        zs = zstream_prefix(versions[0]) or versions[0]
    else:
        zs = cli_version.strip().lstrip("odf-").rstrip(".z")
    return f"odf-{zs}.z"


def field_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("name", "value", "displayName"):
            if value.get(key):
                return str(value[key])
        return str(value)
    if isinstance(value, list):
        return " ".join(field_to_text(v) for v in value)
    return str(value)


def target_release_matches(field_value: Any, cli_version: str) -> bool:
    """True if issue Target Release matches CLI version (odf-X.Y.z convention)."""
    if not cli_version:
        return False
    text = field_to_text(field_value).strip()
    if not text:
        return False

    expected = cli_to_target_release_value(cli_version)
    if text.lower() == expected.lower():
        return True

    # Normalized compare on odf-M.m.z pattern in field
    field_m = _TARGET_RELEASE_RE.match(text.lower())
    expected_m = _TARGET_RELEASE_RE.match(expected.lower())
    if field_m and expected_m and field_m.group(1) == expected_m.group(1):
        return True

    # Legacy / alternate labels (ODF 4.19, etc.)
    cli_versions = extract_versions(cli_version)
    field_versions = extract_versions(text)
    if cli_versions and field_versions:
        zs = zstream_prefix(cli_versions[0])
        if zs and any(zstream_prefix(fv) == zs or fv == zs for fv in field_versions):
            return True

    return expected.lower() in text.lower()
