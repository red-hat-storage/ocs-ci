#!/usr/bin/env python3
"""
Compare JIRA-mentioned product build version vs ODF installed on cluster.

Rule: if JIRA specifies a product/ODF build version, cluster installed version
must be >= that version. Otherwise verification must not proceed.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

_CLAUDE_ROOT = Path(__file__).resolve().parents[1]  # .../ocs-ci/.claude
import sys

if str(_CLAUDE_ROOT / "jira-repro") not in sys.path:
    sys.path.insert(0, str(_CLAUDE_ROOT / "jira-repro"))

# Red Hat DFBUGS — "Prod build version" (when set)
_PROD_BUILD_FIELD = "customfield_10566"

_COMPONENTS_VERSION_RE = re.compile(
    r"version of all relevant components[^:]*:\s*"
    r"(?:OCP,\s*ODF,\s*RHCS,\s*ACM[^:]*:)?\s*"
    r"(\d+\.\d+(?:\.\d+)?)",
    re.IGNORECASE | re.DOTALL,
)


def parse_build_version(version_str: str) -> tuple[int, ...] | None:
    """Parse ODF/CSV style versions into comparable integer tuple."""
    if not version_str or not str(version_str).strip():
        return None
    s = str(version_str).strip().lower()
    s = s.removeprefix("odf-").removesuffix(".z")
    parts = re.findall(r'\d+', s)
    if len(parts) < 2:
        return None
    # Pad to 4 elements (major, minor, patch, build) for consistent comparison
    ints = [int(p) for p in parts[:4]]
    while len(ints) < 4:
        ints.append(0)
    return tuple(ints)


def version_gte(installed: tuple[int, ...], required: tuple[int, ...]) -> bool:
    """True if installed >= required (component-wise, padded with zeros)."""
    n = max(len(installed), len(required))
    a = installed + (0,) * (n - len(installed))
    b = required + (0,) * (n - len(required))
    return a >= b


def extract_jira_product_build_versions(
    raw: dict | None,
    analysis: dict | None = None,
) -> list[str]:
    """Collect product/ODF build versions mentioned in JIRA (not Target Release z-stream)."""
    found: list[str] = []

    if raw:
        fields = raw.get("fields") or {}

        # Check Prod Build Version field first
        prod = fields.get(_PROD_BUILD_FIELD)
        if isinstance(prod, str) and prod.strip():
            found.append(prod.strip())
        elif prod:
            found.append(str(prod))
        if found:
            return _dedup(found)

        # Check affects-versions
        for av in fields.get("versions") or []:
            name = (av or {}).get("name", "")
            if name and re.search(r"\d+\.\d+", name):
                found.append(name)
        if found:
            return _dedup(found)

        # Extract description text
        d = fields.get("description")
        if isinstance(d, dict):
            from build_repro_steps import adf_text
            desc = adf_text(d) or ""
        elif isinstance(d, str):
            desc = d
        else:
            desc = ""
    else:
        desc = (analysis or {}).get("description_excerpt") or ""

    # Single regex scan on description for version patterns
    m = _COMPONENTS_VERSION_RE.search(desc)
    if m:
        found.append(m.group(1))

    for m in re.finditer(r"ODF[,\s]+(\d+\.\d+(?:\.\d+)?)", desc, re.IGNORECASE):
        found.append(m.group(1))

    return _dedup(found)


def _dedup(items: list[str]) -> list[str]:
    """De-duplicate preserving order."""
    seen: set[str] = set()
    out: list[str] = []
    for v in items:
        key = v.lower()
        if key not in seen:
            seen.add(key)
            out.append(v)
    return out


def evaluate_build_version_gate(
    cluster_installed: str,
    jira_versions: list[str],
) -> dict[str, Any]:
    """
    If jira_versions is non-empty, require cluster_installed >= max(jira_versions).
    """
    installed_t = parse_build_version(cluster_installed)
    parsed_jira: list[tuple[int, ...]] = []
    labels: list[str] = []
    for jv in jira_versions:
        t = parse_build_version(jv)
        if t:
            parsed_jira.append(t)
            labels.append(jv)

    if not parsed_jira:
        return {
            "check_applied": False,
            "proceed": True,
            "reason": "no product build version mentioned in JIRA — gate skipped",
            "jira_product_build_versions": [],
            "jira_required_minimum": None,
            "cluster_installed": cluster_installed,
            "cluster_installed_parsed": str(installed_t) if installed_t else None,
        }

    if not installed_t:
        return {
            "check_applied": True,
            "proceed": False,
            "reason": "cluster ODF version could not be parsed",
            "jira_product_build_versions": labels,
            "jira_required_minimum": str(max(parsed_jira)),
            "cluster_installed": cluster_installed,
            "cluster_installed_parsed": None,
        }

    required_t = max(parsed_jira)
    ok = version_gte(installed_t, required_t)

    if ok:
        reason = f"cluster build {installed_t} >= JIRA minimum product build {required_t}"
    else:
        reason = (
            f"cluster build {installed_t} is lower than JIRA product build "
            f"{required_t} — verification blocked"
        )

    return {
        "check_applied": True,
        "proceed": ok,
        "reason": reason,
        "jira_product_build_versions": labels,
        "jira_required_minimum": str(required_t),
        "cluster_installed": cluster_installed,
        "cluster_installed_parsed": str(installed_t),
        "version_mismatch": not ok,
    }


def merge_into_cluster_fit(
    art: Path, gate: dict[str, Any], *, cluster_reachable: bool
) -> dict:
    fit_path = art / "cluster-fit.json"
    fit: dict = {}
    if fit_path.is_file():
        fit = json.loads(fit_path.read_text())
    fit["build_version_check"] = gate
    fit["verify_proceed"] = bool(cluster_reachable and gate.get("proceed", True))
    if gate.get("version_mismatch"):
        fit["compatible"] = False
        fit["reason"] = gate.get("reason", "build version mismatch")
    fit_path.write_text(json.dumps(fit, indent=2) + "\n")
    return fit


def main() -> None:
    parser = argparse.ArgumentParser(description="JIRA vs cluster build version gate")
    parser.add_argument("--art", type=Path, required=True)
    parser.add_argument(
        "--cluster-version", required=True, help="ODF CSV spec.version on cluster"
    )
    parser.add_argument("--cluster-reachable", choices=("0", "1"), default="1")
    args = parser.parse_args()

    raw = None
    raw_path = args.art / "jira-raw.json"
    if raw_path.is_file():
        raw = json.loads(raw_path.read_text())
    analysis = None
    ap = args.art / "analysis.json"
    if ap.is_file():
        analysis = json.loads(ap.read_text())

    jira_versions = extract_jira_product_build_versions(raw, analysis)
    gate = evaluate_build_version_gate(args.cluster_version, jira_versions)
    merge_into_cluster_fit(
        args.art,
        gate,
        cluster_reachable=args.cluster_reachable == "1",
    )

    print(json.dumps(gate, indent=2))
    if gate.get("check_applied") and not gate.get("proceed"):
        raise SystemExit(2)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
