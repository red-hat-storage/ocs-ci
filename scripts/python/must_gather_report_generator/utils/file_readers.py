"""File reading utilities and safe access for parsed must-gather documents."""

from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Defaults when YAML/JSON omits fields:
# UNKNOWN — generic missing string; NOT_AVAILABLE — optional IDs / N/A slots;
# ZERO — numeric counts; HEALTH_STATUS_UNKNOWN — Ceph/summary uppercase placeholder.
UNKNOWN = "Unknown"
NOT_AVAILABLE = "N/A"
ZERO = 0
HEALTH_STATUS_UNKNOWN = "UNKNOWN"


def items_or_empty(doc: Any) -> list:
    """Kubernetes-style ``items`` list, or empty if missing or wrong type."""
    if not isinstance(doc, dict):
        return []
    raw = doc.get("items")
    return raw if isinstance(raw, list) else []


def list_from(doc: Any, key: str) -> list:
    """Return ``doc[key]`` if it is a list; otherwise ``[]``."""
    if not isinstance(doc, dict):
        return []
    raw = doc.get(key)
    return raw if isinstance(raw, list) else []


def first_item(doc: Any) -> Any | None:
    """First element of ``items_or_empty(doc)``, or ``None``."""
    items = items_or_empty(doc)
    return items[0] if items else None


def read_yaml_file(filepath):
    """Read and parse YAML file. Returns None on missing file, read error, or parse error."""
    path = Path(filepath)
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except OSError as e:
        logger.warning("Cannot read YAML file %s: %s", path, e)
        return None
    except UnicodeDecodeError as e:
        logger.warning("Cannot decode YAML file %s: %s", path, e)
        return None
    except yaml.YAMLError as e:
        logger.warning("Invalid YAML in %s: %s", path, e)
        return None


def read_json_file(filepath):
    """Read and parse JSON file. Returns None on missing file, read error, or parse error."""
    path = Path(filepath)
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except OSError as e:
        logger.warning("Cannot read JSON file %s: %s", path, e)
        return None
    except UnicodeDecodeError as e:
        logger.warning("Cannot decode JSON file %s: %s", path, e)
        return None
    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON in %s: %s", path, e)
        return None


def read_file(filepath):
    """Read text file. Returns None on missing file or read/decode error."""
    path = Path(filepath)
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except OSError as e:
        logger.warning("Cannot read file %s: %s", path, e)
        return None
    except UnicodeDecodeError as e:
        logger.warning("Cannot decode file %s: %s", path, e)
        return None


def read_file_tail(filepath, max_lines: int = 50) -> str | None:
    """
    Last ``max_lines`` of a text file via ``tail -n`` (POSIX must-gather hosts).

    Returns None if ``tail`` fails; ``""`` if there is no output.
    """
    path = Path(filepath).resolve()
    try:
        proc = subprocess.run(
            ["tail", "-n", str(max_lines), str(path)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        logger.warning("tail failed for %s: %s", path, e)
        return None
    if proc.returncode != 0:
        logger.warning(
            "tail exited %s for %s: %s",
            proc.returncode,
            path,
            (proc.stderr or "").strip(),
        )
        return None
    return proc.stdout or ""


def find_must_gather_dir(base_path):
    """Find the actual must-gather data directory"""
    base = Path(base_path)

    # Look for quay.io directory pattern (more flexible matching)
    try:
        for item in base.iterdir():
            if item.is_dir() and (
                "quay" in item.name.lower() or "registry" in item.name.lower()
            ):
                print(f"Found must-gather data dir: {item.name}")
                return item
    except OSError as e:
        logger.warning("Could not iterate directory %s: %s", base, e)
        print(f"Warning: Could not iterate directory: {e}")

    # If no quay.io dir found, check if base has expected structure
    if (base / "ceph").exists() or (base / "namespaces").exists():
        print("Using base directory as must-gather root")
        return base

    print("Warning: Could not find must-gather data structure")
    return base


def find_external_ceph_dir(base_path):
    """
    Find the external ceph logs directory in the must-gather base path.

    Looks for directories matching pattern: external_ceph_logs_TIMESTAMP/ceph_external/ceph/

    Args:
        base_path: Path to must-gather base directory (parent of must-gather data dir)

    Returns:
        Path or None: Path to external ceph data directory (external_ceph_logs_*/ceph_external/ceph/)
                      or None if not found
    """
    base = Path(base_path)

    try:
        # Look for external_ceph_logs_* directories
        for item in base.iterdir():
            if item.is_dir() and item.name.startswith("external_ceph_logs_"):
                # Check for expected structure
                ceph_data_dir = item / "ceph_external" / "ceph"
                if ceph_data_dir.exists():
                    print(f"Found external ceph logs dir: {item.name}")
                    return ceph_data_dir
    except OSError as e:
        logger.warning("Could not search for external ceph logs in %s: %s", base, e)

    return None


def detect_deployment_type(mg_dir, external_ceph_dir=None):
    """
    Detect if this is internal (converged) or external Ceph deployment.

    Args:
        mg_dir: Path to must-gather data directory
        external_ceph_dir: Optional Path to external ceph directory (if already found)

    Returns:
        str: "internal", "external", or "unknown"
    """
    mg_dir = Path(mg_dir)

    # Check for internal Ceph indicators
    internal_ceph_commands = mg_dir / "ceph" / "must_gather_commands"
    internal_ceph_json = mg_dir / "ceph" / "must_gather_commands_json_output"

    # Check for external Ceph indicators
    # If external_ceph_dir was passed and exists, that's a strong indicator
    has_external_ceph_dir = (
        external_ceph_dir is not None and Path(external_ceph_dir).exists()
    )

    # Check StorageCluster for external storage config
    sc_file = mg_dir / "namespaces/openshift-storage/oc_output/storagecluster.yaml"
    has_external_in_sc = False
    if sc_file.exists():
        sc_data = read_yaml_file(sc_file)
        sc_item = first_item(sc_data)
        if sc_item:
            external_storage = sc_item.get("spec", {}).get("externalStorage", {})
            has_external_in_sc = external_storage.get("enable", False)

    # Detection logic
    if internal_ceph_commands.exists() or internal_ceph_json.exists():
        return "internal"
    elif has_external_ceph_dir or has_external_in_sc:
        return "external"
    else:
        return "unknown"
