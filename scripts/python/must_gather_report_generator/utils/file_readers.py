"""File reading utilities"""

import json
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


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
