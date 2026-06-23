"""Load agent modules without flat import name collisions (e.g. operations.py)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def load_agent_module(
    agent_dir: Path, module_file: str, unique_name: str
) -> ModuleType:
    """
    Load a Python file from an agent directory under a unique module name.

    The agent directory is prepended to ``sys.path`` so in-agent sibling imports
    (``from config import ...``) continue to work.
    """
    agent_dir_str = str(agent_dir.resolve())
    if agent_dir_str not in sys.path:
        sys.path.insert(0, agent_dir_str)

    module_path = agent_dir / module_file
    if not module_path.is_file():
        raise ImportError(f"Agent module not found: {module_path}")

    spec = importlib.util.spec_from_file_location(unique_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load agent module: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[unique_name] = module
    spec.loader.exec_module(module)
    return module
