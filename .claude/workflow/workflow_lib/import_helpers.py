"""Load agent modules without flat import name collisions (e.g. operations.py)."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

# Flat module names reused across .claude/agents/* packages
_COLLIDING_MODULE_NAMES = frozenset(
    {
        "models",
        "operations",
        "config",
        "client",
        "parser",
        "jql",
        "compatibility",
        "cluster_context",
    }
)


def _module_file_path(mod: ModuleType) -> str | None:
    file_path = getattr(mod, "__file__", None)
    if not file_path:
        return None
    return str(Path(file_path).resolve())


def _stash_colliding_modules(agent_dir: Path) -> dict[str, ModuleType]:
    """Temporarily remove same-named modules from other agent directories."""
    agent_prefix = str(agent_dir.resolve()) + os.sep
    stashed: dict[str, ModuleType] = {}
    for name in _COLLIDING_MODULE_NAMES:
        mod = sys.modules.get(name)
        if mod is None:
            continue
        mod_path = _module_file_path(mod)
        if mod_path is None or mod_path.startswith(agent_prefix):
            continue
        stashed[name] = sys.modules.pop(name)
    return stashed


def _restore_stashed_modules(stashed: dict[str, ModuleType]) -> None:
    for name, mod in stashed.items():
        sys.modules[name] = mod


def _clear_agent_colliding_modules(agent_dir: Path) -> None:
    """Remove side-effect imports left in sys.modules by the agent load."""
    agent_prefix = str(agent_dir.resolve()) + os.sep
    for name in _COLLIDING_MODULE_NAMES:
        mod = sys.modules.get(name)
        if mod is None:
            continue
        mod_path = _module_file_path(mod)
        if mod_path and mod_path.startswith(agent_prefix):
            sys.modules.pop(name, None)


def ensure_agent_path(agent_dir: Path) -> None:
    """Prepend agent directory so sibling modules resolve at runtime."""
    agent_dir_str = str(agent_dir.resolve())
    if agent_dir_str not in sys.path:
        sys.path.insert(0, agent_dir_str)


def load_agent_module(
    agent_dir: Path, module_file: str, unique_name: str
) -> ModuleType:
    """
    Load a Python file from an agent directory under a unique module name.

    The agent directory is prepended to ``sys.path`` only for the duration of
    the load. Common flat names like ``models`` are stashed so agents do not
    shadow each other, then restored; collateral imports from the loaded agent
    are cleared afterward.
    """
    agent_dir = agent_dir.resolve()
    agent_dir_str = str(agent_dir)
    path_added = False
    if agent_dir_str not in sys.path:
        sys.path.insert(0, agent_dir_str)
        path_added = True

    module_path = agent_dir / module_file
    if not module_path.is_file():
        raise ImportError(f"Agent module not found: {module_path}")

    spec = importlib.util.spec_from_file_location(unique_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load agent module: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[unique_name] = module

    stashed = _stash_colliding_modules(agent_dir)
    try:
        spec.loader.exec_module(module)
    finally:
        _restore_stashed_modules(stashed)
        _clear_agent_colliding_modules(agent_dir)
        if path_added:
            sys.path.remove(agent_dir_str)

    return module
