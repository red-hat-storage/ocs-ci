"""Shared workflow config loader for pipeline and agent CLIs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from workflow_lib.loader import load_yaml

_CONFIG_DIR = Path(__file__).resolve().parent / "config"
DEFAULT_CONFIG_PATH = _CONFIG_DIR / "workflow.yaml"
EXAMPLE_CONFIG_PATH = _CONFIG_DIR / "workflow.example.yaml"

# Map agents.* keys to pipeline default keys used by issue_verification.yaml
_AGENT_DEFAULT_ALIASES: dict[str, dict[str, str]] = {
    "live_repro": {
        "dry_run": "live_repro_dry_run",
        "model": "live_repro_model",
        "max_turns": "live_repro_max_turns",
        "backend": "live_repro_backend",
        "oc_command_path": "oc_command_path",
        "skip_on_env_mismatch": "skip_on_env_mismatch",
    },
    "test_match": {
        "top_n": "top_n",
        "use_claude": "use_claude",
        "model": "claude_model",
    },
    "ocs_ci_run": {
        "dry_run": "dry_run",
        "tests_per_issue": "tests_per_issue",
        "run_teardown": "run_teardown",
    },
    "repro_steps": {
        "refresh_jira": "refresh_jira",
        "include_fix_prs": "include_fix_prs",
        "backend": "repro_steps_backend",
        "model": "repro_claude_model",
        "max_turns": "repro_claude_max_turns",
    },
}


def resolve_config_path(path: Path | str | None = None) -> Path | None:
    """Return an existing config path: explicit arg, then default workflow.yaml."""
    if path is not None:
        candidate = Path(path)
        if not candidate.is_file():
            raise FileNotFoundError(f"Workflow config not found: {candidate}")
        return candidate
    if DEFAULT_CONFIG_PATH.is_file():
        return DEFAULT_CONFIG_PATH
    return None


def _merge_agent_defaults(
    defaults: dict[str, Any],
    agents: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(defaults)
    for agent_name, aliases in _AGENT_DEFAULT_ALIASES.items():
        agent_cfg = agents.get(agent_name) or {}
        if not isinstance(agent_cfg, dict):
            continue
        for agent_key, default_key in aliases.items():
            if agent_key in agent_cfg:
                merged[default_key] = agent_cfg[agent_key]
    return merged


def _merge_issue_parameters(
    parameters: dict[str, Any],
    agents: dict[str, Any],
) -> dict[str, Any]:
    """Promote agents.jira_intake.issues into parameters when not set at top level."""
    merged = dict(parameters)
    if merged.get("issues"):
        return merged
    jira_intake = agents.get("jira_intake") or {}
    if isinstance(jira_intake, dict) and jira_intake.get("issues"):
        merged["issues"] = jira_intake["issues"]
    return merged


def load_workflow_config(path: Path | str | None = None) -> dict[str, Any]:
    """
    Load shared workflow config.

    Returns dict with pipeline, description, parameters, defaults, run, agents, auth.
    """
    config_path = resolve_config_path(path)
    if config_path is None:
        return {
            "pipeline": "issue_verification",
            "description": None,
            "parameters": {},
            "defaults": {},
            "run": {},
            "agents": {},
            "auth": {},
            "config_path": None,
        }

    data = load_yaml(config_path)
    parameters = dict(data.get("parameters") or {})
    defaults = dict(data.get("defaults") or {})
    agents = dict(data.get("agents") or {})
    parameters = _merge_issue_parameters(parameters, agents)
    defaults = _merge_agent_defaults(defaults, agents)

    return {
        "pipeline": data.get("pipeline") or "issue_verification",
        "description": data.get("description"),
        "parameters": parameters,
        "defaults": defaults,
        "run": dict(data.get("run") or {}),
        "agents": agents,
        "auth": dict(data.get("auth") or {}),
        "config_path": str(config_path),
    }


def to_run_config_format(config: dict[str, Any]) -> dict[str, Any]:
    """Format compatible with workflow_lib.run_config.load_run_config output."""
    return {
        "pipeline": config.get("pipeline"),
        "description": config.get("description"),
        "parameters": dict(config.get("parameters") or {}),
        "defaults": dict(config.get("defaults") or {}),
        "run": dict(config.get("run") or {}),
    }


def get_agent_settings(config: dict[str, Any], agent: str) -> dict[str, Any]:
    """Return per-agent settings from config (empty dict if unset)."""
    agents = config.get("agents") or {}
    section = agents.get(agent) or {}
    return dict(section) if isinstance(section, dict) else {}


def apply_config_to_namespace(
    args: Any,
    *,
    agent: str | None = None,
    config_path: Path | str | None = None,
    mappings: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Fill missing argparse Namespace fields from workflow config.

    mappings: argparse dest -> config dotted path (e.g. deploy_job_url -> parameters.deploy_job_url)
    """
    config = load_workflow_config(config_path)
    agent_cfg = get_agent_settings(config, agent) if agent else {}

    default_mappings = {
        "odf_version": "parameters.odf_version",
        "deploy_job_url": "parameters.deploy_job_url",
        "jira_config": "parameters.jira_config",
        "force_live_repro": "parameters.force_live_repro",
        "run_id": "run.run_id",
    }
    all_mappings = {**default_mappings, **(mappings or {})}

    for arg_name, dotted in all_mappings.items():
        if getattr(args, arg_name, None) not in (None, ""):
            continue
        value = _lookup_dotted(config, agent_cfg, dotted)
        if value is not None:
            setattr(args, arg_name, value)

    return config


def _lookup_dotted(
    config: dict[str, Any],
    agent_cfg: dict[str, Any],
    dotted: str,
) -> Any:
    if dotted.startswith("agents."):
        _, key = dotted.split(".", 1)
        return agent_cfg.get(key)
    parts = dotted.split(".")
    current: Any = config
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current
