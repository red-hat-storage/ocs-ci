"""
Live ODF cluster issue reproduction verification.

Backends:
- **claude-cli** (default): ``claude -p`` using Claude Code login — no API key
- **sdk**: ``claude-agent-sdk`` (also uses Claude Code credentials when logged in)

Authenticate once with ``claude login``. Kubeconfig is downloaded via ocs_ci_run.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from models import (
    SKIP_ENV_MISMATCH,
    SKIP_MISSING_REPRO,
    SKIP_NO_CLUSTER,
    VERDICT_FIXED,
    VERDICT_INCONCLUSIVE,
    VERDICT_NOT_FIXED,
    VERDICT_SKIPPED,
    VERIFIER_LIVE,
)

VerificationBackend = Literal["auto", "claude-cli", "sdk"]

log = logging.getLogger(__name__)


def _sdk_is_available() -> bool:
    try:
        import claude_agent_sdk  # noqa: F401
    except ImportError:
        return False
    return True


PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
_AGENT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _AGENT_DIR.parents[2]
_DEFAULT_WORK_ROOT = _REPO_ROOT / "_ocs_ci_live_repro"

VERIFY_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "issue_id": {"type": "string"},
        "issue_reproduced": {
            "type": "string",
            "enum": ["Yes", "No", "Inconclusive"],
        },
        "verdict": {
            "type": "string",
            "enum": ["fixed", "not_fixed", "inconclusive"],
        },
        "reproduction_steps_summary": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "step": {"type": "string"},
                    "status": {"type": "string", "enum": ["Pass", "Fail"]},
                    "details": {"type": "string"},
                },
                "required": ["step", "status", "details"],
            },
        },
        "expected_results_validation": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "expected_result": {"type": "string"},
                    "status": {"type": "string", "enum": ["Pass", "Fail"]},
                    "observations": {"type": "string"},
                },
                "required": ["expected_result", "status", "observations"],
            },
        },
        "resources_created": {
            "type": "array",
            "items": {"type": "string"},
        },
        "cleanup_status": {
            "type": "object",
            "properties": {
                "all_deleted": {"type": "boolean"},
                "details": {"type": "string"},
            },
            "required": ["all_deleted", "details"],
        },
        "output_log_path": {"type": "string"},
        "conclusion": {"type": "string"},
        "analysis_notes": {"type": "string"},
    },
    "required": [
        "issue_id",
        "issue_reproduced",
        "verdict",
        "reproduction_steps_summary",
        "expected_results_validation",
        "resources_created",
        "cleanup_status",
        "conclusion",
    ],
}


def _load_prompt(name: str) -> str:
    return (PROMPTS_DIR / name).read_text(encoding="utf-8")


def _bullet_list(items: list[str]) -> str:
    return (
        "\n".join(f"{idx}. {item}" for idx, item in enumerate(items, start=1))
        if items
        else "1. (none)"
    )


def _verification_work_dir(
    issue_key: str,
    cluster_profile: dict[str, Any],
    *,
    work_root: Path | None = None,
) -> Path:
    """Directory for verification logs and artifacts."""
    build = (
        cluster_profile.get("source_job", {}).get("build_number")
        or cluster_profile.get("cluster_name")
        or "unknown"
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    root = work_root or _DEFAULT_WORK_ROOT
    return root / issue_key / str(build) / stamp


def build_verify_prompt(
    issue: dict[str, Any],
    *,
    cluster_profile: dict[str, Any],
    kubeconfig_path: str,
    oc_command_path: str = "oc",
    output_log_path: str,
) -> tuple[str, str]:
    """
    Build system and user prompts for live cluster verification.

    Returns:
        tuple[str, str]: (system_prompt, user_prompt)

    """
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    env = repro.get("environment_requirements", {})
    env_text = json.dumps(env, indent=2) if env else "(not specified)"

    user_prompt = _load_prompt("odf_issue_reproduction_verify_user.txt").format(
        issue_id=issue.get("key", ""),
        issue_summary=issue.get("summary", repro.get("issue_summary", "")),
        components=", ".join(issue.get("components", [])) or "(none)",
        cluster_name=cluster_profile.get("cluster_name") or "target-cluster",
        ocs_version=cluster_profile.get("ocs_version") or "unknown",
        ocp_version=cluster_profile.get("ocp_version") or "unknown",
        topology=", ".join(cluster_profile.get("topology_hints") or []) or "standard",
        kubeconfig_path=kubeconfig_path,
        oc_command_path=oc_command_path,
        environment_requirements=env_text,
        reproduction_steps=_bullet_list(repro.get("reproduction_steps", [])),
        expected_result=repro.get("expected_result", "(not specified)"),
        verification_steps=_bullet_list(repro.get("verification_steps", [])),
        output_log_path=output_log_path,
    )
    system_prompt = _load_prompt("odf_issue_reproduction_verify_system.txt")
    return system_prompt, user_prompt


def _extract_json_from_text(text: str) -> dict[str, Any]:
    text = text.strip()
    if not text:
        raise json.JSONDecodeError("empty response", text, 0)

    fence = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fence:
        text = fence.group(1)
    else:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start : end + 1]
        else:
            raise json.JSONDecodeError("no JSON object found in response", text, 0)
    return json.loads(text)


def _parse_structured_response(messages: list[Any]) -> dict[str, Any] | None:
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    for message in reversed(messages):
        if isinstance(message, ResultMessage):
            structured = getattr(message, "structured_output", None)
            if isinstance(structured, dict):
                return structured
            if isinstance(structured, str) and structured.strip():
                try:
                    return _extract_json_from_text(structured)
                except json.JSONDecodeError:
                    pass
            if message.result:
                try:
                    return _extract_json_from_text(message.result)
                except json.JSONDecodeError:
                    pass

    last_assistant = ""
    for message in messages:
        if isinstance(message, AssistantMessage):
            parts = [
                block.text
                for block in message.content
                if isinstance(block, TextBlock) and block.text
            ]
            if parts:
                last_assistant = "\n".join(parts)

    if last_assistant:
        try:
            return _extract_json_from_text(last_assistant)
        except json.JSONDecodeError:
            pass
    return None


def _collect_analysis_text(messages: list[Any]) -> str:
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    for message in reversed(messages):
        if isinstance(message, ResultMessage) and message.result:
            return message.result.strip()

    last_assistant = ""
    for message in messages:
        if isinstance(message, AssistantMessage):
            parts = [
                block.text
                for block in message.content
                if isinstance(block, TextBlock) and block.text
            ]
            if parts:
                last_assistant = "\n".join(parts)
    return last_assistant.strip()


def _normalize_verdict(parsed: dict[str, Any]) -> dict[str, Any]:
    """Map issue_reproduced to verdict when the model omitted verdict."""
    reproduced = str(parsed.get("issue_reproduced", "")).strip()
    if not parsed.get("verdict"):
        mapping = {
            "Yes": VERDICT_NOT_FIXED,
            "No": VERDICT_FIXED,
            "Inconclusive": VERDICT_INCONCLUSIVE,
        }
        parsed["verdict"] = mapping.get(reproduced, VERDICT_INCONCLUSIVE)
    return parsed


async def _format_analysis_as_json(
    analysis: str,
    issue_id: str,
    cwd: Path,
    model: str | None,
) -> dict[str, Any]:
    from claude_agent_sdk import ClaudeAgentOptions, query

    if not analysis:
        raise RuntimeError("No analysis text to format as JSON")

    format_prompt = _load_prompt("odf_issue_reproduction_verify_format.txt").format(
        issue_id=issue_id,
        analysis=analysis[:30000],
    )
    options = ClaudeAgentOptions(
        system_prompt=(
            "You convert cluster verification reports into JSON. "
            "Return ONLY valid JSON matching the schema. No markdown, no explanation."
        ),
        cwd=str(cwd),
        allowed_tools=[],
        max_turns=1,
        output_format={
            "type": "json_schema",
            "schema": VERIFY_OUTPUT_SCHEMA,
        },
    )
    if model:
        options.model = model

    messages: list[Any] = []
    async for message in query(prompt=format_prompt, options=options):
        messages.append(message)

    parsed = _parse_structured_response(messages)
    if parsed is None:
        raw = _collect_analysis_text(messages)
        parsed = _extract_json_from_text(raw)
    return _normalize_verdict(parsed)


async def verify_issue_with_claude_agent(
    issue: dict[str, Any],
    *,
    cluster_profile: dict[str, Any],
    kubeconfig_path: str,
    oc_command_path: str = "oc",
    model: str | None = None,
    work_dir: Path | None = None,
    max_turns: int = 40,
) -> dict[str, Any]:
    """
    Run live cluster verification for one issue via claude-agent-sdk.

    Returns:
        dict: Stage data for run record (verdict, report tables, etc.)

    """
    try:
        from claude_agent_sdk import ClaudeAgentOptions, query
    except ImportError as exc:
        raise ImportError(
            "claude-agent-sdk is required for live cluster verification. "
            "Install with: pip install claude-agent-sdk"
        ) from exc

    issue_key = issue.get("key", "")
    verify_dir = work_dir or _verification_work_dir(issue_key, cluster_profile)
    verify_dir.mkdir(parents=True, exist_ok=True)
    output_log_path = str(verify_dir / "verification.log")

    system_prompt, user_prompt = build_verify_prompt(
        issue,
        cluster_profile=cluster_profile,
        kubeconfig_path=kubeconfig_path,
        oc_command_path=oc_command_path,
        output_log_path=output_log_path,
    )

    options = ClaudeAgentOptions(
        system_prompt=system_prompt,
        cwd=str(verify_dir),
        allowed_tools=["Bash", "Read"],
        max_turns=max_turns,
    )
    if model:
        options.model = model

    log.info(
        "Claude cluster verify phase 1 (live) for %s (kubeconfig=%s)",
        issue_key,
        kubeconfig_path,
    )
    research_messages: list[Any] = []
    async for message in query(prompt=user_prompt, options=options):
        research_messages.append(message)

    analysis = _collect_analysis_text(research_messages)
    if not analysis:
        raise RuntimeError(
            f"Claude agent returned no verification report for {issue_key}"
        )

    log.info("Claude cluster verify phase 2 (JSON format) for %s", issue_key)
    try:
        parsed = await _format_analysis_as_json(analysis, issue_key, verify_dir, model)
    except (json.JSONDecodeError, RuntimeError) as exc:
        log.warning(
            "JSON format phase failed for %s (%s); trying inline parse",
            issue_key,
            exc,
        )
        parsed = _parse_structured_response(research_messages)
        if parsed is None:
            raise RuntimeError(f"Invalid JSON from Claude agent: {exc}") from exc
        parsed = _normalize_verdict(parsed)

    parsed.setdefault("issue_id", issue_key)
    parsed.setdefault("output_log_path", output_log_path)
    parsed.setdefault("resources_created", [])
    parsed.setdefault("reproduction_steps_summary", [])
    parsed.setdefault("expected_results_validation", [])
    parsed.setdefault("cleanup_status", {"all_deleted": False, "details": ""})
    parsed["matcher"] = VERIFIER_LIVE
    parsed["backend"] = "sdk"
    parsed["dry_run"] = False
    parsed["issue_summary"] = issue.get("summary", "")
    parsed["verification_report"] = analysis
    parsed["work_dir"] = str(verify_dir)
    return parsed


def verify_issue_with_claude_agent_sync(
    issue: dict[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    """Synchronous wrapper for verify_issue_with_claude_agent."""
    return asyncio.run(verify_issue_with_claude_agent(issue, **kwargs))


def resolve_verification_backend(backend: VerificationBackend = "auto") -> str:
    """
    Pick live verification backend.

    ``auto`` prefers Claude Code CLI (``claude login``), then claude-agent-sdk.
    """
    if backend == "claude-cli":
        from claude_cli_verifier import is_claude_cli_available

        if not is_claude_cli_available():
            raise RuntimeError(
                "Claude Code CLI not available. Install Claude Code and run "
                "'claude login'."
            )
        return "claude-cli"

    if backend == "sdk":
        if not _sdk_is_available():
            raise RuntimeError(
                "claude-agent-sdk not installed. "
                "pip install -r .claude/agents/ocs_ci_live_repro/requirements-agent.txt"
            )
        return "sdk"

    from claude_cli_verifier import is_claude_cli_available

    if is_claude_cli_available():
        return "claude-cli"
    if _sdk_is_available():
        return "sdk"
    raise RuntimeError(
        "No Claude backend available. Install Claude Code and run 'claude login', "
        "or pip install claude-agent-sdk."
    )


def live_verify_issue(
    issue: dict[str, Any],
    *,
    cluster_profile: dict[str, Any],
    compatibility: dict[str, Any],
    skip_on_env_mismatch: bool = True,
    force: bool = False,
    oc_command_path: str = "oc",
    model: str | None = None,
    work_dir: Path | None = None,
    max_turns: int = 40,
    backend: VerificationBackend = "auto",
    permission_mode: str = "bypassPermissions",
    timeout: int = 3600,
) -> dict[str, Any]:
    """Run live cluster verification for one issue (Phase B)."""
    issue_key = issue.get("key", "")
    repro_stage = issue.get("stages", {}).get("repro_steps")

    if not repro_stage or repro_stage.get("status") != "completed":
        return {
            "stage_status": "skipped",
            "issue_id": issue_key,
            "verdict": VERDICT_SKIPPED,
            "skip_reason": SKIP_MISSING_REPRO,
            "matcher": VERIFIER_LIVE,
            "compatibility": compatibility,
            "cluster_profile": cluster_profile,
            "analysis_notes": "repro_steps stage must complete before cluster verification",
        }

    if not compatibility.get("compatible") and skip_on_env_mismatch and not force:
        return {
            "stage_status": "skipped",
            "issue_id": issue_key,
            "verdict": VERDICT_SKIPPED,
            "skip_reason": SKIP_ENV_MISMATCH,
            "matcher": VERIFIER_LIVE,
            "compatibility": compatibility,
            "cluster_profile": cluster_profile,
            "analysis_notes": (
                "Skipped: cluster environment does not match issue requirements. "
                "Use force=true to verify anyway."
            ),
        }

    kubeconfig_path = cluster_profile.get("kubeconfig_path")
    if not kubeconfig_path or not Path(kubeconfig_path).is_file():
        return {
            "stage_status": "failed",
            "issue_id": issue_key,
            "verdict": VERDICT_INCONCLUSIVE,
            "skip_reason": SKIP_NO_CLUSTER,
            "matcher": VERIFIER_LIVE,
            "compatibility": compatibility,
            "cluster_profile": cluster_profile,
            "analysis_notes": (
                "Kubeconfig not available. Ensure deploy_job_url resolves and "
                "Magna kubeconfig download succeeds."
            ),
        }

    try:
        resolved = resolve_verification_backend(backend)
        if resolved == "claude-cli":
            from claude_cli_verifier import verify_issue_with_claude_cli

            result = verify_issue_with_claude_cli(
                issue,
                cluster_profile=cluster_profile,
                kubeconfig_path=kubeconfig_path,
                oc_command_path=oc_command_path,
                model=model,
                work_dir=work_dir,
                permission_mode=permission_mode,
                timeout=timeout,
            )
        else:
            result = verify_issue_with_claude_agent_sync(
                issue,
                cluster_profile=cluster_profile,
                kubeconfig_path=kubeconfig_path,
                oc_command_path=oc_command_path,
                model=model,
                work_dir=work_dir,
                max_turns=max_turns,
            )
    except Exception as exc:
        log.error("Live cluster verification failed for %s: %s", issue_key, exc)
        return {
            "stage_status": "failed",
            "issue_id": issue_key,
            "verdict": VERDICT_INCONCLUSIVE,
            "matcher": VERIFIER_LIVE,
            "compatibility": compatibility,
            "cluster_profile": cluster_profile,
            "error": str(exc),
            "analysis_notes": f"Live verification failed: {exc}",
        }

    result["stage_status"] = "completed"
    result["compatibility"] = compatibility
    result["cluster_profile"] = cluster_profile
    return result
