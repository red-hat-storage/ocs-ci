"""
Generate reproduction/verification steps via Claude (Rovo-quality analysis).

Uses JIRA description + comments + linked fix PRs as context. Backends:
- claude-cli (default): ``claude -p`` with Claude Code login
- sdk: claude-agent-sdk

There is no public Rovo REST API; Claude replaces Rovo with the same JIRA context.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Literal

log = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_WORK_ROOT = _REPO_ROOT / "tmp" / "repro_steps_claude"

ReproBackend = Literal["auto", "claude-cli", "sdk"]

REPRO_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "reproduction_steps": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
        },
        "verification_steps": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
        },
        "expected_result": {"type": "string"},
        "analysis_notes": {"type": "string"},
    },
    "required": ["reproduction_steps", "verification_steps", "expected_result"],
}

_CLAUDE_ENV_VARS = (
    "CLAUDECODE",
    "CLAUDE_CODE",
    "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_CODE_SSE_PORT",
)

GENERATOR_CLAUDE_CLI = "claude_code_cli"
GENERATOR_CLAUDE_SDK = "claude_agent_sdk"


def _load_prompt(name: str) -> str:
    return (_PROMPTS_DIR / name).read_text(encoding="utf-8")


def _build_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in _CLAUDE_ENV_VARS:
        env.pop(key, None)
    return env


def _resolve_claude_bin() -> str:
    path = shutil.which("claude", path=_build_env().get("PATH"))
    if not path:
        raise RuntimeError(
            "Claude Code CLI ('claude') not found. Install Claude Code, run "
            "'claude login', or set repro_steps_backend: sdk with claude-agent-sdk."
        )
    return path


def is_claude_cli_available() -> bool:
    try:
        claude_bin = _resolve_claude_bin()
    except RuntimeError:
        return False
    try:
        proc = subprocess.run(
            [claude_bin, "--version"],
            capture_output=True,
            timeout=15,
            env=_build_env(),
            stdin=subprocess.DEVNULL,
        )
        return proc.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _sdk_is_available() -> bool:
    try:
        import claude_agent_sdk  # noqa: F401
    except ImportError:
        return False
    return True


def _resolve_backend(backend: str) -> str:
    if backend == "claude-cli":
        return "claude-cli"
    if backend == "sdk":
        return "sdk"
    if is_claude_cli_available():
        return "claude-cli"
    if _sdk_is_available():
        return "sdk"
    raise RuntimeError(
        "Claude is required for reproduction steps. Install Claude Code CLI "
        "('claude login') or pip install claude-agent-sdk."
    )


def _format_comments(issue: dict[str, Any]) -> str:
    comments = issue.get("comments") or []
    if not comments:
        return "(no comments)"
    blocks: list[str] = []
    for idx, comment in enumerate(comments, start=1):
        author = comment.get("author") or "unknown"
        created = comment.get("created") or ""
        body = (comment.get("body") or "").strip()
        if not body:
            continue
        blocks.append(f"--- Comment {idx} ({author}, {created}) ---\n{body}")
    return "\n\n".join(blocks) if blocks else "(no comment text)"


def _format_pull_requests(issue: dict[str, Any]) -> str:
    pull_requests = issue.get("fix_pull_requests") or []
    if not pull_requests:
        return "(no linked fix pull requests found)"

    import importlib.util

    pr_context_path = (
        Path(__file__).resolve().parents[2] / "agents" / "ocs_ci_jira" / "pr_context.py"
    )
    spec = importlib.util.spec_from_file_location(
        "ocs_ci_jira_pr_context_fmt",
        pr_context_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load pr_context from {pr_context_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.format_pull_requests_for_prompt(pull_requests)


def build_repro_prompt(
    issue: dict[str, Any],
    *,
    target_odf_version: str,
    topology_info: dict[str, Any],
) -> tuple[str, str]:
    user_prompt = _load_prompt("repro_steps_user.txt").format(
        issue_id=issue.get("key", ""),
        issue_summary=issue.get("summary", ""),
        components=", ".join(issue.get("components", [])) or "(none)",
        labels=", ".join(issue.get("labels", [])) or "(none)",
        target_odf_version=target_odf_version,
        topology=topology_info.get("topology", ""),
        topology_label=topology_info.get("topology_label", ""),
        topology_details=topology_info.get("topology_details", ""),
        description=(issue.get("description") or "").strip() or "(empty)",
        comment_count=len(issue.get("comments") or []),
        comments_text=_format_comments(issue),
        fix_pr_count=len(issue.get("fix_pull_requests") or []),
        fix_prs_text=_format_pull_requests(issue),
    )
    system_prompt = _load_prompt("repro_steps_system.txt")
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


def _validate_repro_payload(payload: dict[str, Any]) -> dict[str, Any]:
    repro = payload.get("reproduction_steps")
    verify = payload.get("verification_steps")
    expected = payload.get("expected_result")
    if not isinstance(repro, list) or not repro:
        raise ValueError("Claude response missing non-empty reproduction_steps")
    if not isinstance(verify, list) or not verify:
        raise ValueError("Claude response missing non-empty verification_steps")
    if not expected or not str(expected).strip():
        raise ValueError("Claude response missing expected_result")
    return {
        "reproduction_steps": [str(s).strip() for s in repro if str(s).strip()],
        "verification_steps": [str(s).strip() for s in verify if str(s).strip()],
        "expected_result": str(expected).strip(),
        "analysis_notes": str(payload.get("analysis_notes") or "").strip(),
    }


def _run_claude_cli_prompt(
    system_prompt: str,
    user_prompt: str,
    *,
    model: str | None = None,
    cwd: Path,
    timeout: int = 600,
) -> dict[str, Any]:
    claude_bin = _resolve_claude_bin()
    cmd = [
        claude_bin,
        "-p",
        user_prompt,
        "--output-format",
        "json",
        "--permission-mode",
        "bypassPermissions",
        "--system-prompt",
        system_prompt,
    ]
    if model:
        cmd.extend(["--model", model])

    log.info("Running claude -p for repro steps (cwd=%s)", cwd)
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_build_env(),
        cwd=str(cwd),
        stdin=subprocess.DEVNULL,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Claude CLI failed (code {proc.returncode}): "
            f"{(proc.stderr or proc.stdout or '')[:800]}"
        )
    raw = (proc.stdout or "").strip()
    response = json.loads(raw)
    if response.get("is_error"):
        raise RuntimeError(response.get("result", "Claude CLI error"))
    return _validate_repro_payload(_extract_json_from_text(response.get("result", "")))


async def _run_claude_sdk_prompt(
    system_prompt: str,
    user_prompt: str,
    *,
    model: str | None = None,
    max_turns: int = 20,
) -> dict[str, Any]:
    from claude_agent_sdk import AssistantMessage, ClaudeAgentOptions, TextBlock, query

    options = ClaudeAgentOptions(
        system_prompt=system_prompt,
        max_turns=max_turns,
        model=model,
    )
    messages: list[Any] = []
    async for message in query(prompt=user_prompt, options=options):
        messages.append(message)

    for message in reversed(messages):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock) and block.text.strip():
                    return _validate_repro_payload(_extract_json_from_text(block.text))

    raise RuntimeError("Claude agent SDK returned no reproduction steps JSON")


def _run_claude_sdk_prompt_sync(
    system_prompt: str,
    user_prompt: str,
    *,
    model: str | None = None,
    max_turns: int = 20,
) -> dict[str, Any]:
    import asyncio

    return asyncio.run(
        _run_claude_sdk_prompt(
            system_prompt,
            user_prompt,
            model=model,
            max_turns=max_turns,
        )
    )


def generate_repro_steps_with_claude(
    issue: dict[str, Any],
    *,
    target_odf_version: str,
    topology_info: dict[str, Any],
    model: str | None = None,
    backend: str = "auto",
    max_turns: int = 20,
    work_root: Path | None = None,
) -> dict[str, Any]:
    """
    Generate reproduction/verification steps using Claude (mandatory).

    Returns dict with reproduction_steps, verification_steps, expected_result,
    analysis_notes, and generator metadata.
    """
    resolved = _resolve_backend(backend)
    system_prompt, user_prompt = build_repro_prompt(
        issue,
        target_odf_version=target_odf_version,
        topology_info=topology_info,
    )
    issue_key = issue.get("key", "unknown")
    work_dir = (work_root or _DEFAULT_WORK_ROOT) / issue_key / str(int(time.time()))
    work_dir.mkdir(parents=True, exist_ok=True)

    if resolved == "claude-cli":
        payload = _run_claude_cli_prompt(
            system_prompt,
            user_prompt,
            model=model,
            cwd=_REPO_ROOT,
        )
        generator = GENERATOR_CLAUDE_CLI
    else:
        payload = _run_claude_sdk_prompt_sync(
            system_prompt,
            user_prompt,
            model=model,
            max_turns=max_turns,
        )
        generator = GENERATOR_CLAUDE_SDK

    payload["generator"] = generator
    payload["rovo_equivalent"] = True
    log.info(
        "Claude generated %d repro + %d verification steps for %s (%s)",
        len(payload["reproduction_steps"]),
        len(payload["verification_steps"]),
        issue_key,
        generator,
    )
    return payload
