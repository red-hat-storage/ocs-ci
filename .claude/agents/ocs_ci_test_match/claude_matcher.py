"""
Claude Agent SDK integration for matching ocs-ci tests to reproduction steps.

Uses claude-agent-sdk query() with Read/Glob/Grep to search tests/ and return
structured JSON matches. Requires:

    pip install claude-agent-sdk
    export ANTHROPIC_API_KEY=...

Or run from Claude Code with MCP/JIRA and agent tools enabled.
"""

import asyncio
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

_AGENT_DIR = Path(__file__).resolve().parent
if str(_AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENT_DIR))

from models import MATCHER_CLAUDE_AGENT

log = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
REPO_ROOT = Path(__file__).resolve().parents[3]

MATCH_TESTS_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "issue_id": {"type": "string"},
        "matching_test_count": {"type": "integer"},
        "matching_tests": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "test_node_id": {"type": "string"},
                    "file_path": {"type": "string"},
                    "test_name": {"type": "string"},
                    "class_name": {"type": ["string", "null"]},
                    "relevance_score": {"type": "number"},
                    "match_reasons": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "coverage_summary": {"type": "string"},
                    "coverage_areas": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "pytest_command": {"type": "string"},
                },
                "required": [
                    "test_node_id",
                    "file_path",
                    "test_name",
                    "relevance_score",
                    "match_reasons",
                ],
            },
        },
        "analysis_notes": {"type": "string"},
    },
    "required": ["issue_id", "matching_tests"],
}


def _load_prompt(name: str) -> str:
    path = PROMPTS_DIR / name
    return path.read_text(encoding="utf-8")


def _format_fix_pull_requests(issue: dict[str, Any]) -> str:
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    prs = repro.get("fix_pull_requests") or issue.get("fix_pull_requests") or []
    if not prs:
        return "(none)"
    lines = []
    for pr in prs[:5]:
        title = pr.get("title") or pr.get("url") or "?"
        lines.append(f"- {title} ({pr.get('url', '')})")
    return "\n".join(lines)


def build_match_tests_prompt(
    issue: dict[str, Any],
    *,
    heuristic_candidates: list[dict[str, Any]] | None = None,
    top_n: int = 10,
) -> tuple[str, str]:
    """
    Build system and user prompts for Claude test matching.

    Args:
        issue (dict): Issue from run record (with repro_steps stage)
        heuristic_candidates (list | None): Optional heuristic matches as hints
        top_n (int): Max tests to return

    Returns:
        tuple[str, str]: (system_prompt, user_prompt)

    """
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    env = repro.get("environment_requirements", {})

    def _bullet_list(items: list[str]) -> str:
        return "\n".join(f"- {item}" for item in items) if items else "- (none)"

    env_text = json.dumps(env, indent=2) if env else "(not specified)"

    user_prompt = _load_prompt("match_ocs_ci_tests_user.txt").format(
        issue_id=issue.get("key", ""),
        issue_summary=issue.get("summary", repro.get("issue_summary", "")),
        components=", ".join(issue.get("components", [])) or "(none)",
        labels=", ".join(issue.get("labels", [])) or "(none)",
        topology=repro.get("topology", "unknown"),
        topology_label=repro.get("topology_label", repro.get("topology", "unknown")),
        topology_details=repro.get("topology_details", ""),
        footprint=env.get("footprint", "unknown"),
        cluster_count=env.get("cluster_count", 1),
        environment_requirements=env_text,
        reproduction_steps=_bullet_list(repro.get("reproduction_steps", [])),
        expected_result=repro.get("expected_result", ""),
        verification_steps=_bullet_list(repro.get("verification_steps", [])),
        fix_pull_requests=_format_fix_pull_requests(issue),
        top_n=top_n,
    )
    system_prompt = _load_prompt("match_ocs_ci_tests_system.txt")
    return system_prompt, user_prompt


def _extract_json_from_text(text: str) -> dict[str, Any]:
    """Parse JSON from agent response, tolerating markdown fences and leading prose."""
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


def _parse_node_id(node_id: str) -> tuple[str, str | None, str]:
    """Split pytest node id into file_path, class_name, test_name."""
    parts = node_id.split("::")
    file_path = parts[0] if parts else node_id
    if len(parts) == 3:
        return file_path, parts[1], parts[2]
    if len(parts) == 2:
        return file_path, None, parts[1]
    test_name = Path(file_path).stem
    return file_path, None, test_name


def _normalize_match_payload(
    parsed: dict[str, Any],
    issue: dict[str, Any],
    *,
    matcher: str,
    analysis_notes: str = "",
    verification_report: str | None = None,
) -> dict[str, Any]:
    """Normalize Claude JSON into run-record test_matching stage data."""
    issue_key = issue.get("key", "")
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    matches_in = parsed.get("matching_tests") or []
    matches: list[dict[str, Any]] = []

    for item in matches_in:
        node_id = str(item.get("test_node_id") or "").strip()
        if not node_id:
            continue
        file_path, class_name, test_name = _parse_node_id(node_id)
        if not (REPO_ROOT / file_path).is_file():
            log.warning("Skipping non-existent test path from Claude: %s", node_id)
            continue
        matches.append(
            {
                "test_node_id": node_id,
                "file_path": item.get("file_path") or file_path,
                "test_name": item.get("test_name") or test_name,
                "class_name": item.get("class_name", class_name),
                "relevance_score": int(item.get("relevance_score", 0)),
                "match_reasons": list(item.get("match_reasons") or []),
                "coverage_summary": str(item.get("coverage_summary") or ""),
                "pytest_command": item.get("pytest_command") or f"pytest {node_id}",
            }
        )

    payload: dict[str, Any] = {
        "issue_id": parsed.get("issue_id") or issue_key,
        "issue_summary": issue.get("summary", repro.get("issue_summary", "")),
        "matcher": matcher,
        "matching_test_count": len(matches),
        "matching_tests": matches,
        "analysis_notes": analysis_notes or parsed.get("analysis_notes") or "",
        "verification_steps_used": repro.get("verification_steps") or [],
    }
    if verification_report:
        payload["matching_report"] = verification_report
    return payload


def _parse_structured_response(messages: list[Any]) -> dict[str, Any] | None:
    """Extract structured JSON from ResultMessage or parseable assistant text."""
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
    """Collect the final assistant analysis (prose) from a tool-using research phase."""
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


async def _format_analysis_as_json(
    analysis: str,
    issue_id: str,
    top_n: int,
    cwd: Path,
    model: str | None,
) -> dict[str, Any]:
    """Phase 2: convert research analysis to structured JSON (no tools)."""
    from claude_agent_sdk import ClaudeAgentOptions, query

    if not analysis:
        raise RuntimeError("No analysis text to format as JSON")

    format_prompt = _load_prompt("match_ocs_ci_tests_format.txt").format(
        issue_id=issue_id,
        top_n=top_n,
        analysis=analysis[:20000],
    )
    options = ClaudeAgentOptions(
        system_prompt=(
            "You convert test-matching analysis into JSON. "
            "Return ONLY valid JSON matching the schema. No markdown, no explanation."
        ),
        cwd=str(cwd),
        allowed_tools=[],
        max_turns=1,
        output_format={
            "type": "json_schema",
            "schema": MATCH_TESTS_OUTPUT_SCHEMA,
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
    return parsed


async def match_tests_with_claude_agent(
    issue: dict[str, Any],
    *,
    top_n: int = 10,
    model: str | None = None,
    repo_root: Path | None = None,
    max_turns: int = 25,
) -> dict[str, Any]:
    """
    Use claude-agent-sdk to find matching ocs-ci tests for one issue.

    Args:
        issue (dict): Issue from run record
        heuristic_candidates (list | None): Heuristic matches as search hints
        top_n (int): Max tests to return
        model (str | None): Claude model id
        repo_root (Path | None): ocs-ci repo root (cwd for agent)
        max_turns (int): Max agent turns

    Returns:
        dict: Stage data for run record (matching_tests, etc.)

    """
    try:
        from claude_agent_sdk import ClaudeAgentOptions, query
    except ImportError as exc:
        raise ImportError(
            "claude-agent-sdk is required for Claude test matching. "
            "Install with: pip install claude-agent-sdk"
        ) from exc

    system_prompt, user_prompt = build_match_tests_prompt(issue, top_n=top_n)
    cwd = repo_root or REPO_ROOT

    options = ClaudeAgentOptions(
        system_prompt=system_prompt,
        cwd=str(cwd),
        allowed_tools=["Read", "Glob", "Grep"],
        max_turns=max_turns,
    )
    if model:
        options.model = model

    issue_key = issue.get("key", "")
    log.info("Claude agent phase 1 (search) for %s (cwd=%s)", issue_key, cwd)
    research_messages: list[Any] = []
    async for message in query(prompt=user_prompt, options=options):
        research_messages.append(message)

    analysis = _collect_analysis_text(research_messages)
    if not analysis:
        raise RuntimeError(f"Claude agent returned no analysis for {issue_key}")

    log.info("Claude agent phase 2 (JSON format) for %s", issue_key)
    try:
        parsed = await _format_analysis_as_json(analysis, issue_key, top_n, cwd, model)
    except (json.JSONDecodeError, RuntimeError) as exc:
        log.warning(
            "JSON format phase failed for %s (%s); trying inline parse",
            issue_key,
            exc,
        )
        parsed = _parse_structured_response(research_messages)
        if parsed is None:
            log.error(
                "Failed to parse Claude response for %s: %s",
                issue_key,
                analysis[:500],
            )
            raise RuntimeError(f"Invalid JSON from Claude agent: {exc}") from exc

    parsed.setdefault("issue_id", issue.get("key", ""))
    return _normalize_match_payload(
        parsed,
        issue,
        matcher=MATCHER_CLAUDE_AGENT,
        analysis_notes=str(parsed.get("analysis_notes") or ""),
        verification_report=analysis,
    )


def match_tests_with_claude_agent_sync(
    issue: dict[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    """Synchronous wrapper for match_tests_with_claude_agent."""
    return asyncio.run(match_tests_with_claude_agent(issue, **kwargs))


def run_test_matching_claude_stage(
    issues: list[dict[str, Any]],
    *,
    top_n: int = 10,
    model: str | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Find matching tests for all issues using claude-agent-sdk (repo search agent).
    """
    per_issue: dict[str, dict[str, Any]] = {}

    for issue in issues:
        key = issue.get("key")
        if not key:
            continue

        try:
            stage_data = match_tests_with_claude_agent_sync(
                issue,
                top_n=top_n,
                model=model,
            )
            per_issue[key] = stage_data
            log.info(
                "Claude SDK found %d tests for %s",
                stage_data.get("matching_test_count", 0),
                key,
            )
        except Exception as exc:
            log.error("Claude SDK test matching failed for %s: %s", key, exc)
            per_issue[key] = {
                "issue_id": key,
                "issue_summary": issue.get("summary", ""),
                "matcher": MATCHER_CLAUDE_AGENT,
                "status": "failed",
                "error": str(exc),
                "matching_test_count": 0,
                "matching_tests": [],
            }

    return per_issue
