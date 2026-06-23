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
from pathlib import Path
from typing import Any

from coverage_mapper import infer_issue_coverage_areas
from models import MATCHER_CLAUDE_AGENT, MATCHER_VECTOR_DB_FALLBACK

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
    issue_coverage = infer_issue_coverage_areas(issue)

    def _bullet_list(items: list[str]) -> str:
        return "\n".join(f"- {item}" for item in items) if items else "- (none)"

    def _coverage_areas_text() -> str:
        lines = []
        for detail in issue_coverage.get("area_details", []):
            lines.append(
                f"- **{detail['label']}** (`{detail['area_id']}`): "
                f"upstream={', '.join(detail['upstream_repos'])}; "
                f"tests under {', '.join(detail['test_dirs'][:3])}"
            )
        if not lines:
            return "- (none inferred)"
        return "\n".join(lines)

    candidates_text = "(none — search tests/ from scratch)"
    if heuristic_candidates:
        lines = []
        for cand in heuristic_candidates[:15]:
            areas = ", ".join(cand.get("coverage_area_labels", [])[:2])
            area_hint = f"; areas={areas}" if areas else ""
            lines.append(
                f"- {cand.get('test_node_id')} (score={cand.get('relevance_score')}): "
                f"{', '.join(cand.get('match_reasons', [])[:2])}{area_hint}"
            )
        candidates_text = "\n".join(lines)

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
        heuristic_candidates=candidates_text,
        top_n=top_n,
        code_coverage_areas=_coverage_areas_text(),
        upstream_repos=", ".join(issue_coverage.get("upstream_repos", []))
        or "(unknown)",
        preferred_test_dirs="\n".join(
            f"- {d}" for d in issue_coverage.get("preferred_test_dirs", [])
        )
        or "- (none)",
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
    heuristic_candidates: list[dict[str, Any]] | None = None,
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

    system_prompt, user_prompt = build_match_tests_prompt(
        issue, heuristic_candidates=heuristic_candidates, top_n=top_n
    )
    issue_coverage = infer_issue_coverage_areas(issue)
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
    parsed.setdefault("matching_test_count", len(parsed.get("matching_tests", [])))
    parsed["matcher"] = MATCHER_CLAUDE_AGENT
    parsed["issue_summary"] = issue.get("summary", "")
    parsed["issue_coverage_areas"] = issue_coverage
    return parsed


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
    use_heuristic_hints: bool = True,
) -> dict[str, dict[str, Any]]:
    """
    Stage 3 (Claude): find matching tests for all issues using claude-agent-sdk.

    Optionally seeds Claude with vector DB candidates from matcher.py.

    Args:
        issues (list): Issues from run record
        top_n (int): Max matches per issue
        model (str | None): Claude model override
        use_heuristic_hints (bool): Pre-run vector DB matcher for candidate hints

    Returns:
        dict: issue_key -> stage data

    """
    from matcher import find_matching_tests_for_issue

    per_issue: dict[str, dict[str, Any]] = {}

    for issue in issues:
        key = issue.get("key")
        if not key:
            continue

        hints = None
        if use_heuristic_hints:
            hints, _ = find_matching_tests_for_issue(issue, top_n=15)

        try:
            stage_data = match_tests_with_claude_agent_sync(
                issue,
                heuristic_candidates=hints,
                top_n=top_n,
                model=model,
            )
            per_issue[key] = stage_data
            log.info(
                "Claude agent found %d tests for %s",
                stage_data.get("matching_test_count", 0),
                key,
            )
        except Exception as exc:
            log.error("Claude test matching failed for %s: %s", key, exc)
            # Fallback to vector DB matcher so the stage still produces results
            if use_heuristic_hints:
                log.info("Falling back to vector DB test matcher for %s", key)
                matches, issue_coverage = find_matching_tests_for_issue(
                    issue, top_n=top_n
                )
                per_issue[key] = {
                    "issue_id": key,
                    "issue_summary": issue.get("summary", ""),
                    "issue_coverage_areas": issue_coverage,
                    "matcher": MATCHER_VECTOR_DB_FALLBACK,
                    "claude_error": str(exc),
                    "matching_test_count": len(matches),
                    "matching_tests": matches,
                    "analysis_notes": (
                        "Claude agent failed; results from vector DB matcher. "
                        f"Error: {exc}"
                    ),
                }
            else:
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
