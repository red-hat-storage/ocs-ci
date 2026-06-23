"""
Find ocs-ci automated tests that match issue reproduction and verification steps.

Uses the shared vector DB (``.claude/vectorDB/``) for semantic similarity search
against indexed test metadata. Test file parsing helpers remain for vector DB
indexing (``code_parser.py``).
"""

import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from coverage_mapper import (
    CODE_COVERAGE_AREAS,
    infer_issue_coverage_areas,
    infer_test_coverage_areas,
)

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass
class TestCandidate:
    """Indexed ocs-ci test metadata."""

    file_path: str
    test_name: str
    class_name: str | None
    docstring: str
    polarion_ids: list[str] = field(default_factory=list)
    jira_ids: list[str] = field(default_factory=list)
    search_text: str = ""
    coverage_areas: list[str] = field(default_factory=list)

    @property
    def node_id(self) -> str:
        if self.class_name:
            return f"{self.file_path}::{self.class_name}::{self.test_name}"
        return f"{self.file_path}::{self.test_name}"


def _extract_docstring_after_def(content: str, def_pos: int) -> str:
    """Extract docstring immediately following a def statement."""
    tail = content[def_pos:]
    match = re.search(
        r"def\s+\w+\([^)]*\)\s*(?:->[^:]{0,80})?:\s*(\"\"\"(.*?)\"\"\"|\'\'\'(.*?)\'\'\')",
        tail,
        re.DOTALL,
    )
    if not match:
        return ""
    return (match.group(2) or match.group(3) or "").strip()


def _parse_test_file(path: Path) -> list[TestCandidate]:
    """Parse a test file for test functions and metadata."""
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except OSError as exc:
        log.debug("Skipping unreadable file %s: %s", path, exc)
        return []

    rel_path = str(path.relative_to(REPO_ROOT))
    polarion_ids = re.findall(
        r"polarion_id\(\s*[\"']([^\"']+)[\"']\s*\)|"
        r"pytest\.mark\.polarion_id\(\s*[\"']([^\"']+)[\"']\s*\)",
        content,
    )
    file_polarion = [p[0] or p[1] for p in polarion_ids if p[0] or p[1]]
    file_jira = re.findall(r"@jira\(\s*[\"']([^\"']+)[\"']\s*\)", content)

    class_name = None
    class_match = re.search(r"^class\s+(Test\w+)", content, re.MULTILINE)
    if class_match:
        class_name = class_match.group(1)

    candidates: list[TestCandidate] = []
    for match in re.finditer(r"^(\s*)def\s+(test_\w+)\s*\(", content, re.MULTILINE):
        indent = len(match.group(1))
        test_name = match.group(2)
        if indent > 0:
            continue

        def_pos = match.start()
        docstring = _extract_docstring_after_def(content, def_pos)

        window_start = max(0, def_pos - 800)
        window = content[window_start:def_pos]
        test_jira = re.findall(r"@jira\(\s*[\"']([^\"']+)[\"']\s*\)", window)
        test_polarion = re.findall(
            r"polarion_id\(\s*[\"']([^\"']+)[\"']\s*\)|"
            r"pytest\.mark\.polarion_id\(\s*[\"']([^\"']+)[\"']\s*\)",
            window,
        )
        polarion = [p[0] or p[1] for p in test_polarion if p[0] or p[1]]

        search_text = " ".join(
            filter(
                None,
                [rel_path, test_name, class_name or "", docstring, " ".join(file_jira)],
            )
        ).lower()

        candidates.append(
            TestCandidate(
                file_path=rel_path,
                test_name=test_name,
                class_name=class_name,
                docstring=docstring,
                polarion_ids=polarion or file_polarion[:3],
                jira_ids=test_jira or file_jira,
                search_text=search_text,
                coverage_areas=infer_test_coverage_areas(rel_path, content),
            )
        )

    return candidates


def _vector_results_to_matches(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert vector DB search hits to the stage output format."""
    matches: list[dict[str, Any]] = []
    for item in results:
        coverage_areas = item.get("coverage_areas", [])
        area_labels = [
            CODE_COVERAGE_AREAS[a]["label"]
            for a in coverage_areas
            if a in CODE_COVERAGE_AREAS
        ]
        docstring = item.get("docstring", "")
        node_id = item.get("node_id") or ""
        matches.append(
            {
                "test_node_id": node_id,
                "file_path": item.get("file_path"),
                "test_name": item.get("test_name"),
                "class_name": item.get("class_name"),
                "polarion_ids": item.get("polarion_ids", []),
                "jira_ids": item.get("jira_ids", []),
                "coverage_areas": coverage_areas,
                "coverage_area_labels": area_labels,
                "relevance_score": int(round(item.get("score", 0) * 100)),
                "match_reasons": item.get("match_reasons", []),
                "docstring_excerpt": (
                    docstring[:300] + "..." if len(docstring) > 300 else docstring
                ),
                "pytest_command": item.get("pytest_command")
                or (f"pytest {node_id}" if node_id else None),
            }
        )
    return matches


def find_matching_tests_for_issue(
    issue: dict[str, Any],
    *,
    top_n: int = 10,
    min_score: int = 15,
    score_threshold: float | None = None,
    qdrant_url: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Find ocs-ci tests matching an issue's reproduction/verification plan.

    Queries the vector DB for semantically similar test cases.

    Args:
        issue (dict): Issue from run record (must include repro_steps stage)
        top_n (int): Maximum matches to return
        min_score (int): Minimum relevance score on 0-100 scale
        score_threshold (float | None): Cosine similarity threshold (default from min_score)
        qdrant_url (str | None): Optional remote Qdrant URL

    Returns:
        tuple: (ranked matching tests, issue coverage area metadata)

    """
    vector_db_dir = Path(__file__).resolve().parents[2] / "vectorDB"
    if str(vector_db_dir) not in sys.path:
        sys.path.insert(0, str(vector_db_dir))

    from retrieval import find_similar_tests  # noqa: E402

    issue_coverage = infer_issue_coverage_areas(issue)
    threshold = score_threshold if score_threshold is not None else min_score / 100

    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    query_parts = [
        issue.get("key", ""),
        issue.get("summary", ""),
        issue.get("description", ""),
        repro.get("issue_summary", ""),
        repro.get("expected_result", ""),
    ]
    query = " ".join(filter(None, query_parts))

    try:
        results = find_similar_tests(
            query,
            top_k=top_n,
            qdrant_url=qdrant_url,
            score_threshold=threshold,
            components=issue.get("components"),
            reproduction_steps=repro.get("reproduction_steps"),
            verification_steps=repro.get("verification_steps"),
            chunk_type="test",
        )
    except Exception as exc:
        log.error(
            "Vector DB test search failed for %s: %s",
            issue.get("key", ""),
            exc,
        )
        results = []

    matches = _vector_results_to_matches(results)

    if not matches:
        log.warning(
            "No vector DB matches for %s (indexed tests in .claude/vectorDB/?). "
            "Run: python .claude/vectorDB/vector_db_cli.py create",
            issue.get("key", ""),
        )

    return matches, issue_coverage


def run_test_matching_stage(
    issues: list[dict[str, Any]],
    *,
    top_n: int = 10,
) -> dict[str, dict[str, Any]]:
    """
    Stage 3: find matching ocs-ci tests for all issues in the run record.

    Args:
        issues (list): Issues from run record (repro_steps stage required)
        top_n (int): Max matches per issue

    Returns:
        dict: issue_key -> stage data for append_stage_bulk

    """
    per_issue: dict[str, dict[str, Any]] = {}

    for issue in issues:
        key = issue.get("key")
        if not key:
            continue

        repro_stage = issue.get("stages", {}).get("repro_steps")
        if not repro_stage or repro_stage.get("status") != "completed":
            log.warning(
                "Issue %s missing completed repro_steps stage; matching with intake data only",
                key,
            )

        matches, issue_coverage = find_matching_tests_for_issue(issue, top_n=top_n)
        per_issue[key] = {
            "issue_id": key,
            "issue_summary": issue.get("summary", ""),
            "issue_coverage_areas": issue_coverage,
            "matcher": "vector_db",
            "matching_test_count": len(matches),
            "matching_tests": matches,
            "analysis_notes": (
                "Tests ranked by semantic similarity via the ocs-ci vector DB "
                "(reproduction/verification steps, summary, components, coverage areas). "
                "Review top matches before selecting regression scope."
            ),
        }
        log.info("Found %d matching tests for %s", len(matches), key)

    return per_issue
