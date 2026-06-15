"""
Find ocs-ci automated tests that match issue reproduction and verification steps.

Scans tests/ for test functions and scores them against each issue's
repro_steps stage output (summary, components, topology, verification steps)
and inferred code coverage areas (upstream repos, component families, test dirs).
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from coverage_mapper import (
    CODE_COVERAGE_AREAS,
    coverage_area_overlap_score,
    infer_issue_coverage_areas,
    infer_test_coverage_areas,
)

log = logging.getLogger(__name__)

STAGE_TEST_MATCHING = "test_matching"

REPO_ROOT = Path(__file__).resolve().parents[3]
TESTS_DIR = REPO_ROOT / "tests"

TOPOLOGY_TEST_DIRS: dict[str, list[str]] = {
    "standard_ipi": [
        "tests/functional/pv",
        "tests/functional/storageclass",
        "tests/functional/z_cluster",
        "tests/functional/object",
        "tests/functional/monitoring",
        "tests/functional/upgrade",
        "tests/functional/pod_and_daemons",
    ],
    "regional_dr": [
        "tests/functional/disaster-recovery/regional-dr",
        "tests/cross_functional/ui/test_odf_topology.py",
    ],
    "metro_dr": [
        "tests/functional/disaster-recovery/metro-dr",
        "tests/functional/disaster-recovery/sc_arbiter",
    ],
    "provider_client": [
        "tests/functional/provider_mode",
        "tests/functional/object/test_obc_deletion_client_provider.py",
    ],
    "external_mode": ["tests/functional/external_mode"],
    "lso_baremetal": ["tests/functional/lso"],
}

COMPONENT_KEYWORDS: dict[str, list[str]] = {
    "noobaa": ["noobaa", "mcg", "bucket", "obc", "namespace store", "s3"],
    "mcg": ["mcg", "noobaa", "bucket", "object bucket"],
    "rbd": ["rbd", "block", "pvc", "snapshot", "clone", "csi"],
    "cephfs": ["cephfs", "file", "pvc", "snapshot"],
    "ocs-operator": ["operator", "upgrade", "deployment", "storagecluster", "ocs"],
    "csi": ["csi", "pvc", "storageclass", "volume"],
    "dr": ["failover", "relocate", "ramen", "disaster", "regional", "metro"],
    "monitoring": ["alert", "prometheus", "monitoring", "metric"],
    "encryption": ["encrypt", "kms", "vault"],
}

STOPWORDS = frozenset(
    {
        "the",
        "and",
        "for",
        "with",
        "from",
        "that",
        "this",
        "issue",
        "test",
        "verify",
        "using",
        "after",
        "before",
        "should",
        "cluster",
        "openshift",
        "storage",
        "odf",
        "ocs",
    }
)


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


def _tokenize(text: str) -> set[str]:
    tokens = re.findall(r"[a-z0-9][a-z0-9_-]{2,}", text.lower())
    return {t for t in tokens if t not in STOPWORDS and len(t) > 2}


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
        # Skip nested helper functions inside tests (indented defs)
        if indent > 0:
            continue

        def_pos = match.start()
        docstring = _extract_docstring_after_def(content, def_pos)

        # Jira marks on the test (look at 15 lines before def)
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


def build_test_index(
    tests_dir: Path | None = None, *, max_files: int | None = None
) -> list[TestCandidate]:
    """
    Index all test_*.py files under tests/.

    Args:
        tests_dir (Path | None): Override tests root (default: repo tests/)
        max_files (int | None): Optional limit for debugging

    Returns:
        list[TestCandidate]: Indexed tests

    """
    root = tests_dir or TESTS_DIR
    if not root.is_dir():
        raise FileNotFoundError(f"Tests directory not found: {root}")

    index: list[TestCandidate] = []
    files = sorted(root.rglob("test_*.py"))
    if max_files:
        files = files[:max_files]

    for path in files:
        index.extend(_parse_test_file(path))

    log.info("Indexed %d tests from %d files under %s", len(index), len(files), root)
    return index


def _issue_search_corpus(issue: dict[str, Any]) -> tuple[str, set[str]]:
    """Build searchable text and tokens from issue + repro_steps stage."""
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    parts = [
        issue.get("key", ""),
        issue.get("summary", ""),
        issue.get("description", ""),
        " ".join(issue.get("components", [])),
        " ".join(issue.get("labels", [])),
        repro.get("issue_summary", ""),
        repro.get("topology", ""),
        repro.get("topology_details", ""),
        " ".join(repro.get("reproduction_steps", [])),
        " ".join(repro.get("verification_steps", [])),
        repro.get("expected_result", ""),
    ]
    env = repro.get("environment_requirements", {})
    if env:
        parts.append(str(env.get("topology_type", "")))
        parts.extend(env.get("prerequisites", []))

    text = " ".join(filter(None, parts)).lower()
    return text, _tokenize(text)


def _preferred_dirs(
    issue: dict[str, Any], issue_coverage: dict[str, Any] | None = None
) -> set[str]:
    """Return test directory prefixes likely relevant for this issue."""
    dirs: set[str] = set()
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    topology = repro.get("topology", "")
    if topology in TOPOLOGY_TEST_DIRS:
        dirs.update(TOPOLOGY_TEST_DIRS[topology])

    if issue_coverage:
        dirs.update(issue_coverage.get("preferred_test_dirs", []))

    corpus, _ = _issue_search_corpus(issue)
    for _component, keywords in COMPONENT_KEYWORDS.items():
        if any(kw in corpus for kw in keywords):
            for topo_dirs in TOPOLOGY_TEST_DIRS.values():
                for d in topo_dirs:
                    if any(kw in d for kw in keywords):
                        dirs.add(d)

    for component in issue.get("components", []):
        comp_lower = component.lower()
        for key, keywords in COMPONENT_KEYWORDS.items():
            if key in comp_lower or comp_lower in key:
                for topo_dirs in TOPOLOGY_TEST_DIRS.values():
                    for d in topo_dirs:
                        if any(kw in d for kw in keywords):
                            dirs.add(d)

    if not dirs:
        dirs.update(TOPOLOGY_TEST_DIRS["standard_ipi"])
    return dirs


def _score_test(
    issue_key: str,
    issue_tokens: set[str],
    issue_text: str,
    preferred_dirs: set[str],
    candidate: TestCandidate,
    issue_coverage_areas: list[str] | None = None,
) -> tuple[int, list[str]]:
    """Score a test candidate against an issue. Returns (score, reasons)."""
    score = 0
    reasons: list[str] = []

    if issue_coverage_areas and candidate.coverage_areas:
        area_score, area_reasons = coverage_area_overlap_score(
            issue_coverage_areas, candidate.coverage_areas
        )
        if area_score:
            score += area_score
            reasons.extend(area_reasons)

    if issue_key in candidate.jira_ids:
        score += 200
        reasons.append(f"direct @jira({issue_key}) link")

    if issue_key.lower() in candidate.search_text:
        score += 150
        reasons.append(f"mentions {issue_key} in test file")

    file_tokens = _tokenize(candidate.search_text)
    overlap = issue_tokens & file_tokens
    if overlap:
        overlap_score = min(len(overlap) * 8, 80)
        score += overlap_score
        sample = sorted(overlap)[:6]
        reasons.append(f"keyword overlap: {sample}")

    for pref in preferred_dirs:
        if candidate.file_path.startswith(pref):
            score += 25
            reasons.append(f"in relevant area: {pref}")
            break

    path_lower = candidate.file_path.lower()
    for token in issue_tokens:
        if len(token) > 4 and token in path_lower:
            score += 5

    for step_kw in (
        "audit",
        "404",
        "noobaa",
        "operator",
        "upgrade",
        "failover",
        "snapshot",
    ):
        if step_kw in issue_text and step_kw in candidate.search_text:
            score += 10
            reasons.append(f"verification keyword: {step_kw}")

    if candidate.docstring and len(candidate.docstring) > 40:
        doc_tokens = _tokenize(candidate.docstring)
        doc_overlap = issue_tokens & doc_tokens
        if doc_overlap:
            score += min(len(doc_overlap) * 5, 40)
            reasons.append(f"docstring overlap: {sorted(doc_overlap)[:4]}")

    return score, reasons


def find_matching_tests_for_issue(
    issue: dict[str, Any],
    test_index: list[TestCandidate],
    *,
    top_n: int = 10,
    min_score: int = 15,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Find ocs-ci tests matching an issue's reproduction/verification plan.

    Args:
        issue (dict): Issue from run record (must include repro_steps stage)
        test_index (list): Pre-built test index
        top_n (int): Maximum matches to return
        min_score (int): Minimum relevance score

    Returns:
        tuple: (ranked matching tests, issue coverage area metadata)

    """
    issue_key = issue.get("key", "")
    issue_text, issue_tokens = _issue_search_corpus(issue)
    issue_coverage = infer_issue_coverage_areas(issue)
    preferred_dirs = _preferred_dirs(issue, issue_coverage)

    ranked: list[tuple[int, TestCandidate, list[str]]] = []
    for candidate in test_index:
        score, reasons = _score_test(
            issue_key,
            issue_tokens,
            issue_text,
            preferred_dirs,
            candidate,
            issue_coverage_areas=issue_coverage.get("coverage_areas"),
        )
        if score >= min_score:
            ranked.append((score, candidate, reasons))

    ranked.sort(key=lambda item: item[0], reverse=True)

    matches = []
    for score, candidate, reasons in ranked[:top_n]:
        area_labels = [
            CODE_COVERAGE_AREAS[a]["label"]
            for a in candidate.coverage_areas
            if a in CODE_COVERAGE_AREAS
        ]
        matches.append(
            {
                "test_node_id": candidate.node_id,
                "file_path": candidate.file_path,
                "test_name": candidate.test_name,
                "class_name": candidate.class_name,
                "polarion_ids": candidate.polarion_ids,
                "jira_ids": candidate.jira_ids,
                "coverage_areas": candidate.coverage_areas,
                "coverage_area_labels": area_labels,
                "relevance_score": score,
                "match_reasons": reasons,
                "docstring_excerpt": (
                    candidate.docstring[:300] + "..."
                    if len(candidate.docstring) > 300
                    else candidate.docstring
                ),
                "pytest_command": f"pytest {candidate.node_id}",
            }
        )

    return matches, issue_coverage


def run_test_matching_stage(
    issues: list[dict[str, Any]],
    *,
    tests_dir: Path | None = None,
    top_n: int = 10,
) -> dict[str, dict[str, Any]]:
    """
    Stage 3: find matching ocs-ci tests for all issues in the run record.

    Args:
        issues (list): Issues from run record (repro_steps stage required)
        tests_dir (Path | None): Optional tests/ path override
        top_n (int): Max matches per issue

    Returns:
        dict: issue_key -> stage data for append_stage_bulk

    """
    test_index = build_test_index(tests_dir=tests_dir)
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

        matches, issue_coverage = find_matching_tests_for_issue(
            issue, test_index, top_n=top_n
        )
        per_issue[key] = {
            "issue_id": key,
            "issue_summary": issue.get("summary", ""),
            "issue_coverage_areas": issue_coverage,
            "matching_test_count": len(matches),
            "matching_tests": matches,
            "analysis_notes": (
                "Tests ranked by code coverage area overlap (upstream component/repo), "
                "JIRA link, keyword overlap with reproduction/verification steps, "
                "component/topology directory hints, and docstring similarity. "
                "Review top matches before selecting regression scope."
            ),
        }
        log.info("Found %d matching tests for %s", len(matches), key)

    return per_issue
