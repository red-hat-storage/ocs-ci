"""Backward-compatible re-exports. Prefer ``ocs_ci_test_match.coverage_mapper``."""

import sys
from pathlib import Path

_AGENT = Path(__file__).resolve().parent.parent / "ocs_ci_test_match"
if str(_AGENT) not in sys.path:
    sys.path.insert(0, str(_AGENT))

from coverage_mapper import (  # noqa: F401
    CODE_COVERAGE_AREAS,
    JIRA_COMPONENT_TO_AREA,
    UPSTREAM_REPO_TO_AREA,
    coverage_area_overlap_score,
    infer_issue_coverage_areas,
    infer_test_coverage_areas,
)
