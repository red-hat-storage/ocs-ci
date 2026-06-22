"""Backward-compatible re-exports. Prefer ``ocs_ci_test_match.matcher``."""

import sys
from pathlib import Path

_AGENT = Path(__file__).resolve().parent.parent / "ocs_ci_test_match"
if str(_AGENT) not in sys.path:
    sys.path.insert(0, str(_AGENT))

from matcher import (  # noqa: F401
    TestCandidate,
    build_test_index,
    find_matching_tests_for_issue,
    run_test_matching_stage,
    _parse_test_file,
)
from models import STAGE_TEST_MATCHING  # noqa: F401
