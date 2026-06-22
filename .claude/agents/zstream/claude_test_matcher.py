"""Backward-compatible re-exports. Prefer ``ocs_ci_test_match.claude_matcher``."""

import sys
from pathlib import Path

_AGENT = Path(__file__).resolve().parent.parent / "ocs_ci_test_match"
if str(_AGENT) not in sys.path:
    sys.path.insert(0, str(_AGENT))

from claude_matcher import (  # noqa: F401
    MATCH_TESTS_OUTPUT_SCHEMA,
    MATCHER_CLAUDE_AGENT,
    STAGE_TEST_MATCHING_CLAUDE,
    build_match_tests_prompt,
    match_tests_with_claude_agent,
    match_tests_with_claude_agent_sync,
    run_test_matching_claude_stage,
)
