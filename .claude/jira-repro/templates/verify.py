"""DFBUGS verification template — customize per issue."""

from __future__ import annotations

import logging
import time

import pytest

log = logging.getLogger(__name__)

ISSUE_KEY = "DFBUGS-XXXX"


@pytest.fixture(scope="module")
def issue_context():
    log.info("Starting verification for %s", ISSUE_KEY)
    yield {}
    log.info("Cleanup for %s", ISSUE_KEY)


def test_verify_fix(issue_context):
    """Replace with steps from repro-steps.yaml."""
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            # TODO: implement reproduction / validation
            assert True
            return
        except AssertionError:
            if attempt == max_retries:
                raise
            log.warning("Attempt %s failed, retrying", attempt)
            time.sleep(10)
