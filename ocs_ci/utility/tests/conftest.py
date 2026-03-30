"""
Pytest configuration for utility tests.
"""

import pytest
from ocs_ci.framework.logger_factory import set_log_record_factory


@pytest.fixture(scope="session", autouse=True)
def setup_logging():
    """
    Set up the custom log record factory for all tests.
    This ensures the 'clusterctx' attribute is available in log records.
    """
    set_log_record_factory()
