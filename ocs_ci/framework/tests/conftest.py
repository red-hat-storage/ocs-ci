# -*- coding: utf-8 -*-
"""
Pytest configuration for framework unit tests.

Sets up logging configuration required for testing the custom logger.
"""
import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_log_record_factory():
    """
    Set up the custom log record factory for all tests.

    This ensures that the 'clusterctx' field is added to all log records,
    which is expected by the pytest log format in pytest.ini.
    """
    from ocs_ci.framework.logger_factory import set_log_record_factory

    set_log_record_factory()
