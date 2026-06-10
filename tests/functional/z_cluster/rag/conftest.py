"""
Pytest configuration and fixtures for OLS RAG tests.

This module provides fixtures for OLS test setup and teardown.
"""
import logging
import pytest

from ocs_ci.helpers.ols_helpers import cleanup_ols_operator

log = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def ols_cleanup(request):
    """
    Fixture to cleanup OLS operator resources after test class execution.

    This fixture runs after all tests in the TestRagImageDeploymentAndConfiguration
    class have completed. It ensures complete cleanup of:
    - OLSConfig custom resource
    - watsonx API secret
    - OLS operator subscription
    - OLS operator CSV
    - openshift-lightspeed namespace

    Usage:
        Add this fixture to the test class:
        @pytest.mark.usefixtures("ols_cleanup")
        class TestRagImageDeploymentAndConfiguration(ManageTest):
            ...

    Scope: class - runs once after all tests in the class complete
    """
    # Setup: nothing to do before tests
    yield

    # Teardown: cleanup after all tests in the class
    log.info("Running OLS operator cleanup after test class completion")
    try:
        cleanup_ols_operator()
        log.info("OLS operator cleanup completed successfully")
    except Exception as ex:
        log.error("Error during OLS operator cleanup: %s", ex)
        # Don't fail the test due to cleanup errors
