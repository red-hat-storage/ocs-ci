import pytest
import logging

from ocs_ci.framework.testlib import libtest
from tests.conftest import awscli_pod_fixture

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def awscli_pod_function(request):
    return awscli_pod_fixture(request, scope_name="function")


@libtest
def test_set_up_session_scope_awscli_pod(awscli_pod_session):
    logger.info("Setting up awscli resources leftovers for the next test")


@libtest
def test_set_up_function_scope_awscli_pod(awscli_pod_function):
    assert awscli_pod_function
    logger.info("The awscli_pod_fixture has succeeded despite the leftovers")
