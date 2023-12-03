import pytest
import logging

from ocs_ci.framework.testlib import libtest
from tests.conftest import awscli_pod_fixture


@pytest.fixture(scope="function")
def awscli_pod_function(request):
    return awscli_pod_fixture(request, scope_name="function")


@libtest
def test_set_up_session_awscli_pod(awscli_pod_session):
    logging.info("Setting up awscli resources leftovers for the next test")


@libtest
def test_set_up_function_awscli_pod(awscli_pod_function):
    assert awscli_pod_function
    logging.info("The awscli_pod_fixture has succeeded despite the leftovers")
