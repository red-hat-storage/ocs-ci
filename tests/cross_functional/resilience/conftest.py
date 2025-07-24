import pytest
import os
import logging
from ocs_ci.ocs import constants
from ocs_ci.resiliency.resiliency_helper import ResiliencyConfig

log = logging.getLogger(__name__)


@pytest.fixture
def platfrom_failure_scenarios():
    """List Platform Failures scanarios"""
    PLATFORM_FAILURES_CONFIG_FILE = os.path.join(
        constants.RESILIENCY_DIR, "conf", "platform_failures.yaml"
    )
    data = ResiliencyConfig.load_yaml(PLATFORM_FAILURES_CONFIG_FILE)
    return data


@pytest.fixture
def storage_component_failure_scenarios():
    """List Platform Failures scanarios"""
    STORAGECLUSTER_COMPONENT_FAILURES_CONFIG_FILE = os.path.join(
        constants.RESILIENCY_DIR, "conf", "storagecluster_component_failures.yaml"
    )
    data = ResiliencyConfig.load_yaml(STORAGECLUSTER_COMPONENT_FAILURES_CONFIG_FILE)
    return data
