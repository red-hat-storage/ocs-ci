import logging
from ocs_ci.framework import config
from ocs_ci.ocs.constants import OPENSHIFT_DEDICATED_PLATFORM, ROSA_PLATFORM

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to skip certain tests when running on
    openshift dedicated platform
    Args:
        items: list of collected tests
    """
    # Skip the below test till node implementaion completed for ODF-MS platform
    skip_till_node_implement = [
        "test_registry_reboot_node",
        "test_registry_rolling_reboot_node",
        "test_registry_shutdown_and_recovery_node",
    ]
    if (
        config.ENV_DATA["platform"].lower() == OPENSHIFT_DEDICATED_PLATFORM
        or config.ENV_DATA["platform"].lower() == ROSA_PLATFORM
    ):
        for item in items.copy():
            for testname in skip_till_node_implement:
                if testname in str(item):
                    log.info(
                        f"Test {item} is removed from the collected items"
                        f" till node implementation is in place"
                    )
                    items.remove(item)
                    break
