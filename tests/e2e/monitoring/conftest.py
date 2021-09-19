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
    # Skip the below test till node implementation completed for ODF-MS platform
    skip_till_node_implement = [
        "test_monitoring_when_one_of_the_prometheus_node_down",
        "test_monitoring_after_rebooting_master_node",
        "test_monitoring_after_rebooting_node_where_mgr_is_running",
        "test_monitoring_shutdown_and_recovery_prometheus_node",
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
