import logging
from ocs_ci.framework import config
from ocs_ci.ocs.constants import MANAGED_SERVICE_PLATFORMS

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
        "test_amq_after_rebooting_node",
        "test_amq_after_shutdown_and_recovery_worker_node",
        "test_run_couchbase_node_reboot",
        "test_run_jenkins_node_reboot",
        "test_run_pgsql_reboot_node",
    ]
    if config.ENV_DATA["platform"].lower() in MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            for testname in skip_till_node_implement:
                if testname in str(item):
                    log.info(
                        f"Test {item} is removed from the collected items"
                        f" till node implementation is in place"
                    )
                    items.remove(item)
                    break
