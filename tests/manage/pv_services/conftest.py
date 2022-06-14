import logging

from ocs_ci.ocs import constants
from ocs_ci.framework import config

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    Skip tests in a directory based on conditions

    Args:
        items: list of collected tests

    """
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if "manage/pv_services/pvc_snapshot" in str(item.fspath):
                log.info(
                    f"Test {item} is removed from the collected items. PVC snapshot is not supported on"
                    f" {config.ENV_DATA['platform'].lower()} due to the bug 2069367"
                )
                items.remove(item)
