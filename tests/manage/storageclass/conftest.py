import logging
from ocs_ci.framework import config
from ocs_ci.ocs.constants import MANAGED_SERVICE_PLATFORMS

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out storageclass tests
    when running on managed service platforms

    Args:
        items: list of collected tests

    """
    if config.ENV_DATA["platform"].lower() in MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if "manage/storageclass" in str(item.fspath):
                log.debug(
                    f"Test {item} is removed from the collected items"
                    f" New storage-class creation is not supported on"
                    f" {config.ENV_DATA['platform'].lower()}"
                )
                items.remove(item)
