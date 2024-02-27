import logging

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.utility import version

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    Skip tests in a directory based on conditions

    Args:
        items: list of collected tests

    """
    ocs_version = version.get_semantic_ocs_version_from_config()

    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if "functional/pv/pvc_snapshot" in str(item.fspath) and (
                ocs_version < version.VERSION_4_11
            ):
                log.debug(
                    f"Test {item} is removed from the collected items. PVC snapshot is not supported on"
                    f" {config.ENV_DATA['platform'].lower()} with ODF < 4.11 due to the bug 2069367"
                )
                items.remove(item)
