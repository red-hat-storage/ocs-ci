import logging
from ocs_ci.framework import config
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out MDR tests

    Args:
        items: list of collected tests

    """
    client_cluster_available = config.hci_client_exist()
    if config.MULTICLUSTER.get("multicluster_mode") != constants.MDR_MODE:
        for item in items.copy():
            if "disaster-recovery/metro-dr" in str(item.fspath):
                log.debug(
                    f"Test {item} is removed from the collected items. Test runs only on MDR clusters"
                )
                items.remove(item)
    else:
        for item in items.copy():
            if (
                "disaster-recovery/metro-dr/" in str(item.fspath)
                and client_cluster_available
            ):
                log.debug(
                    f"Test {item} is removed from the collected items. Test does not run on client MDR clusters"
                )
                items.remove(item)
