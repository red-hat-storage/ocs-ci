import logging

from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to  filter out Stretch Cluster Arbiter tests

    Args:
        items: list of collected tests
    """

    if config.ENV_DATA.get("arbiter_deployment") is False:
        for item in items:
            if "disaster-recovery/sc_arbiter" in str(item.fspath):
                logger.debug(
                    f"Test {item} is removed from the collected items. Test runs only on Stretch clusters"
                )
                items.remove(item)
