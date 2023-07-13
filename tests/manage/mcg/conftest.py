import logging
from ocs_ci.framework import config
from ocs_ci.ocs.constants import CLOUD_PLATFORMS, MANAGED_SERVICE_PLATFORMS

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out mcg tests
    when running on openshift dedicated platform

    Args:
        items: list of collected tests

    """
    # Need to update the condition when MCG will get supported
    if config.ENV_DATA["platform"].lower() in MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if "manage/mcg" in str(item.fspath):
                log.debug(
                    f"Test {item} is removed from the collected items"
                    f" mcg is not supported on {config.ENV_DATA['platform'].lower()}"
                )
                items.remove(item)

    if config.DEPLOYMENT.get("disconnected"):
        for item in items.copy():
            if any(
                cloud_platform.upper() in item.name.upper()
                for cloud_platform in CLOUD_PLATFORMS
            ):
                log.debug(
                    f"{item} will be skipped since cloud tests cannot be run on disconnected clusters"
                )
                items.remove(item)
