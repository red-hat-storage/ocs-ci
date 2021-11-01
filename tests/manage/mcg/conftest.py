import logging
from ocs_ci.framework import config
from ocs_ci.ocs.constants import (
    OPENSHIFT_DEDICATED_PLATFORM,
    ROSA_PLATFORM,
    CLOUD_PLATFORMS,
)

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out mcg tests
    when running on openshift dedicated platform

    Args:
        items: list of collected tests

    """
    # Need to update the condition when MCG will get supported
    if (
        config.ENV_DATA["platform"].lower() == OPENSHIFT_DEDICATED_PLATFORM
        or config.ENV_DATA["platform"].lower() == ROSA_PLATFORM
    ):
        for item in items.copy():
            if "manage/mcg" in str(item.fspath):
                log.info(
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
                log.info(
                    f"{item} will be skipped since cloud tests cannot be run on disconnected clusters"
                )
                items.remove(item)
