import logging
from ocs_ci.framework import config
from ocs_ci.ocs.constants import ON_PREM_PLATFORMS

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out RGW tests
    when running on cloud platforms

    Args:
        items: list of collected tests

    """
    if config.ENV_DATA['platform'].lower() not in ON_PREM_PLATFORMS:
        for item in items.copy():
            if 'manage/rgw' in str(item.fspath):
                log.info(
                    f"Test {item} is removed from the collected items"
                    f" due to {config.ENV_DATA['platform'].lower()} not being an on-prem platform"
                )
                items.remove(item)
