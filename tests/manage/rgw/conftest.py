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
    for item in items:
        if r'manage/rgw' in str(item.fspath):
            if config.ENV_DATA['platform'].lower() not in ON_PREM_PLATFORMS:
                log.info(
                    f'Test: {item} will be skipped due to not running on an on-prem platform'
                )
                items.remove(item)
