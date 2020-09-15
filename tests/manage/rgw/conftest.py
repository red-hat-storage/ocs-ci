import logging
from ocs_ci.framework import config
from ocs_ci.ocs.constants import ON_PREM_PLATFORMS
from ocs_ci.utility.utils import skipif_upgraded_from, skipif_ocs_version

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out RGW tests
    when running on cloud platforms

    Args:
        items: list of collected tests

    """
    if (
        config.ENV_DATA['platform'].lower() not in ON_PREM_PLATFORMS
        or skipif_ocs_version('<4.5', reason='RGW not supported on OCS <4.5')
        or skipif_upgraded_from(
            ['4.2', '4.3', '4.4'],
            reason='RGW functionality blocked when upgrading to 4.5, BZ1873580'
        )
    ):
        for item in items.copy():
            if 'manage/rgw' in str(item.fspath):
                log.info(
                    f"Test {item} is removed from the collected items"
                    f" due to {config.ENV_DATA['platform'].lower()} not being an on-prem platform "
                    f"or OCS version ({config.ENV_DATA['ocs_version']}) being lower than 4.5"
                )
                items.remove(item)
