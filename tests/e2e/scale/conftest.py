import logging
from ocs_ci.framework import config
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to skip certain tests when running on vSphere.

    Args:
        items: list of collected tests

    """
    skip_list = [
        "test_scale_osds_fill_75%_reboot_workers",
    ]
    if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
        for item in items.copy():
            for testname in skip_list:
                if testname in str(item.fspath):
                    log.info(
                        f"Test {item} is removed from the collected items"
                        f" since it does not run on vSphere"
                    )
                    items.remove(item)
                    break
