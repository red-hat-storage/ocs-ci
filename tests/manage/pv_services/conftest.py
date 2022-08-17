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
        skip_snapshot_tests = [
            "manage/pv_services/pvc_snapshot/test_pvc_snapshot_when_snapshotclass_deleted.py",
            "manage/pv_services/pvc_snapshot/test_restore_snapshot_using_different_sc.py",
            "manage/pv_services/pvc_snapshot/test_verify_rbd_trash_purge.py",
        ]
        for item in items.copy():
            if "manage/pv_services/pvc_snapshot" in str(item.fspath):
                # Remove all tests in "manage/pv_services/pvc_snapshot/" if OCS version is below 4.11
                # If OCS version is 4.11 or above, remove only the tests given in the list 'skip_snapshot_tests'
                if (
                    all(
                        testpath not in str(item.fspath)
                        for testpath in skip_snapshot_tests
                    )
                    and ocs_version >= version.VERSION_4_11
                ):
                    continue
                log.debug(
                    f"Test {item} is removed from the collected items. PVC snapshot is not supported on"
                    f" {config.ENV_DATA['platform'].lower()} due to the bug 2069367"
                )
                items.remove(item)
