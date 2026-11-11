import logging

from ocs_ci.framework import config
from ocs_ci.helpers.dr_helpers import is_fusion_access_dr
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)

FA_MDR_TEST_PATH = "disaster-recovery/metro-dr-fa/"


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out Fusion Access MDR tests.

    Fusion Access MDR reuses multicluster_mode "metro-dr", so the mode alone does
    not separate these tests from the ODF MDR ones. Both the mode and the Fusion
    Access backend have to match, otherwise the tests are dropped. Without this
    the tests would be collected on plain ODF MDR clusters, where the Scale
    resources they rely on do not exist.

    Args:
        items: list of collected tests

    """
    if (
        config.MULTICLUSTER.get("multicluster_mode") == constants.MDR_MODE
        and is_fusion_access_dr()
    ):
        return

    for item in items.copy():
        if FA_MDR_TEST_PATH in str(item.fspath):
            log.debug(
                f"Test {item} is removed from the collected items. "
                "Test runs only on Fusion Access MDR clusters"
            )
            items.remove(item)
