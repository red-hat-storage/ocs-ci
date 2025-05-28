import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.helpers.dr_helpers import check_mirroring_status_ok
from ocs_ci.ocs import constants
from ocs_ci.deployment import acm
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out RDR tests

    Args:
        items: list of collected tests

    """
    if config.MULTICLUSTER.get("multicluster_mode") != constants.RDR_MODE:
        for item in items.copy():
            if "disaster-recovery/regional-dr" in str(item.fspath):
                log.debug(
                    f"Test {item} is removed from the collected items. Test runs only on RDR clusters"
                )
                items.remove(item)


@pytest.fixture(autouse=True)
def check_subctl_cli():
    # Check whether subctl cli is present
    if config.MULTICLUSTER.get("multicluster_mode") != constants.RDR_MODE:
        return
    try:
        run_cmd("./bin/subctl")
    except (CommandFailed, FileNotFoundError):
        log.debug("subctl binary not found, downloading now...")
        submariner = acm.Submariner()
        submariner.download_binary()


@pytest.fixture(autouse=True)
def get_initial_mirror_replaying_count():
    for cluster_item in get_non_acm_cluster_config():
        config.switch_ctx(cluster_item.MULTICLUSTER["multicluster_index"])
        replaying_count = check_mirroring_status_ok(get_count=True)
        cluster_item.ENV_DATA["replaying_count"] = replaying_count
        log.info(f"Current mirror replaying count on cluster is {replaying_count}")
