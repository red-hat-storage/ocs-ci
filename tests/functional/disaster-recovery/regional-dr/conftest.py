import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.deployment import acm
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
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


@pytest.fixture()
def cnv_custom_storage_class(storageclass_factory):
    existing_sc_list = get_all_storageclass()
    cnv_custom_sc_name = "rbd-cnv-custom-sc-r2"
    if cnv_custom_sc_name in existing_sc_list:
        log.info(f"Storage class {cnv_custom_sc_name} already exists")
        return cnv_custom_sc_name
    cnv_custom_sc_name = storageclass_factory(
        sc_name="rbd-cnv-custom-sc-r2",
        replica=2,
        new_rbd_pool=True,
        pool_name="rdr-test-storage-pool-2way",
        mapOptions="krbd:rxbounce",
    )
    assert cnv_custom_sc_name.name == "rbd-cnv-custom-sc-r2", (
        "Custom RBD Storage Class creation using Custom Pool of Replica-2 for "
        "Discovered apps failed"
    )
    log.info(
        f"Custom RBD Storage Class creation using Custom Pool of Replica-2 for "
        f"Discovered apps succeeded, SC name is {cnv_custom_sc_name.name}"
    )
    return cnv_custom_sc_name
