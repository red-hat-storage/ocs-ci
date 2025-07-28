import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.deployment import acm
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
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


@pytest.fixture()
def cnv_custom_storage_class(request, storageclass_factory):
    """
    Uses storage class factory fixture to create a custom RBD storage class which and a custom block pool
    with replica-2 to be used by CNV discovered applications

    Returns:
        all_clusters_success (bool): True if custom SC is found or created on both the managed clusters, False otherwise

    """

    pool_name = "rdr-test-storage-pool-2way"
    sc_name = "rbd-cnv-custom-sc-r2"
    all_clusters_success_list = []

    for cluster in get_non_acm_cluster_config():
        config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
        # Create or verify existing SC in all clusters
        existing_sc_list = get_all_storageclass()
        current_cluster_sc_success = True
        sc_obj = None
        if sc_name in existing_sc_list:
            log.info(f"Storage class {sc_name} already exists")
        else:
            try:
                sc_obj = storageclass_factory(
                    sc_name=sc_name,
                    replica=2,
                    new_rbd_pool=True,
                    pool_name=pool_name,
                    mapOptions="krbd:rxbounce",
                )
                if sc_obj is None or sc_obj.name != sc_name:
                    log.error(
                        f"Failed to create SC '{sc_name}' or name mismatch: "
                        f"Created '{sc_obj.name if sc_obj else 'None'}'"
                    )
                    current_cluster_sc_success = False
                else:
                    log.info(f"Successfully created custom RBD SC: {sc_name}")
            except Exception as e:
                log.error(f"Error creating SC '{sc_name}': {e}")
                current_cluster_sc_success = False

        all_clusters_success_list.append(current_cluster_sc_success)
    config.reset_ctx()
    overall_all_clusters_success = all(all_clusters_success_list)
    yield overall_all_clusters_success
