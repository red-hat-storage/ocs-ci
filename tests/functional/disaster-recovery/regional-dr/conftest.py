import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_ceph_block_pool
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
    Uses storage class factory fixture to create a custom RBD storage class which uses a custom block pool
    with replica-2 to be used by CNV discovered applications

    Returns:
        all_clusters_success (bool): True if custom SC is found or created on both the managed clusters, False otherwise

    """
    custom_sc = getattr(request, "param", False)

    if not custom_sc:
        log.info("Skipping custom SC creation as request.param is not set.")
        yield True
        return

    pool_name = "rdr-test-storage-pool-2way"
    replica_count = 2
    pool_instances = []

    try:
        # Create pools in all clusters
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            pool = create_ceph_block_pool(
                pool_name=pool_name,
                replica=replica_count,
                verify=True,
            )
            pool_instances.append((cluster, pool))

        sc_name = "rbd-cnv-custom-sc-r2"
        all_clusters_success = True

        # Create or verify SC in all clusters
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            existing_sc_list = get_all_storageclass()

            if sc_name in existing_sc_list:
                log.info(f"Storage class {sc_name} already exists")
            else:
                try:
                    sc_obj = storageclass_factory(
                        sc_name=sc_name,
                        new_rbd_pool=False,
                        pool_name=pool.name,
                    )
                    assert sc_obj.name == sc_name, f"[Failed to create SC '{sc_name}']"
                    log.info(f"Successfully created custom RBD SC: {sc_name}")
                except Exception as e:
                    log.error(f"Error creating SC '{sc_name}': {e}")
                    all_clusters_success = False

        config.reset_ctx()
        yield all_clusters_success

    finally:
        for cluster, pool in pool_instances:
            try:
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                pool.delete(force=True)
                pool.ocp.wait_for_delete(pool.name)
                log.info(f"Deleted pool {pool.name} in cluster {cluster.name}")
            except Exception as e:
                log.warning(
                    f"Failed to delete pool {pool.name} in cluster {cluster.name}: {e}"
                )
        config.reset_ctx()
