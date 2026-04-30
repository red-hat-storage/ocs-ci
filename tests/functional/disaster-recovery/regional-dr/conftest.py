import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.deployment import acm
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers import helpers

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
    Uses storage class factory fixture to create a custom RBD storage class and a custom block pool
    with replica-2 to be used by CNV discovered applications

    Raises Exception if the custom SC creation fails on any of the managed clusters

    """
    created = {"sc": False}

    def factory(replica, compression):
        """
        Args:
            replica (int):  Replica count used in Pool creation
            compression (str): Type of compression to be used in the Pool, defaults to None

        """

        pool_name = constants.RDR_CUSTOM_RBD_POOL
        sc_name = constants.RDR_CUSTOM_RBD_STORAGECLASS

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])

            # Wait for leftover pool from a previous run to finish deleting
            pool_ocp = ocp.OCP(
                kind=constants.CEPHBLOCKPOOL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            if pool_ocp.is_exist(resource_name=pool_name):
                log.info(
                    f"Pool {pool_name} exists (possibly terminating), "
                    f"waiting for deletion to complete"
                )
                try:
                    pool_ocp.wait_for_delete(resource_name=pool_name, timeout=300)
                except Exception:
                    log.warning(
                        f"Pool {pool_name} still exists after waiting, "
                        f"attempting to remove finalizers"
                    )
                    try:
                        pool_ocp.patch(
                            resource_name=pool_name,
                            params='{"metadata":{"finalizers":null}}',
                            format_type="merge",
                        )
                        pool_ocp.wait_for_delete(resource_name=pool_name, timeout=120)
                    except Exception:
                        log.error(f"Failed to clean up leftover pool {pool_name}")

            # Delete leftover SC if pool was cleaned up
            sc_ocp = ocp.OCP(kind=constants.STORAGECLASS)
            if sc_ocp.is_exist(resource_name=sc_name):
                if not pool_ocp.is_exist(resource_name=pool_name):
                    log.info(f"Deleting leftover StorageClass {sc_name}")
                    sc_ocp.delete(resource_name=sc_name)

            # Create or verify existing SC
            existing_sc_list = get_all_storageclass()
            if sc_name in existing_sc_list:
                log.info(f"Storage class {sc_name} already exists")
            else:
                try:
                    sc_obj = storageclass_factory(
                        sc_name=sc_name,
                        replica=replica,
                        compression=compression,
                        new_rbd_pool=True,
                        pool_name=pool_name,
                        mapOptions="krbd:rxbounce",
                    )
                    if sc_obj is None or sc_obj.name != sc_name:
                        log.error(
                            f"Failed to create SC '{sc_name}' or name mismatch: "
                            f"Created '{sc_obj.name if sc_obj else 'None'}'"
                        )
                    else:
                        log.info(f"Successfully created custom RBD SC: {sc_name}")
                        created["sc"] = True
                        time.sleep(60)
                except Exception as e:
                    log.error(f"Error creating SC '{sc_name}': {e}")
                    raise
        config.reset_ctx()

    def finalizer():
        pool_name = constants.RDR_CUSTOM_RBD_POOL
        sc_name = constants.RDR_CUSTOM_RBD_STORAGECLASS

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            try:
                sc_ocp = ocp.OCP(kind=constants.STORAGECLASS)
                if sc_ocp.is_exist(resource_name=sc_name):
                    log.info(f"Teardown: deleting StorageClass {sc_name}")
                    sc_ocp.delete(resource_name=sc_name)

                pool_ocp = ocp.OCP(
                    kind=constants.CEPHBLOCKPOOL,
                    namespace=config.ENV_DATA["cluster_namespace"],
                )
                if pool_ocp.is_exist(resource_name=pool_name):
                    log.info(f"Teardown: deleting CephBlockPool {pool_name}")
                    pool_ocp.delete(resource_name=pool_name)
                    try:
                        pool_ocp.wait_for_delete(resource_name=pool_name, timeout=300)
                    except Exception:
                        log.warning(
                            f"Teardown: pool {pool_name} stuck, " f"removing finalizers"
                        )
                        pool_ocp.patch(
                            resource_name=pool_name,
                            params='{"metadata":{"finalizers":null}}',
                            format_type="merge",
                        )
            except Exception as e:
                log.error(
                    f"Teardown: failed to clean up custom SC/pool "
                    f"on cluster {cluster.ENV_DATA.get('cluster_name')}: {e}"
                )
        config.reset_ctx()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture
def scale_deployments(request):
    """
    Fixture that allows scaling deployments down/up inside tests.
    Ensures deployments are scaled back up in finalizer no matter what.
    """
    deployments_to_scale = [
        {
            "name": constants.RBD_MIRROR_DAEMON_DEPLOYMENT,
            "namespace": constants.OPENSHIFT_STORAGE_NAMESPACE,
        },
        {
            "name": constants.MDS_DAEMON_DEPLOYMENT_ONE,
            "namespace": constants.OPENSHIFT_STORAGE_NAMESPACE,
        },
        {
            "name": constants.MDS_DAEMON_DEPLOYMENT_TWO,
            "namespace": constants.OPENSHIFT_STORAGE_NAMESPACE,
        },
        {
            "name": constants.ROOK_CEPH_OSD_ONE,
            "namespace": constants.OPENSHIFT_STORAGE_NAMESPACE,
        },
        {
            "name": constants.ROOK_CEPH_MGR_A,
            "namespace": constants.OPENSHIFT_STORAGE_NAMESPACE,
        },
        {
            "name": constants.NOOBAA_ENDPOINT_DEPLOYMENT,
            "namespace": constants.OPENSHIFT_STORAGE_NAMESPACE,
        },
        {
            "name": constants.NOOBAA_OPERATOR_DEPLOYMENT,
            "namespace": constants.OPENSHIFT_STORAGE_NAMESPACE,
        },
        {
            "name": constants.SUBMARINER_DEPLOYMENT,
            "namespace": constants.SUBMARINER_OPERATOR_NAMESPACE,
        },
        {
            "name": constants.SUBMARINER_LIGHTHOUSE_AGENT_DEPLOYMENT,
            "namespace": constants.SUBMARINER_OPERATOR_NAMESPACE,
        },
        {
            "name": constants.SUBMARINER_LIGHTHOUSE_COREDNS_DEPLOYMENT,
            "namespace": constants.SUBMARINER_OPERATOR_NAMESPACE,
        },
    ]

    cluster_name = []

    def _scale(status="down"):
        if status == "down":
            cluster_name.append(config.current_cluster_name())
        replica_count = 0 if status == "down" else 1
        for dep in deployments_to_scale:
            try:
                helpers.modify_deployment_replica_count(
                    deployment_name=dep["name"],
                    replica_count=replica_count,
                    namespace=dep["namespace"],
                )
                log.info(f"Scaled {dep['namespace']}/{dep['name']} to {replica_count}")

            except Exception as e:
                log.error(f"Failed scaling {dep['namespace']}/{dep['name']}: {e}")

    def teardown():
        log.info("Finalizer: scaling up deployments")
        if cluster_name:
            log.info(f"Switching to cluster '{cluster_name[0]}' before scaling up")
            config.switch_to_cluster_by_name(cluster_name[0])
        _scale("up")

    request.addfinalizer(teardown)
    return _scale
