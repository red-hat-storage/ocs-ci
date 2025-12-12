import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants
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
            # Create or verify existing SC in all clusters
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
                        time.sleep(60)
                except Exception as e:
                    log.error(f"Error creating SC '{sc_name}': {e}")
                    raise
        config.reset_ctx()

    return factory


@pytest.fixture()
def cephfs_custom_storage_class(request, storageclass_factory):
    """
    Uses storage class factory fixture to create a custom CephFS storage class and a custom
    cephfs pool with replica-2 to be used by discovered applications

    Raises Exception if the custom SC creation fails on any of the managed clusters
    """

    def factory(replica, compression):
        """
        Args:
            replica (int):  Replica count used in Pool creation
            compression (str): Type of compression to be used in the Pool, defaults to None

        """

        cephfs_pool_name = constants.RDR_CUSTOM_CEPHFS_POOL
        cephfs_sc_name = constants.RDR_CUSTOM_CEPHFS_STORAGECLASS

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            # Create or verify existing SC in all clusters
            existing_sc_list = get_all_storageclass()
            if cephfs_sc_name in existing_sc_list:
                log.info(f"Storage class {cephfs_sc_name} already exists")
            else:
                try:
                    sc_obj = storageclass_factory(
                        sc_name=cephfs_sc_name,
                        replica=replica,
                        compression=compression,
                        pool_name=cephfs_pool_name,
                    )
                    if sc_obj is None or sc_obj.name != cephfs_sc_name:
                        log.error(
                            f"Failed to create SC '{cephfs_sc_name}' or name mismatch: "
                            f"Created '{sc_obj.name if sc_obj else 'None'}'"
                        )
                    else:
                        log.info(
                            f"Successfully created custom CephFS SC: {cephfs_sc_name}"
                        )
                        time.sleep(60)

                except Exception as e:
                    log.error(f"Error creating SC '{cephfs_sc_name}': {e}")
                    raise
        config.reset_ctx()

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

    def _scale(status="down"):
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
        _scale("up")

    request.addfinalizer(teardown)
    return _scale
