import logging
import platform
import os
from ocs_ci.utility import templating
import pytest

from ocs_ci.framework import config
from ocs_ci.helpers.virtctl import get_virtctl_tool
from ocs_ci.ocs import constants
from ocs_ci.deployment import acm
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import (
    exec_cmd,
    run_cmd,
)
from ocs_ci.helpers.dr_helpers import (
    apply_itms_to_managed_clusters,
    generate_rdr_mirror_images,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceNotFoundError,
    CephHealthException,
    CephHealthNotRecoveredException,
    CephHealthRecoveredException,
    UnexpectedDeploymentConfiguration,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.dr_helpers import (
    check_rbd_mirror_running,
    check_mirroring_status_ok,
)

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


@pytest.fixture(autouse=True, scope="session")
def check_subctl_cli():
    if config.MULTICLUSTER.get("multicluster_mode") != constants.RDR_MODE:
        return
    if platform.system() == "Darwin":
        log.warning("subctl binary is not available for macOS, skipping download")
        return
    try:
        run_cmd("./bin/subctl")
    except (CommandFailed, FileNotFoundError):
        log.debug("subctl binary not found, downloading now...")
        submariner = acm.Submariner()
        submariner.download_binary()


@pytest.fixture(autouse=True, scope="function")
def rdr_health_check():
    """
    Verify cluster health on both managed clusters before each RDR test.
    Checks Ceph health, rbd-mirror daemon status, and mirroring health.

    """
    if config.MULTICLUSTER.get("multicluster_mode") != constants.RDR_MODE:
        return

    if config.RUN["cli_params"].get("dev_mode"):
        log.info("Skipping RDR health checks for development mode")
        return

    restore_index = config.cur_index
    try:
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            if not config.RUN.get("cephcluster"):
                continue
            cluster_name = config.ENV_DATA.get("cluster_name")
            log.info(f"Running RDR health check on managed cluster: {cluster_name}")
            try:
                helpers.ceph_health_check_with_toolbox_recovery(
                    namespace=config.ENV_DATA["cluster_namespace"],
                    tries=5,
                    delay=10,
                )
                log.info(f"Ceph health check passed on {cluster_name}")
            except (CephHealthException, CephHealthNotRecoveredException) as e:
                log.error(f"Ceph health check failed on {cluster_name}: {e}")
                pytest.skip(f"Ceph health check failed on {cluster_name}")
            except CephHealthRecoveredException:
                log.warning(
                    f"Ceph health was not OK but recovered on {cluster_name}. "
                    "Proceeding with test execution."
                )
            try:
                check_rbd_mirror_running()
            except UnexpectedDeploymentConfiguration as e:
                log.error(f"rbd-mirror daemon check failed on {cluster_name}: {e}")
                pytest.skip(f"rbd-mirror daemon check failed on {cluster_name}")
            if not check_mirroring_status_ok():
                log.error(f"Mirroring health is not OK on {cluster_name}")
                pytest.skip(f"Mirroring health is not OK on {cluster_name}")
    finally:
        config.switch_ctx(restore_index)


@pytest.fixture(scope="session", autouse=True)
def get_virtctl():
    with config.RunWithProviderConfigContextIfAvailable():
        get_virtctl_tool()


@pytest.fixture()
def cnv_custom_storage_class(request, ceph_pool_factory, storageclass_factory):
    """
    Creates a custom CephBlockPool and RBD StorageClass on both managed
    clusters for CNV discovered-app DR tests.

    The flow ensures the CephBlockPoolRadosNamespace reaches Ready before
    the StorageClass is created so that Ramen assigns a unique
    groupreplicationID to the custom pool.

    Raises Exception if the custom pool/SC creation fails on any managed cluster.

    """

    def factory(replica, compression):
        """
        Args:
            replica (int): Replica count for the pool
            compression (str): Compression type for the pool, or None

        """
        from ocs_ci.ocs import ocp
        from ocs_ci.ocs.resources.pod import delete_pods, get_all_pods
        from ocs_ci.utility.utils import TimeoutSampler

        pool_name = constants.RDR_CUSTOM_RBD_POOL
        sc_name = constants.RDR_CUSTOM_RBD_STORAGECLASS

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            existing_sc_list = get_all_storageclass()
            if sc_name in existing_sc_list:
                log.info(f"Storage class {sc_name} already exists")
                continue
            pool_ocp = ocp.OCP(
                kind=constants.CEPHBLOCKPOOL,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=pool_name,
            )
            if pool_ocp.is_exist(resource_name=pool_name):
                log.info(f"Pool {pool_name} already exists, skipping creation")
            else:
                ceph_pool_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    replica=replica,
                    compression=compression,
                    pool_name=pool_name,
                )
                for sample in TimeoutSampler(600, 10, pool_ocp.get):
                    phase = sample.get("status", {}).get("phase") if sample else None
                    if phase == constants.STATUS_READY:
                        log.info(f"CephBlockPool {pool_name} is Ready")
                        break

        radosns_name = f"{pool_name}-builtin-implicit"
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            existing_sc_list = get_all_storageclass()
            if sc_name in existing_sc_list:
                continue
            namespace = config.ENV_DATA["cluster_namespace"]
            radosns_ocp = ocp.OCP(
                kind=constants.CEPHBLOCKPOOLRADOSNS,
                namespace=namespace,
                resource_name=radosns_name,
            )
            radosns_data = radosns_ocp.get()
            radosns_phase = (
                radosns_data.get("status", {}).get("phase") if radosns_data else None
            )
            if radosns_phase != constants.STATUS_READY:
                log.info(
                    "CephBlockPoolRadosNamespace %s is %s, "
                    "restarting ocs-operator to reset "
                    "MirroringController backoff",
                    radosns_name,
                    radosns_phase,
                )
                ocs_pods = get_all_pods(
                    namespace=namespace,
                    selector=["ocs-operator"],
                    selector_label="name",
                )
                delete_pods(ocs_pods)
                for sample in TimeoutSampler(600, 10, radosns_ocp.get):
                    radosns_phase = (
                        sample.get("status", {}).get("phase") if sample else None
                    )
                    log.info(
                        "CephBlockPoolRadosNamespace %s phase: %s",
                        radosns_name,
                        radosns_phase,
                    )
                    if radosns_phase == constants.STATUS_READY:
                        break
            else:
                log.info(
                    "CephBlockPoolRadosNamespace %s is already Ready",
                    radosns_name,
                )

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            existing_sc_list = get_all_storageclass()
            if sc_name in existing_sc_list:
                log.info(f"Storage class {sc_name} already exists")
                continue
            try:
                sc_obj = storageclass_factory(
                    sc_name=sc_name,
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
            except Exception as e:
                log.error(f"Error creating SC '{sc_name}': {e}")
                raise

        log.info(
            "Waiting for DRPolicy peerClasses to include %s",
            sc_name,
        )
        from ocs_ci.helpers.dr_helpers import get_all_drpolicy

        for sample in TimeoutSampler(600, 10, get_all_drpolicy):
            if not sample:
                continue
            drpolicy = sample[0]
            peer_classes = (
                drpolicy.get("status", {}).get("async", {}).get("peerClasses", [])
            )
            sc_names = [pc.get("storageClassName") for pc in peer_classes]
            log.info("DRPolicy peerClasses SCs: %s", sc_names)
            if sc_name in sc_names:
                log.info("DRPolicy peerClasses now includes %s", sc_name)
                break

        config.reset_ctx()

    def teardown():
        """
        Clean up custom pool and SC on all managed clusters.

        Order: delete SC first, then RadosNamespace, then pool.
        The pool cannot be deleted while it has images or dependents,
        so we wait with a longer timeout.

        """
        from ocs_ci.ocs import ocp
        from ocs_ci.ocs.resources.storage_cluster import (
            delete_storageclass_and_deregister,
        )

        pool_name = constants.RDR_CUSTOM_RBD_POOL
        sc_name = constants.RDR_CUSTOM_RBD_STORAGECLASS
        radosns_name = f"{pool_name}-builtin-implicit"

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            namespace = config.ENV_DATA["cluster_namespace"]
            cluster_name = config.ENV_DATA.get("cluster_name")

            try:
                sc_ocp = ocp.OCP(kind=constants.STORAGECLASS)
                if sc_ocp.is_exist(resource_name=sc_name):
                    log.info("Deleting SC %s on %s", sc_name, cluster_name)
                    delete_storageclass_and_deregister(sc_name=sc_name, sc_ocp=sc_ocp)
            except Exception as e:
                log.warning(
                    "Failed to delete SC %s on %s: %s",
                    sc_name,
                    cluster_name,
                    e,
                )

            try:
                radosns_ocp = ocp.OCP(
                    kind=constants.CEPHBLOCKPOOLRADOSNS,
                    namespace=namespace,
                    resource_name=radosns_name,
                )
                if radosns_ocp.is_exist(resource_name=radosns_name):
                    log.info(
                        "Deleting RadosNamespace %s on %s",
                        radosns_name,
                        cluster_name,
                    )
                    radosns_ocp.delete(resource_name=radosns_name)
                    radosns_ocp.wait_for_delete(radosns_name, timeout=300)
            except Exception as e:
                log.warning(
                    "Failed to delete RadosNamespace %s on %s: %s",
                    radosns_name,
                    cluster_name,
                    e,
                )

            try:
                pool_ocp = ocp.OCP(
                    kind=constants.CEPHBLOCKPOOL,
                    namespace=namespace,
                    resource_name=pool_name,
                )
                if pool_ocp.is_exist(resource_name=pool_name):
                    log.info(
                        "Deleting pool %s on %s",
                        pool_name,
                        cluster_name,
                    )
                    pool_ocp.delete(resource_name=pool_name)
                    pool_ocp.wait_for_delete(pool_name, timeout=300)
            except Exception as e:
                log.warning(
                    "Failed to delete pool %s on %s: %s",
                    pool_name,
                    cluster_name,
                    e,
                )

        config.reset_ctx()

    request.addfinalizer(teardown)
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


@pytest.fixture(scope="session", autouse=True)
def mirror_rdr_images():
    """
    Mirror RDR images to disconnected registry and apply ITMS to managed clusters.
    """
    if not config.DEPLOYMENT.get("disconnected"):
        return

    imageset_config_data = templating.load_yaml(constants.OC_MIRROR_IMAGESET_CONFIG_V2)

    # Get RDR images and add to additionalImages
    rdr_images = generate_rdr_mirror_images()
    if not rdr_images:
        log.warning("No RDR images found to mirror. Exiting function.")
        return

    imageset_config_data["mirror"]["additionalImages"] = rdr_images
    log.info(f"Added {len(rdr_images)} RDR images to mirror configuration")

    # Mirror required images
    log.info(
        f"Mirror required images to mirror registry {config.DEPLOYMENT['mirror_registry']}"
    )
    imageset_config_file = os.path.join(
        config.ENV_DATA["cluster_path"],
        f"imageset-config-{config.RUN['run_id']}.yaml",
    )
    templating.dump_data_to_temp_yaml(imageset_config_data, imageset_config_file)

    cmd = (
        f"oc mirror --config {imageset_config_file} "
        f"docker://{config.DEPLOYMENT['mirror_registry']} "
        "--workspace file://oc-mirror-workspace/results-files --v2"
    )

    try:
        exec_cmd(cmd, timeout=18000)
    except CommandFailed as e:
        # if itms is configured, the oc mirror command might fail (return non 0 rc),
        # but we want to continue to try to mirror the images manually with applied the itms rules
        log.warning(f"oc mirror command failed: {e}")
        # Continue to apply ITMS rules and mirror images manually

    # Look for itms file in the workspace
    itms_file_path = "oc-mirror-workspace/results-files/working-dir/cluster-resources/itms-oc-mirror.yaml"

    if os.path.exists(itms_file_path):
        log.info(f"Found ITMS file at {itms_file_path}")
        apply_itms_to_managed_clusters(itms_file_path)
    else:
        error_msg = f"ITMS file not found at expected location: {itms_file_path}"
        log.error(error_msg)
        raise ResourceNotFoundError(error_msg)
