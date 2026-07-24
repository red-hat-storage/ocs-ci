import logging
import platform
import os
import tempfile
from ocs_ci.deployment.ocp import download_pull_secret
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
    TimeoutExpiredError,
    UnexpectedDeploymentConfiguration,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.dr_helpers import (
    check_rbd_mirror_running,
    wait_for_mirroring_status_ok,
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
        try:
            wait_for_mirroring_status_ok(timeout=120)
        except TimeoutExpiredError:
            log.error("Mirroring health is not OK on managed clusters")
            pytest.skip("Mirroring health is not OK on managed clusters")
    finally:
        config.switch_ctx(restore_index)


@pytest.fixture(scope="session", autouse=True)
def get_virtctl():
    with config.RunWithProviderConfigContextIfAvailable():
        get_virtctl_tool()


@pytest.fixture()
def cnv_custom_storage_class(
    request, secret_factory, ceph_pool_factory, storageclass_factory
):
    """
    Creates a custom CephBlockPool and StorageClass on both managed
    clusters, and a DRPolicy on the ACM hub for CNV discovered-app
    DR tests with custom pools.

    In an RDR setup, when a custom CephBlockPool is created the auto-generated
    CephBlockPoolRadosNamespace gets stuck at Progressing because the
    MirroringController cannot enable mirroring until the peer cluster's pool
    exists. This fixture handles the ordering to ensure all resources are ready
    before DR protection.

    Setup flow:
        1. Create CephBlockPool on each managed cluster and wait for Ready
        2. Wait for CephBlockPoolRadosNamespace to reach Ready on each cluster.
           If stuck at Progressing, restart the ocs-operator pod to reset
           the MirroringController's exponential backoff
        3. Create StorageClass on each managed cluster (only after RadosNamespace
           is Ready so Ramen sees a fully mirrored pool)
        4. Create a new DRPolicy on the ACM hub by cloning the existing policy's
           spec with a new name
        5. Wait for the new DRPolicy to reach Validated status
        6. Wait for the new DRPolicy's peerClasses to include the custom SC

    Teardown flow:
        1. Delete the custom DRPolicy from ACM hub
        2. Delete CephBlockPoolRadosNamespace on each managed cluster
        3. Delete CephBlockPool on each managed cluster

    Args:
        request: pytest request object
        secret_factory: Fixture for creating secrets (ensures correct
            teardown ordering: SC -> pool -> secret)
        ceph_pool_factory: Fixture for creating CephBlockPool resources
        storageclass_factory: Fixture for creating StorageClass resources

    Returns:
        factory: A callable that accepts replica and compression parameters,
            creates all resources, and returns the new DRPolicy name

    """

    def factory(replica, compression):
        """
        Create custom pool, SC, and DRPolicy on both managed clusters.

        Args:
            replica (int): Replica count for the CephBlockPool
            compression (str): Compression type for the pool, or None

        Returns:
            str: Name of the newly created DRPolicy

        """
        from ocs_ci.ocs import ocp
        from ocs_ci.ocs.resources.pod import delete_pods, get_all_pods
        from ocs_ci.utility.utils import TimeoutSampler

        pool_name = constants.RDR_CUSTOM_RBD_POOL
        sc_name = constants.RDR_CUSTOM_RBD_STORAGECLASS

        log.test_step(f"Create CephBlockPool {pool_name} on both managed clusters")
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            cluster_name = config.ENV_DATA.get("cluster_name")
            existing_sc_list = get_all_storageclass()
            if sc_name in existing_sc_list:
                log.info(f"Storage class {sc_name} already exists on {cluster_name}")
                continue
            pool_ocp = ocp.OCP(
                kind=constants.CEPHBLOCKPOOL,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=pool_name,
            )
            if pool_ocp.is_exist(resource_name=pool_name):
                log.info(
                    f"Pool {pool_name} already exists on {cluster_name},"
                    f" skipping creation"
                )
            else:
                log.info(
                    f"Creating CephBlockPool {pool_name} on {cluster_name}"
                    f" with replica={replica}, compression={compression}"
                )
                ceph_pool_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    replica=replica,
                    compression=compression,
                    pool_name=pool_name,
                )
                for sample in TimeoutSampler(600, 10, pool_ocp.get):
                    phase = sample.get("status", {}).get("phase") if sample else None
                    if phase == constants.STATUS_READY:
                        log.info(
                            f"CephBlockPool {pool_name} is Ready" f" on {cluster_name}"
                        )
                        break

        log.test_step(
            "Wait for CephBlockPoolRadosNamespace to reach Ready" " on both clusters"
        )
        radosns_name = f"{pool_name}-builtin-implicit"
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            cluster_name = config.ENV_DATA.get("cluster_name")
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
                    f"CephBlockPoolRadosNamespace {radosns_name} is"
                    f" {radosns_phase} on {cluster_name}, restarting"
                    f" ocs-operator to reset MirroringController backoff"
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
                        f"CephBlockPoolRadosNamespace {radosns_name}"
                        f" phase: {radosns_phase} on {cluster_name}"
                    )
                    if radosns_phase == constants.STATUS_READY:
                        break
            else:
                log.info(
                    f"CephBlockPoolRadosNamespace {radosns_name}"
                    f" is already Ready on {cluster_name}"
                )

        log.test_step(f"Create StorageClass {sc_name} on both managed clusters")
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            cluster_name = config.ENV_DATA.get("cluster_name")
            existing_sc_list = get_all_storageclass()
            if sc_name in existing_sc_list:
                log.info(f"Storage class {sc_name} already exists on {cluster_name}")
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
                    log.info(
                        f"Successfully created custom RBD SC:"
                        f" {sc_name} on {cluster_name}"
                    )
            except Exception as e:
                log.error(f"Error creating SC '{sc_name}' on {cluster_name}: {e}")
                raise

        from ocs_ci.helpers.dr_helpers import (
            get_all_drpolicy,
            validate_drpolicy_replication_ids,
        )

        dr_policy_name = constants.RDR_CUSTOM_RBD_DR_POLICY

        log.test_step(f"Create new DRPolicy {dr_policy_name} on ACM hub")
        existing_drpolicy = get_all_drpolicy()[0]
        dr_policy_data = {
            "apiVersion": existing_drpolicy["apiVersion"],
            "kind": existing_drpolicy["kind"],
            "metadata": {
                "name": dr_policy_name,
                "labels": existing_drpolicy["metadata"].get("labels", {}),
            },
            "spec": existing_drpolicy["spec"],
        }
        dr_policy_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="dr_policy_custom_", delete=False
        )
        templating.dump_data_to_temp_yaml(dr_policy_data, dr_policy_yaml.name)
        config.switch_acm_ctx()
        exec_cmd(f"oc create -f {dr_policy_yaml.name}")

        log.test_step(f"Wait for DRPolicy {dr_policy_name} to reach Validated status")
        drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY)
        for sample in TimeoutSampler(600, 10, drpolicy_ocp.get, dr_policy_name):
            reason = (
                sample.get("status", {}).get("conditions", [{}])[-1].get("reason", "")
            )
            log.info(f"DRPolicy {dr_policy_name} reason: {reason}")
            if reason in constants.DRPOLICY_SUCCESS_REASONS:
                break

        log.test_step(
            f"Wait for DRPolicy {dr_policy_name} peerClasses" f" to include {sc_name}"
        )
        for sample in TimeoutSampler(600, 10, drpolicy_ocp.get, dr_policy_name):
            peer_classes = (
                sample.get("status", {}).get("async", {}).get("peerClasses", [])
            )
            sc_names_in_policy = [pc.get("storageClassName") for pc in peer_classes]
            log.info(
                f"DRPolicy {dr_policy_name}" f" peerClasses SCs: {sc_names_in_policy}"
            )
            if sc_name in sc_names_in_policy:
                log.info(
                    f"DRPolicy {dr_policy_name}" f" peerClasses now includes {sc_name}"
                )
                break

        log.test_step(
            "Validate groupreplicationID values across default and custom SCs"
        )
        validate_drpolicy_replication_ids(
            drpolicy_name=dr_policy_name,
            sc_names=[constants.DEFAULT_STORAGECLASS_RBD, sc_name],
        )

        config.reset_ctx()
        return dr_policy_name

    def teardown():
        """
        Clean up custom DR resources after test execution.

        Deletes in order: DRPolicy (hub) -> RadosNamespace (both clusters)
        -> CephBlockPool (both clusters). Each step is wrapped in
        try/except so a failure on one resource doesn't block cleanup
        of the remaining resources.

        """
        from ocs_ci.ocs import ocp

        log.test_step("Teardown: Delete custom DRPolicy from ACM hub")
        dr_policy_name = constants.RDR_CUSTOM_RBD_DR_POLICY
        try:
            config.switch_acm_ctx()
            drpolicy_ocp = ocp.OCP(kind=constants.DRPOLICY)
            if drpolicy_ocp.is_exist(resource_name=dr_policy_name):
                log.info(f"Deleting DRPolicy {dr_policy_name}")
                drpolicy_ocp.delete(resource_name=dr_policy_name)
                drpolicy_ocp.wait_for_delete(dr_policy_name, timeout=300)
            else:
                log.info(f"DRPolicy {dr_policy_name} not found, skipping")
        except Exception as e:
            log.warning(f"Failed to delete DRPolicy {dr_policy_name}: {e}")

        log.test_step(
            "Teardown: Delete custom pool and RadosNamespace"
            " on both managed clusters"
        )
        pool_name = constants.RDR_CUSTOM_RBD_POOL
        radosns_name = f"{pool_name}-builtin-implicit"

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            namespace = config.ENV_DATA["cluster_namespace"]
            cluster_name = config.ENV_DATA.get("cluster_name")

            try:
                radosns_ocp = ocp.OCP(
                    kind=constants.CEPHBLOCKPOOLRADOSNS,
                    namespace=namespace,
                    resource_name=radosns_name,
                )
                if radosns_ocp.is_exist(resource_name=radosns_name):
                    log.info(
                        f"Deleting RadosNamespace {radosns_name}" f" on {cluster_name}"
                    )
                    radosns_ocp.delete(resource_name=radosns_name)
                    radosns_ocp.wait_for_delete(radosns_name, timeout=300)
                else:
                    log.info(
                        f"RadosNamespace {radosns_name} not found"
                        f" on {cluster_name}, skipping"
                    )
            except Exception as e:
                log.warning(
                    f"Failed to delete RadosNamespace {radosns_name}"
                    f" on {cluster_name}: {e}"
                )

            try:
                pool_ocp = ocp.OCP(
                    kind=constants.CEPHBLOCKPOOL,
                    namespace=namespace,
                    resource_name=pool_name,
                )
                if pool_ocp.is_exist(resource_name=pool_name):
                    log.info(f"Deleting pool {pool_name} on {cluster_name}")
                    pool_ocp.delete(resource_name=pool_name)
                    pool_ocp.wait_for_delete(pool_name, timeout=300)
                else:
                    log.info(
                        f"Pool {pool_name} not found" f" on {cluster_name}, skipping"
                    )
            except Exception as e:
                log.warning(
                    f"Failed to delete pool {pool_name}" f" on {cluster_name}: {e}"
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
def mirror_rdr_images(request):
    """
    Mirror RDR images to disconnected registry and apply ITMS to managed clusters.
    Skip this fixture when test_deploy_rdr is being executed.
    """
    # Skip this fixture for test_deploy_rdr
    if request.session.items:
        for item in request.session.items:
            if "test_deploy_rdr" in item.nodeid:
                log.info("Skipping mirror_rdr_images fixture for test_deploy_rdr")
                return

    if not config.DEPLOYMENT.get("disconnected"):
        return

    # TODO: update itms_name to  odf-generic-0
    # Check if odf-generic-0 ITMS already exists on managed clusters - skip mirroring if present
    itms_name = "itms-generic-0"
    managed_clusters = get_non_acm_cluster_config()

    if managed_clusters:
        # Store original context
        original_ctx = config.cur_index
        itms_exists_on_all = True

        try:
            for cluster_config in managed_clusters:
                cluster_name = cluster_config.ENV_DATA.get("cluster_name", "unknown")
                cluster_index = cluster_config.MULTICLUSTER.get("multicluster_index")

                if cluster_index is not None:
                    config.switch_ctx(cluster_index)
                    try:
                        # Check if odf-generic-0 ITMS exists on this cluster
                        run_cmd(f"oc get itms {itms_name}")
                        log.info(
                            f"ITMS '{itms_name}' already exists on cluster {cluster_name}"
                        )
                    except CommandFailed:
                        log.info(
                            f"ITMS '{itms_name}' not found on cluster {cluster_name}"
                        )
                        itms_exists_on_all = False
                        break
        finally:
            config.switch_ctx(original_ctx)

        if itms_exists_on_all:
            log.info(
                f"ITMS '{itms_name}' already exists on all managed clusters. "
                "Skipping mirror operation and ITMS application."
            )
            return

    imageset_config_data = templating.load_yaml(constants.OC_MIRROR_IMAGESET_CONFIG_V2)

    # Get RDR images and add to additionalImages
    rdr_images = generate_rdr_mirror_images()
    if not rdr_images:
        log.warning("No RDR images found to mirror. Exiting function.")
        return

    # Convert image list to the format required by oc mirror v2
    # Each image needs to be in format: {"name": "image_url"}
    # Strip docker:// prefix if present
    formatted_images = [
        {"name": image.replace("docker://", "")} for image in rdr_images
    ]

    imageset_config_data["mirror"]["additionalImages"] = formatted_images
    log.info(f"Added {len(formatted_images)} RDR images to mirror configuration")

    # Mirror required images
    log.info(
        f"Mirror required images to mirror registry {config.DEPLOYMENT['mirror_registry']}"
    )
    imageset_config_file = os.path.join(
        config.ENV_DATA["cluster_path"],
        f"imageset-config-{config.RUN['run_id']}.yaml",
    )
    templating.dump_data_to_temp_yaml(imageset_config_data, imageset_config_file)
    pull_secret_path = download_pull_secret()
    cmd = (
        f"oc mirror --config {imageset_config_file} "
        f"docker://{config.DEPLOYMENT['mirror_registry']}/{config.DEPLOYMENT['mirror_registry_path']} "
        f"--authfile {pull_secret_path} "
        "--workspace file://oc-mirror-workspace/results-files --v2 --dest-tls-verify=false --src-tls-verify=false"
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

        # Modify ITMS name to odf-generic-0 (always use -0 suffix)
        try:
            itms_data = templating.load_yaml(itms_file_path)
            if (
                itms_data.get("metadata", {})
                .get("name", "")
                .startswith("itms-generic-")
            ):
                old_name = itms_data["metadata"]["name"]
                new_name = "odf-generic-0"
                itms_data["metadata"]["name"] = new_name
                log.info(f"Renaming ITMS from '{old_name}' to '{new_name}'")

                # Save modified ITMS back to file
                templating.dump_data_to_temp_yaml(itms_data, itms_file_path)
        except Exception as e:
            log.warning(f"Failed to rename ITMS: {e}. Continuing with original name.")

        apply_itms_to_managed_clusters(itms_file_path)
    else:
        error_msg = f"ITMS file not found at expected location: {itms_file_path}"
        log.error(error_msg)
        raise ResourceNotFoundError(error_msg)
