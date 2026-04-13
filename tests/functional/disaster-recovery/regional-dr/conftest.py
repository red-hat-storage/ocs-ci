import logging
import os
from ocs_ci.utility import templating
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.deployment import acm
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import (
    exec_cmd,
    run_cmd,
)
from ocs_ci.helpers.dr_helpers import (
    _apply_itms_to_managed_clusters,
    _generate_rdr_mirror_images,
)
from ocs_ci.ocs.exceptions import CommandFailed, ResourceNotFoundError
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


@pytest.fixture(scope="session", autouse=True)
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


@pytest.fixture(autouse=True)
def mirror_rdr_images():
    """
    Mirror RDR images to disconnected registry and apply ITMS to managed clusters.
    """
    if not config.DEPLOYMENT.get("disconnected"):
        return

    imageset_config_data = templating.load_yaml(constants.OC_MIRROR_IMAGESET_CONFIG_V2)

    # Get RDR images and add to additionalImages
    rdr_images = _generate_rdr_mirror_images()
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
        # even though we use --continue-on-error and --skip-missing arguments
        # (not sure if it is because of a bug in oc mirror plugin or because of some other issue),
        # but we want to continue to try to mirror the images manually with applied the itms rules
        log.warning(f"oc mirror command failed: {e}")
        raise

    # Look for itms file in the workspace
    itms_file_path = "oc-mirror-workspace/results-files/working-dir/cluster-resources/itms-oc-mirror.yaml"

    if os.path.exists(itms_file_path):
        log.info(f"Found ITMS file at {itms_file_path}")
        _apply_itms_to_managed_clusters(itms_file_path)
    else:
        error_msg = f"ITMS file not found at expected location: {itms_file_path}"
        log.error(error_msg)
        raise ResourceNotFoundError(error_msg)
