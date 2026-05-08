import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import purple_squad
from ocs_ci.framework.testlib import deployment, polarion_id
from ocs_ci.ocs.resources.storage_cluster import (
    ocs_install_verification,
    mcg_only_install_verification,
)
from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.utility.utils import is_cluster_running, ceph_health_check
from ocs_ci.utility.rosa import post_onboarding_verification
from ocs_ci.helpers.sanity_helpers import Sanity, SanityExternalCluster

from ocs_ci.utility.azure_utils import azure_storageaccount_check
from ocs_ci.deployment.provider_client.storage_client_deployment import (
    verify_provider_mode_deployment,
)


logger = logging.getLogger(__name__)


@purple_squad
@deployment
@polarion_id(get_polarion_id())
def test_deployment(pvc_factory, pod_factory):
    deploy = config.RUN["cli_params"].get("deploy")
    teardown = config.RUN["cli_params"].get("teardown")
    if not teardown or deploy:
        logger.test_step("Verify OCP cluster is running")
        cluster_path = config.ENV_DATA["cluster_path"]
        cluster_running = is_cluster_running(cluster_path)
        logger.assertion(
            f"OCP cluster status: cluster_path='{cluster_path}', "
            f"running={cluster_running}"
        )
        assert cluster_running
        if not config.ENV_DATA["skip_ocs_deployment"]:
            if config.multicluster:
                logger.test_step("Verify multicluster deployment")
                restore_ctx_index = config.cur_index
                for cluster in get_non_acm_cluster_config():
                    cluster_name = cluster.ENV_DATA["cluster_name"]
                    cluster_index = cluster.MULTICLUSTER["multicluster_index"]
                    logger.info(
                        f"Switching to cluster context: name='{cluster_name}', "
                        f"index={cluster_index}"
                    )
                    config.switch_ctx(cluster_index)

                    if config.DEPLOYMENT.get("external_mode") and (
                        config.MULTICLUSTER["multicluster_mode"] == "metro-dr"
                    ):
                        logger.info(
                            f"Running external mode sanity check for cluster '{cluster_name}'"
                        )
                        sanity_helpers = SanityExternalCluster()
                    else:
                        logger.info(
                            f"Running standard sanity check for cluster '{cluster_name}'"
                        )
                        sanity_helpers = Sanity()
                    sanity_helpers.health_check()
                    sanity_helpers.delete_resources()
                logger.info(f"Restoring context to index {restore_ctx_index}")
                config.switch_ctx(restore_ctx_index)
                if (
                    config.ENV_DATA["platform"].lower()
                    in constants.HCI_PC_OR_MS_PLATFORM
                ):
                    logger.info(
                        f"Running post-onboarding verification for platform: "
                        f"{config.ENV_DATA['platform']}"
                    )
                    post_onboarding_verification()
            else:
                logger.test_step("Verify single cluster deployment")
                ocs_registry_image = config.DEPLOYMENT.get("ocs_registry_image")
                if config.ENV_DATA["mcg_only_deployment"]:
                    logger.info("Verifying MCG-only deployment")
                    mcg_only_install_verification(ocs_registry_image=ocs_registry_image)
                    return
                elif config.ENV_DATA.get("odf_provider_mode_deployment", False):
                    logger.info("Verifying ODF provider mode deployment")
                    verify_provider_mode_deployment()
                else:
                    logger.info("Verifying ODF installation")
                    ocs_install_verification(ocs_registry_image=ocs_registry_image)

                if (
                    config.ENV_DATA["platform"].lower() == constants.AZURE_PLATFORM
                    and config.ENV_DATA["deployment_type"] != "managed"
                ):
                    logger.info("Running Azure storage account check")
                    azure_storageaccount_check()

                logger.test_step("Run sanity checks and resource validation")
                # Check basic cluster functionality by creating resources
                # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
                # run IO and delete the resources
                if config.DEPLOYMENT["external_mode"]:
                    logger.info("Initializing external cluster sanity helpers")
                    sanity_helpers = SanityExternalCluster()
                else:
                    logger.info("Initializing standard sanity helpers")
                    sanity_helpers = Sanity()
                if (
                    config.ENV_DATA["platform"].lower()
                    in constants.HCI_PC_OR_MS_PLATFORM
                ):
                    logger.info(
                        f"Running health check for HCI/PC/MS platform: "
                        f"{config.ENV_DATA['platform']}"
                    )
                    try:
                        sanity_helpers.health_check(
                            fix_ceph_health=True,
                            update_jira=True,
                            no_exception_if_jira_issue_updated=True,
                        )
                    except exceptions.ResourceWrongStatusException as err_msg:
                        logger.warning(
                            f"Resource status exception during health check: {err_msg}"
                        )
                else:
                    logger.info("Running standard health check")
                    sanity_helpers.health_check(
                        fix_ceph_health=True,
                        update_jira=True,
                        no_exception_if_jira_issue_updated=True,
                    )
                logger.info("Cleaning up sanity test resources")
                sanity_helpers.delete_resources()
                logger.test_step("Verify Ceph health after deployment")
                # TODO: Enable the check when a solution is identified for tools pod on FaaS consumer
                if not (
                    config.ENV_DATA.get("platform") == constants.FUSIONAAS_PLATFORM
                    and config.ENV_DATA["cluster_type"].lower() == "consumer"
                ):
                    ceph_healthy = ceph_health_check(
                        tries=10,
                        delay=30,
                        fix_ceph_health=True,
                        update_jira=True,
                        no_exception_if_jira_issue_updated=True,
                    )
                    logger.assertion(f"Ceph health check: healthy={ceph_healthy}")
                    assert ceph_healthy, "Ceph health check failed after deployment"

    if teardown:
        logger.info("Cluster will be destroyed during teardown part of this test.")
