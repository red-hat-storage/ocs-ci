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

log = logging.getLogger(__name__)


@purple_squad
@deployment
@polarion_id(get_polarion_id())
def test_deployment(pvc_factory, pod_factory):
    deploy = config.RUN["cli_params"].get("deploy")
    teardown = config.RUN["cli_params"].get("teardown")
    if not teardown or deploy:
        log.info("Verifying OCP cluster is running")
        assert is_cluster_running(config.ENV_DATA["cluster_path"])
        if not config.ENV_DATA["skip_ocs_deployment"]:
            if config.multicluster:
                restore_ctx_index = config.cur_index
                for cluster in get_non_acm_cluster_config():
                    config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                    log.info(
                        f"Sanity check for cluster: {cluster.ENV_DATA['cluster_name']}"
                    )
                    if config.DEPLOYMENT.get("external_mode") and (
                        config.MULTICLUSTER["multicluster_mode"] == "metro-dr"
                    ):
                        log.info("Sanity check for external mode")
                        sanity_helpers = SanityExternalCluster()
                    else:
                        sanity_helpers = Sanity()
                    sanity_helpers.health_check()
                    sanity_helpers.delete_resources()
                config.switch_ctx(restore_ctx_index)
                if (
                    config.ENV_DATA["platform"].lower()
                    in constants.MANAGED_SERVICE_PLATFORMS
                ):
                    post_onboarding_verification()
            else:
                ocs_registry_image = config.DEPLOYMENT.get("ocs_registry_image")
                if config.ENV_DATA["mcg_only_deployment"]:
                    mcg_only_install_verification(ocs_registry_image=ocs_registry_image)
                    return
                else:
                    ocs_install_verification(ocs_registry_image=ocs_registry_image)

                if (
                    config.ENV_DATA["platform"].lower() == constants.AZURE_PLATFORM
                    and config.ENV_DATA["deployment_type"] != "managed"
                ):
                    azure_storageaccount_check()

                # Check basic cluster functionality by creating resources
                # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
                # run IO and delete the resources
                if config.DEPLOYMENT["external_mode"]:
                    sanity_helpers = SanityExternalCluster()
                else:
                    sanity_helpers = Sanity()
                if (
                    config.ENV_DATA["platform"].lower()
                    in constants.MANAGED_SERVICE_PLATFORMS
                ):
                    try:
                        sanity_helpers.health_check()
                    except exceptions.ResourceWrongStatusException as err_msg:
                        log.warning(err_msg)
                else:
                    sanity_helpers.health_check()
                sanity_helpers.delete_resources()
                # Verify ceph health
                log.info("Verifying ceph health after deployment")
                # TODO: Enable the check when a solution is identified for tools pod on FaaS consumer
                if not (
                    config.ENV_DATA.get("platform") == constants.FUSIONAAS_PLATFORM
                    and config.ENV_DATA["cluster_type"].lower() == "consumer"
                ):
                    assert ceph_health_check(tries=10, delay=30)

    if teardown:
        log.info("Cluster will be destroyed during teardown part of this test.")
