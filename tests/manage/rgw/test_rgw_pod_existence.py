import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import acceptance, rgw
from ocs_ci.helpers.helpers import storagecluster_independent_check
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import check_if_cluster_was_upgraded
from ocs_ci.utility import version
from ocs_ci.utility.rgwutils import get_rgw_count

logger = logging.getLogger(__name__)


@rgw
@acceptance
class TestRGWPodExistence:
    """
    Test the existence of RGW pods based on platform
    """

    def test_rgw_pod_existence(self):
        if (
            config.ENV_DATA["platform"].lower() in constants.CLOUD_PLATFORMS
            or storagecluster_independent_check()
        ):
            if (
                not config.ENV_DATA["platform"] == constants.AZURE_PLATFORM
                and not config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                and (
                    version.get_semantic_ocs_version_from_config() > version.VERSION_4_5
                )
            ):
                logger.info("Checking whether RGW pod is not present")
                assert (
                    not pod.get_rgw_pods()
                ), "RGW pods should not exist in the current platform/cluster"

        elif (
            config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS
            and not config.ENV_DATA["mcg_only_deployment"]
        ):
            rgw_count = get_rgw_count(
                config.ENV_DATA["ocs_version"], check_if_cluster_was_upgraded(), None
            )
            logger.info(
                f'Checking for RGW pod/s on {config.ENV_DATA.get("platform")} platform'
            )
            rgw_pod = OCP(
                kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
            )
            assert rgw_pod.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=constants.RGW_APP_LABEL,
                resource_count=rgw_count,
                timeout=60,
            )
