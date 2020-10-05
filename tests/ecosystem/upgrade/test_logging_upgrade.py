import logging

import pytest

from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.utility.deployment_openshift_logging import (
    check_health_of_clusterlogging, get_clusterlogging_subscription
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.framework.pytest_customization.marks import (
    post_ocp_upgrade
)
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    get_ocp_version, run_cmd
)
from tests.conftest import install_logging


logger = logging.getLogger(__name__)


def check_cluster_logging():
    """
    Few checks to assert for before and after upgrade
    1. Checks for pods in openshift-logging project
    2. Checks for the health of logging cluster before upgrade

    """

    assert get_all_pods(
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    ), 'Some or all pods missing in namespace'

    assert check_health_of_clusterlogging(), "Cluster is not Healthy"


@retry(CommandFailed, tries=5, delay=120, backoff=1)
def get_csv_version(channel):
    """
    Get the cluster-logging and Elasticsearch CSV version
    and Images of the pods

    Args:
        channel (str) : Logging Channel

    Returns:
        tuple: Tuple containing three elements
            cluster_logging_csv (str) : Name of the cluster-logging CSV
            elasticsearch_csv (str) : Name of the elasticsearch CSV
            dict: Images dict like: {'image_name': 'image.url.to:tag', ...}

    """

    clo_package_manifest = PackageManifest(
        resource_name='cluster-logging'
    )
    cluster_logging_csv = clo_package_manifest.get_current_csv(channel)
    es_package_manifest = PackageManifest(
        resource_name='elasticsearch-operator'
    )
    elasticsearch_csv = es_package_manifest.get_current_csv(channel)
    logging_csv = CSV(
        resource_name=cluster_logging_csv,
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    images = ocp.get_images(logging_csv.get())
    return cluster_logging_csv, elasticsearch_csv, images


def check_csv_version_post_upgrade(channel):
    """
    This function is to check the CSV version post upgrade

    Args:
        channel (str) : logging channel

    Returns:
        bool : Returns True if version matches, False otherwise

    """

    cluster_logging_csv, elasticsearch_csv, images = get_csv_version(channel)
    if (
        config.UPGRADE['upgrade_logging_channel'] in cluster_logging_csv
        and config.UPGRADE['upgrade_logging_channel'] in elasticsearch_csv
    ):
        logger.info(
            f"Upgraded version of Cluster-logging "
            f"operator {cluster_logging_csv}"
        )
        logger.info(
            f"Upgraded version of Elastic-search "
            f"operator {elasticsearch_csv}"
        )
        return True
    return False


@retry(AttributeError, tries=10, delay=120, backoff=1)
def check_csv_logging_phase(channel):
    """
    The function checks in the CSV for the phase "Succeeded"
    for cluster-logging operator and Elastic-search operator

    Args:
        channel (str) : Logging channel

    Returns:
        bool: True if CSV status is Succeeded, False otherwise

    """

    cluster_version_csv, elasticsearch_csv, images = get_csv_version(channel)
    logging_csv = CSV(
        resource_name=cluster_version_csv,
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    clo_phase = logging_csv.get().get('status').get('phase')
    logging_csv.get().get('status').get('phase')

    es_csv = CSV(
        resource_name=elasticsearch_csv,
        namespace=constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE
    )
    eso_phase = es_csv.get().get('status').get('phase')

    if clo_phase and eso_phase == 'Succeeded':
        logger.info("CSV phase Succeeded")
        return True

    logger.info("Upgrade not completed yet")
    return False


def upgrade_info(channel):
    """
     Function to provide CSV names and images pre and post upgrade

     Args:
         channel (str): Logging channel

    """

    logging_csv, elasticsearch_csv, images = get_csv_version(channel)
    logger.info(
        f"Cluster-logging CSV for channel {channel} "
        f"{logging_csv}"
    )
    logger.info(
        f"Elastic-search CSV for channel {channel} "
        f"{elasticsearch_csv}"
    )
    logger.info(
        f"Images of cluster-logging components for channel {channel} "
        f"{images}"
    )


@post_ocp_upgrade
@pytest.mark.usefixtures(install_logging.__name__)
@pytest.mark.polarion_id("OCS-2201")
class TestUpgradeLogging():
    """
    This class contains test for upgrade openshift-logging
    after OCP upgrade
    1. Monitors OCS health
    2. Checks OCP version
    3. Checks logging version, if mismatched then upgrades logging
    4. Checks Elasticsearch and clusterlogging operator
    5. Checks logging health post upgrade

    """

    def test_upgrade_logging(self):
        """
        This function contains test to upgrade openshift-logging
        with Entry and Exit criteria for checks

        """

        ceph_cluster = CephCluster()
        with CephHealthMonitor(ceph_cluster):

            #  Pre-check
            logger.info("Checking cluster logging before starting to upgrade")
            check_cluster_logging()

            # Matching the OCP version and cluster-Logging version
            ocp_version = get_ocp_version()
            logger.info(f"OCP version {ocp_version}")
            subscription = get_clusterlogging_subscription()
            logging_channel = subscription.get('spec').get('channel')
            logger.info(
                f"Current Logging channel {logging_channel}"
            )
            upgrade_info(logging_channel)
            upgrade_channel = config.UPGRADE['upgrade_logging_channel']
            if ocp_version > logging_channel:
                # Upgrade Elastic search operator Subscription
                es_subscription_cmd = (
                    'oc patch subscription elasticsearch-operator '
                    f'-n {constants.OPENSHIFT_OPERATORS_REDHAT_NAMESPACE} '
                    '--type merge -p \'{"spec":{"channel": '
                    f'"{upgrade_channel}"}}}}\''
                )
                # Upgrade Cluster-logging operator subscription
                clo_subscription_cmd = (
                    'oc patch subscription cluster-logging '
                    f'-n {constants.OPENSHIFT_LOGGING_NAMESPACE} '
                    '--type merge -p \'{"spec":{"channel": '
                    f'"{upgrade_channel}"}}}}\''
                )
                run_cmd(es_subscription_cmd)
                run_cmd(clo_subscription_cmd)
                assert check_csv_logging_phase(
                    upgrade_channel
                ), "Logging upgrade not completed yet!"
                logger.info(
                    "Logging upgrade completed!"
                )
                assert check_csv_version_post_upgrade(
                    upgrade_channel
                ), "Unable to get version "
                logger.info("Version Matches!")
                upgrade_info(upgrade_channel)
                check_cluster_logging()
            else:
                logger.info(
                    "Logging Version matches the OCP version, "
                    "No upgrade needed"
                )
