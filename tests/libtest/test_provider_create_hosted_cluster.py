import logging
import random

from ocs_ci.deployment.helpers.hypershift_base import (
    get_hosted_cluster_names,
)
from ocs_ci.deployment.hosted_cluster import (
    HypershiftHostedOCP,
    HostedODF,
    HostedClients,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    hci_provider_required,
    libtest,
    purple_squad,
    runs_on_provider,
)

logger = logging.getLogger(__name__)


@libtest
@purple_squad
class TestProviderHosted(object):
    """
    Test provider hosted
    """

    @hci_provider_required
    def test_provider_deploy_OCP_hosted(self):
        """
        Test deploy hosted OCP
        """

        logger.info("Test deploy hosted OCP on provider platform")
        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]

        HypershiftHostedOCP(cluster_name).deploy_ocp()

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_skip_cnv_and_lb(self):
        """
        Test deploy hosted OCP on provider platform with cnv and metallb ready beforehand
        """
        logger.info(
            "Test deploy hosted OCP on provider platform with metallb and cnv ready"
        )
        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]

        HypershiftHostedOCP(cluster_name).deploy_ocp(
            deploy_cnv=False, deploy_metallb=False, download_hcp_binary=True
        )

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_skip_cnv(self):
        """
        Test deploy hosted OCP on provider platform with cnv ready beforehand
        """
        logger.info("Test deploy hosted OCP on provider platform with cnv ready")
        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]

        HypershiftHostedOCP(cluster_name).deploy_ocp(deploy_cnv=False)

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_multiple(self):
        """
        Test deploy hosted OCP on provider platform multiple times
        """
        logger.info("Test deploy hosted OCP on provider platform multiple times")
        HostedClients().deploy_hosted_ocp_clusters()

    @runs_on_provider
    @hci_provider_required
    def test_install_odf_on_hosted_cluster(self):
        """
        Test install ODF on hosted cluster
        """
        logger.info("Test install ODF on hosted cluster")

        HostedClients().download_hosted_clusters_kubeconfig_files()

        hosted_cluster_names = get_hosted_cluster_names()
        cluster_name = random.choice(hosted_cluster_names)

        hosted_odf = HostedODF(cluster_name)
        hosted_odf.do_deploy()

    @runs_on_provider
    @hci_provider_required
    def test_deploy_OCP_and_setup_ODF_client_on_hosted_clusters(self):
        """
        Test install ODF on hosted cluster
        """
        logger.info("Deploy hosted OCP on provider platform multiple times")
        HostedClients().do_deploy()

    @runs_on_provider
    @hci_provider_required
    def test_create_onboarding_key(self):
        """
        Test create onboarding key
        """
        logger.info("Test create onboarding key")
        HostedClients().download_hosted_clusters_kubeconfig_files()

        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]
        assert len(
            HostedODF(cluster_name).get_onboarding_key()
        ), "Failed to get onboarding key"

    @runs_on_provider
    @hci_provider_required
    def test_storage_client_connected(self):
        """
        Test storage client connected
        """
        logger.info("Test storage client connected")
        HostedClients().download_hosted_clusters_kubeconfig_files()

        cluster_names = list(config.ENV_DATA["clusters"].keys())
        assert HostedODF(cluster_names[-1]).get_storage_client_status() == "Connected"
