import logging
import random

from ocs_ci.deployment.helpers.hypershift_base import (
    get_hosted_cluster_names,
    get_random_cluster_name,
)
from ocs_ci.deployment.hosted_cluster import (
    HypershiftHostedOCP,
    HostedODF,
    HostedClients,
)
from ocs_ci.framework import config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.framework.pytest_customization.marks import (
    hci_provider_required,
    libtest,
    purple_squad,
    runs_on_provider,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import get_latest_release_version
from ocs_ci.utility.version import get_ocs_version_from_csv
from ocs_ci.framework import config as ocsci_config


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

    @runs_on_provider
    @hci_provider_required
    def test_create_hosted_cluster_with_fixture(
        self, create_hypershift_clusters, destroy_hosted_cluster
    ):
        """
        Test create hosted cluster with fixture
        """
        log_step("Create hosted client")
        cluster_name = get_random_cluster_name()
        odf_version = str(get_ocs_version_from_csv()).replace(".stable", "")
        ocp_version = get_latest_release_version()
        nodepool_replicas = 2

        create_hypershift_clusters(
            cluster_names=[cluster_name],
            ocp_version=ocp_version,
            odf_version=odf_version,
            setup_storage_client=True,
            nodepool_replicas=nodepool_replicas,
        )

        log_step("Switch to the hosted cluster")
        ocsci_config.switch_to_cluster_by_name(cluster_name)

        server = str(
            OCP().exec_oc_cmd("oc whoami --show-server", out_yaml_format=False)
        )

        assert (
            cluster_name in server
        ), f"Failed to switch to cluster '{cluster_name}' and fetch data"

    @runs_on_provider
    @hci_provider_required
    def test_create_destroy_hosted_cluster_with_fixture(
        self, create_hypershift_clusters, destroy_hosted_cluster
    ):
        """
        Test create hosted cluster with fixture
        """
        log_step("Create hosted client")
        cluster_name = get_random_cluster_name()
        odf_version = str(get_ocs_version_from_csv()).replace(".stable", "")
        ocp_version = get_latest_release_version()
        nodepool_replicas = 2

        create_hypershift_clusters(
            cluster_names=[cluster_name],
            ocp_version=ocp_version,
            odf_version=odf_version,
            setup_storage_client=True,
            nodepool_replicas=nodepool_replicas,
        )

        log_step("Switch to the hosted cluster")
        ocsci_config.switch_to_cluster_by_name(cluster_name)

        server = str(
            OCP().exec_oc_cmd("oc whoami --show-server", out_yaml_format=False)
        )

        assert (
            cluster_name in server
        ), f"Failed to switch to cluster '{cluster_name}' and fetch data"

        log_step("Destroy hosted cluster")
        assert destroy_hosted_cluster(cluster_name), "Failed to destroy hosted cluster"
