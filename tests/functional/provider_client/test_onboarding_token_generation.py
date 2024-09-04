import logging

from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier4c,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
    yellow_squad,
    hci_provider_required,
)
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.deployment.hosted_cluster import (
    HostedODF,
    HostedClients,
)
from ocs_ci.deployment.helpers.hypershift_base import (
    get_random_hosted_cluster_name,
)
from ocs_ci.utility.version import get_ocs_version_from_csv
from ocs_ci.ocs.resources.catalog_source import get_odf_tag_from_redhat_catsrc
from ocs_ci.utility.utils import (
    get_latest_release_version,
)
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@tier4c
@yellow_squad
@skipif_ocs_version("<4.15")
@skipif_ocp_version("<4.15")
@skipif_external_mode
@runs_on_provider
@skipif_managed_service
class TestOnboardingTokenGeneration(ManageTest):
    def test_onboarding_token_generation_option_is_available_in_ui(
        self, setup_ui_class
    ):
        """
        Test to verify storage-->storage clients-->Generate client onboarding token
        option is available in ui

        Steps:
            1. check onboarding-ticket-key and onboarding-private-key are available
                under secrets page for openshift-storage ns
            2. navigate to storage-->storage clients page
            3. check Generate client onboarding token option is available
            4. user can generate onboarding token by selecting this option.
        """
        secret_ocp_obj = ocp.OCP(
            kind=constants.SECRET, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        for secret_name in {
            constants.ONBOARDING_PRIVATE_KEY,
            constants.MANAGED_ONBOARDING_SECRET,
        }:
            assert secret_ocp_obj.is_exist(
                resource_name=secret_name
            ), f"{secret_name} does not exist in {config.ENV_DATA['cluster_namespace']} namespace"

        ValidationUI().verify_storage_clients_page()

    @skipif_ocs_version("<4.17")
    @skipif_ocp_version("<4.17")
    @hci_provider_required
    def test_onboarding_token_generation_with_limited_storage_quota_from_ui(
        self, create_hypershift_clusters, destroy_hosted_cluster
    ):
        """
        Test to verify onboarding token generation with limited storage quota from
        storage-->storage clients-->Generate client onboarding token from ui

        Steps:
            1. check onboarding-ticket-key and onboarding-private-key are available
                under secrets page for openshift-storage ns
            2. navigate to storage-->storage clients page
            3. check Generate client onboarding token option is available
            4. user can generate onboarding token with limited storage quota.
            5. Onboard a storageclient with limited storage-quota
        """
        from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

        storage_clients = PageNavigator().nav_to_storageclients_page()

        log.info("Create hosted client")
        cluster_name = get_random_hosted_cluster_name()
        odf_version = str(get_ocs_version_from_csv()).replace(".stable", "")
        if "rhodf" in odf_version:
            odf_version = get_odf_tag_from_redhat_catsrc()

        ocp_version = get_latest_release_version()
        nodepool_replicas = 2

        create_hypershift_clusters(
            cluster_names=[cluster_name],
            ocp_version=ocp_version,
            odf_version=odf_version,
            setup_storage_client=True,
            nodepool_replicas=nodepool_replicas,
        )

        log.info("Switch to the hosted cluster")
        config.switch_to_cluster_by_name(cluster_name)

        server = str(OCP().exec_oc_cmd("whoami --show-server", out_yaml_format=False))

        assert (
            cluster_name in server
        ), f"Failed to switch to cluster '{cluster_name}' and fetch data"

        log.info("Test create onboarding key")
        HostedClients().download_hosted_clusters_kubeconfig_files()

        assert len(
            storage_clients.generate_client_onboarding_ticket_ui(storage_quota=8)
        ), "Failed to get onboarding key"
        assert HostedODF(cluster_name).get_storage_client_status() == "Connected"

        log.info("Destroy hosted cluster")
        assert destroy_hosted_cluster(cluster_name), "Failed to destroy hosted cluster"

    @hci_provider_required
    def test_onboarding_storageclient_from_hcp_cluster(
        self, create_hypershift_clusters, destroy_hosted_cluster
    ):
        """
        Test to verify that a new storageclient can be onboarded successfully from a hcp cluster
        using the onboardin token generated from provider--storage--storageclients page
        Steps:
            1. Check ux-backend-server pod is up for provider cluster
            2. Delete ux-backend-server pod
            3. Check ux-backend-server pod is respinned.

        """
        log.info("Create hosted client")
        cluster_name = get_random_hosted_cluster_name()
        odf_version = str(get_ocs_version_from_csv()).replace(".stable", "")
        if "rhodf" in odf_version:
            odf_version = get_odf_tag_from_redhat_catsrc()

        ocp_version = get_latest_release_version()
        nodepool_replicas = 2

        create_hypershift_clusters(
            cluster_names=[cluster_name],
            ocp_version=ocp_version,
            odf_version=odf_version,
            setup_storage_client=True,
            nodepool_replicas=nodepool_replicas,
        )

        log.info("Switch to the hosted cluster")
        config.switch_to_cluster_by_name(cluster_name)

        server = str(OCP().exec_oc_cmd("whoami --show-server", out_yaml_format=False))

        assert (
            cluster_name in server
        ), f"Failed to switch to cluster '{cluster_name}' and fetch data"

        log.info("Test create onboarding key")
        HostedClients().download_hosted_clusters_kubeconfig_files()

        assert len(
            HostedODF(cluster_name).get_onboarding_key()
        ), "Failed to get onboarding key"
        assert HostedODF(cluster_name).get_storage_client_status() == "Connected"

        log.info("Destroy hosted cluster")
        assert destroy_hosted_cluster(cluster_name), "Failed to destroy hosted cluster"

    def test_ux_server_pod_respin_for_provider_cluster(self):
        """
        Test to verify that ux-backend-server pod is up and running for provider cluster.
        And it is respinned successfully

        Steps:
            1. Check ux-backend-server pod is up for provider cluster
            2. Delete ux-backend-server pod
            3. Check ux-backend-server pod is respinned.

        """
        pod_obj = ocp.OCP(kind="Pod", namespace=config.ENV_DATA["cluster_namespace"])
        # Check ux-backend-server pod is up for provider cluster
        assert pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.UX_BACKEND_SERVER_LABEL,
            resource_count=1,
            timeout=120,
        ), "ux-backend-server pod is not in running state as expected"

        # Respin ux-backend-server pod
        ux_pod = pod.get_pods_having_label(
            label=constants.UX_BACKEND_SERVER_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        ux_pod_obj = pod.Pod(**ux_pod[0])

        ux_pod_obj.delete()
        assert pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.UX_BACKEND_SERVER_LABEL,
            resource_count=1,
            timeout=60,
        ), "ux-backend-server pod is not in running state as expected"

        ux_pod = pod.get_pods_having_label(
            label=constants.UX_BACKEND_SERVER_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        ux_pod_obj = pod.Pod(**ux_pod[0])
        log.info("ux backed server pod respinned")
        assert pod.validate_pods_are_respinned_and_running_state([ux_pod_obj])
