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
)
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.deployment.hosted_cluster import (
    HostedODF,
    HostedClients,
)

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

    def test_onboarding_storageclient_from_hcp_cluster(self):
        """
        Test to verify that a new storageclient can be onboarded successfully from a hcp cluster
        using the onboardin token generated from provider--storage--storageclients page
        Steps:
            1. Check ux-backend-server pod is up for provider cluster
            2. Delete ux-backend-server pod
            3. Check ux-backend-server pod is respinned.

        """
        log.info("Deploy hosted OCP on provider platform and onboard storageclient")
        HostedClients().do_deploy()
        log.info("Test create onboarding key")
        HostedClients().download_hosted_clusters_kubeconfig_files()

        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]
        assert len(
            HostedODF(cluster_name).get_onboarding_key()
        ), "Failed to get onboarding key"
        assert HostedODF(cluster_name).get_storage_client_status() == "Connected"

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
