import logging
import pytest
import time

from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
    green_squad,
)
from ocs_ci.ocs.ui.validation_ui import ValidationUI

log = logging.getLogger(__name__)


@tier1
@green_squad
@skipif_ocs_version("<4.15")
@skipif_ocp_version("<4.15")
@skipif_external_mode
@runs_on_provider
@skipif_managed_service
class TestOnboardingTokenGeneration(ManageTest):
    @pytest.fixture(autouse=True)
    def setup(self, request):
        """
        Resetting the default value of KeyRotation
        """
        self.pod_obj = ocp.OCP(
            kind="Pod", namespace=config.ENV_DATA["cluster_namespace"]
        )

    def test_ux_server_pod_is_running(self):
        """
        Test to verify that ux-backend-server pod is up and running.

        """
        assert self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.UX_BACKEND_SERVER_LABEL,
            resource_count=1,
            timeout=180,
        ), "ux pod is not in running state as expected"

    def test_respin_of_ux_server_pod(self):
        """
        Test to verify the respin of ux pod.
        Steps:
            1. navigate to storage-->storage clients page
            2. check Generate client onboarding token option is available
            3. user can generate onboarding token by selecting this option.
        """
        # Respin ux-backend-server pod
        ux_pod = pod.get_pods_having_label(
            label=constants.UX_BACKEND_SERVER_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        ux_pod_obj = pod.Pod(**ux_pod[0])

        ux_pod_obj.delete()
        log.info("wait some time for another ux-backend-server pod to come up")
        time.sleep(30)
        ux_pod = pod.get_pods_having_label(
            label=constants.UX_BACKEND_SERVER_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        ux_pod_obj = pod.Pod(**ux_pod[0])
        log.info("ux backed server pod respinned")
        assert pod.validate_pods_are_respinned_and_running_state([ux_pod_obj])

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
