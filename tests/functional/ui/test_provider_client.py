import logging

# from ocs_ci.ocs import constants
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    runs_on_provider,
    black_squad,
    hci_provider_required,
)
from ocs_ci.ocs.resources import storageconsumer
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator


logger = logging.getLogger(__name__)


@tier1
@black_squad
@skipif_ocs_version("<4.17")
@skipif_ocp_version("<4.17")
@runs_on_provider
@hci_provider_required
class TestOnboardingTokenGenerationWithQuota(ManageTest):
    """
    Test onboarding token generation when quota is specified
    """

    def test_token_generation_with_quota(
        self, setup_ui_class, quota_value=2, quota_tib=True
    ):
        storage_clients = PageNavigator().nav_to_storageclients_page()
        token = storage_clients.generate_client_onboarding_ticket(
            quota_value=quota_value, quota_tib=quota_tib
        )
        logger.info(f"Token generated. It begins with {token[:20]}")
        assert len(token) > 20, "Token is too short"

    def test_quota_decrease_blocked(self, setup_ui_class):
        """
        Test that quota cannot be increased for a client:
        if a client has unlimited quota, it cannot be changed.
        If a client has limited quota, the new value cannot be lower
        """
        storage_clients_page = PageNavigator().nav_to_storageclients_page()
        client_clusters = storageconsumer.get_all_client_clusters()
        for client in client_clusters:
            quota = storage_clients_page.get_client_quota_from_ui(client)
            if quota == "Unlimited":
                assert not storage_clients_page.edit_quota(
                    client_cluster_name=client, increase_by_one=True
                )
            else:
                new_quota = int(quota) - 1
                assert not storage_clients_page.edit_quota(
                    client_cluster_name=client,
                    increase_by_one=False,
                    new_value=new_quota,
                )

    def test_quota_increase(self, setup_ui_class):
        """
        Test that quota can be increased in the UI for every client with limited quota
        both by manually setting a new value and by clicking Increment

        """
        storage_clients_page = PageNavigator().nav_to_storageclients_page()
        client_clusters = storageconsumer.get_all_client_clusters()
        for client in client_clusters:
            quota = storage_clients_page.get_client_quota_from_ui(client)
            if quota != "Unlimited":
                new_quota = int(quota) + 1
                assert storage_clients_page.edit_quota(
                    client_cluster_name=client,
                    increase_by_one=False,
                    new_value=new_quota,
                )
                assert storage_clients_page.edit_quota(
                    client_cluster_name=client, increase_by_one=True
                )

    def test_available_capacity_in_quota_edit_popup(self, setup_ui_class):
        """
        Test that Quota edit popup shows correct value of
        Available capacity
        """
        storage_clients_page = PageNavigator().nav_to_storageclients_page()
        ceph_pod = pod.get_ceph_tools_pod()
        ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph df")
        ceph_capacity_bytes = ceph_status["stats"]["total_avail_bytes"]
        ui_capacity = storage_clients_page.get_available_storage_from_quota_edit_popup()
        if "TiB" in ui_capacity:
            ui_capacity_num = float(ui_capacity.split(" ")[0])
            ceph_capacity_tib = ceph_capacity_bytes / 2**40
            assert (ui_capacity_num - ceph_capacity_tib) ** 2 < 0.1

    def test_quota_values_in_ui(self, setup_ui_class):
        """
        Test that all storage clients have correct quota value in the UI
        """
        storage_clients_page = PageNavigator().nav_to_storageclients_page()
        client_clusters = storageconsumer.get_all_client_clusters()
        for client in client_clusters:
            quota_ui = storage_clients_page.get_client_quota_from_ui(client)
            quota_cli = storageconsumer.get_storageconsumer_quota(client)
            assert quota_ui == quota_cli, f"Quota in the UI: {quota_ui}, "
            "quota in the CLI: {quota_cli}"
