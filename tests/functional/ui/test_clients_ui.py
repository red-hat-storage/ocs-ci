import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    ui,
    runs_on_provider,
    hci_provider_and_client_required,
)
from ocs_ci.framework.pytest_customization.marks import black_squad
from ocs_ci.ocs.ui.provider_client_ui import StorageClientUI

logger = logging.getLogger(__name__)


@ui
@black_squad
class TestStorageClientUI(ManageTest):
    """
    Test Storage Clients page of Provider cluster UI in Provider/Client deployments

    """

    @hci_provider_and_client_required
    @runs_on_provider
    def test_clients_on_dashboard(self, setup_ui_class):
        """
        Test that the number of connected and disconnected
        clients on the dashboard is correct

        """
        logger.info("Verifying number of clients on the dashboard")
        storageclient_obj = StorageClientUI()
        storageclient_obj.verify_clients_on_dashboard()

    @hci_provider_and_client_required
    def test_storage_clients_page(self, setup_ui_class):
        """
        Test the values of Storage Clients page:
        Storage client name, cluster ID, OCP and ODF versions, heartbeat

        """
        logger.info("Verifying values on Storage Clients page")
        storageclient_obj = StorageClientUI()
        storageclient_obj.verify_client_data_in_ui()

    @hci_provider_and_client_required
    def test_token_explanation(self, setup_ui_class):
        """
        Test the presense of token explanation
        on the token generation modal
        """
        logger.info("Verifying presense  of token explanation")
        storageclient_obj = StorageClientUI()
        token_explanation = storageclient_obj.get_token_description()
        assert (
            "To onboard the client cluster, the provider cluster requires the onboarding token"
            in token_explanation
        ), f"{token_explanation} doesn't have expected text"
