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
        StorageClientUI.verify_clients_on_dashboard()
