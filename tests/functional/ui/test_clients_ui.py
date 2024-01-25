import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, ui
from ocs_ci.framework.pytest_customization.marks import black_squad
from ocs_ci.ocs.ui.client_ui import verify_clients_on_dashboard


@ui
@black_squad
class TestClientUI(ManageTest):
    """
    Test Client UI in Provider/Client deployments

    """

    def test_clients_on_dashboard(self, setup_ui_class):
        """
        Test that the number of connected or disconnected clients on the dashboard is correct

        """
        verify_clients_on_dashboard()
