import logging
import time

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.resources.storageconsumer import (
    get_all_storageconsumer_names,
    StorageConsumer,
)

logger = logging.getLogger(__name__)


class StorageClientUI(PageNavigator):
    """
    User Interface Selenium for Storage Clients page of Provider cluster UI
    """

    def __init__(self):
        super().__init__()

    def generate_new_token(self):
        """
        Create a new client onboarding token in the UI

        Return:
            string: onboarding token

        """
        self.navigate_client_page()
        logger.info("Click on 'Generate client onboarding token'")
        self.do_click(self.validation["generate_token"])
        token = self.get_element_text(self.validation["token"])
        return token

    def verify_client_data_in_ui(
        self, client_name, ocp_version, odf_version, heartbeat
    ):
        """
        Verify client details on Storage Clients page

        Args:
            client_name (str): name of the client
            ocp_version (str): OCP version of the client
            odf_version (str): ODF version of the client
            heartbeat (str): last heartbeat of the client in the form "X minutes ago"

        """
        self.navigate_client_page()
        logger.info(f"Search for {client_name} client")
        self.do_send_keys(self.validation["search_client"], text=client_name)
        time.sleep(2)
        client_name_ui = self.get_element_text(self.validation["client_name"])
        assert (
            client_name_ui == client_name
        ), f"Client name in the UI is {client_name_ui}. It should be {client_name}"
        ocp_version_ui = self.get_element_text(self.validation["client_ocp_version"])
        assert (
            ocp_version_ui == ocp_version
        ), f"OCP version in the UI is {ocp_version_ui}. It should be {ocp_version}"

    def get_number_of_clients_from_dashboard(self):
        """
        Get the number of connected clients and the total number of clients
        from Storage Dashboard

        Returns:
            list: number of connected clients and total number of clients

        """
        self.nav_object_storage()
        clients_info = self.get_element_text(
            self.validation["clients_number_on_dashboad"]
        )
        connected_clients = clients_info.split(" ")[0]
        total_clients = clients_info.split(" ")[2]
        return [connected_clients, total_clients]

    def verify_clients_on_dashboard(self):
        """
        Verify that the total number of clients and the number of connected clients
        on Storage Dashboard are correct

        """
        consumer_names = get_all_storageconsumer_names()
        connected_clients, total_clients = self.get_number_of_clients_from_dashboard()
        assert len(consumer_names) == int(total_clients), (
            f"Total number of clients on the dashboard: {total_clients}"
            f"Total number of storageconsumers: {len(consumer_names)}"
        )
        clients_with_heartbeat = 0
        for consumer_name in consumer_names:
            client = StorageConsumer(consumer_name)
            if client.is_heartbeat_ok():
                clients_with_heartbeat += 1
        assert clients_with_heartbeat == int(connected_clients), (
            f"Number of connected clients on the dashboard: {connected_clients}",
            f"Number of clients with recent heartbeat: {clients_with_heartbeat}",
        )
