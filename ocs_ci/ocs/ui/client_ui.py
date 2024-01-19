import logging

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


class ClientUI(PageNavigator):
    """
    User Interface Selenium for Clients page
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
        self.do_click(self.client_loc["generate_token"])
        token = self.get_element_text(self.client_loc["token"])
        return token

    def verify_client_ui(
        self, client_name, cluster_id, ocp_version, odf_version, heartbeat
    ):
        """
        Verify client details on Clients page

        Args:
            client_name (str): name of the client
            cluster_id (str): client's cluster ID
            ocp_version (str): OCP version of the client
            odf_version (str): ODF version of the client
            heartbeat (str): last heartbeat of the client in the form "X minutes ago"

        """
        self.navigate_client_page()
        logger.info(f"Search for {client_name} client")
        self.do_send_keys(self.client_loc["search_client"], text=client_name)
        time.sleep(2)
