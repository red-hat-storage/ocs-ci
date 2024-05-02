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
        logget.info("Click on 'Generate client onboarding token'")
        self.do_click(self.client_loc["generate_token"])
        token = self.get_element_text(self.client_loc["token"])
        return token
