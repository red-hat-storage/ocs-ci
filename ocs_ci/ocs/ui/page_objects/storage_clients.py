import logging

from ocs_ci.ocs.ui.base_ui import take_screenshot, copy_dom, BaseUI

logger = logging.getLogger(__name__)


class StorageClients(BaseUI):
    """
    Storage Client page object under PageNavigator / Storage (version 4.14 and above)
    """

    def __init__(self):
        super().__init__()

    def generate_client_onboarding_ticket(self):
        """
        Generate a client onboarding ticket

        Returns:
            str: onboarding_key
        """
        self.do_click(self.storage_clients_loc["generate_client_onboarding_ticket"])
        onboarding_key = self.get_element_text(
            self.storage_clients_loc["onboarding_key"]
        )
        if len(onboarding_key):
            logger.info("Client onboarding ticket generated")
        else:
            logger.error("Client onboarding ticket generation failed")

        take_screenshot("onboarding_token_modal")
        copy_dom("onboarding_token_modal")

        self.close_onboarding_token_modal()

        return onboarding_key

    def close_onboarding_token_modal(self):
        """
        Close the onboarding token modal
        """
        self.do_click(self.storage_clients_loc["close_token_modal"])
