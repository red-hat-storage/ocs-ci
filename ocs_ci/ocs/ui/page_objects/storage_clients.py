import logging

from ocs_ci.ocs.ui.base_ui import take_screenshot, copy_dom, BaseUI
from ocs_ci.utility import version
from ocs_ci.ocs.ui.validation_ui import ValidationUI

logger = logging.getLogger(__name__)


class StorageClients(BaseUI):
    """
    Storage Client page object under PageNavigator / Storage (version 4.14 and above)
    """

    def __init__(self):
        super().__init__()
        self.ocs_version = version.get_semantic_ocs_version_from_config()

    def generate_client_onboarding_ticket_ui(self, storage_quota=None):
        """
        Generate a client onboarding ticket

        Returns:
            str: onboarding_key
        """
        self.do_click(self.storage_clients_loc["generate_client_onboarding_ticket"])
        ValidationUI().verify_storage_clients_page()
        if storage_quota and self.ocs_version >= version.VERSION_4_17:
            self.do_click(
                self.validation_loc["storage_quota_custom"],
                enable_screenshot=True,
            )
            self.do_clear(self.validation_loc["allocate_quota_value"])
            self.do_send_keys(
                locator=self.validation_loc["allocate_quota_value"], text=storage_quota
            )
            self.do_click(
                self.validation_loc["quota_unit_dropdown"], enable_screenshot=True
            )
            self.do_click(
                self.storage_clients_loc["generate_token"], enable_screenshot=True
            )

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
