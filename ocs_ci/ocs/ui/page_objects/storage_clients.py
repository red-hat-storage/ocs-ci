import logging
from selenium.common.exceptions import WebDriverException
from ocs_ci.ocs.ui.base_ui import take_screenshot, copy_dom, BaseUI

logger = logging.getLogger(__name__)


class StorageClients(BaseUI):
    """
    Storage Client page object under PageNavigator / Storage (version 4.14 and above)
    """

    def __init__(self):
        super().__init__()

    def generate_client_onboarding_ticket(self, quota_value=None, quota_tib=None):
        """
        Generate a client onboarding ticket.
        Starting with version 4.17, client quota can be specified

        Args:
            quota_value (int): client's quota in GiB or TiB, unlimited if not defined
            quota_tib (bool): True if quota is in TiB, False otherwise

        Returns:
            str: onboarding_key
        """
        logger.info("Generating onboarding ticket")
        self.do_click(self.storage_clients_loc["generate_client_onboarding_ticket"])
        if quota_value:
            logger.info("Setting client cluster quota")
            self.do_click(self.storage_clients_loc["custom_quota"])
            self.do_clear(
                locator=self.storage_clients_loc["quota_value"],
            )
            self.do_send_keys(
                locator=self.storage_clients_loc["quota_value"],
                text=quota_value,
            )
            if quota_tib:
                self.do_click(self.storage_clients_loc["choose_units"])
                self.do_click(self.storage_clients_loc["quota_ti"])
        logger.info("Confirming token generation")
        self.do_click(self.storage_clients_loc["confirm_generation"])
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

    def find_client_cluster_index(self, client_cluster_name):
        """
        Find the index of the cluster on Storage clients page
        Filtering clients by name isn't working: https://bugzilla.redhat.com/show_bug.cgi?id=2317212

        Args:
            client_cluster_name(str): name of the hosted cluster

        Returns:
            int: index of the cluster on Storage Clients page

        """
        all_names = [
            element.text
            for element in self.get_elements(self.storage_clients_loc["cluster_name"])
        ]
        for index in range(len(all_names)):
            if client_cluster_name in all_names[index]:
                logger.info(f"Storage client {client_cluster_name} has index {index}")
                return index
        logger.error(
            f"Storage client with cluster name {client_cluster_name} not found"
        )

    def get_client_quota_from_ui(self, client_cluster_name):
        """
        Get client's quota from Storage Client's page
        Args:
            client_cluster_name(str): name of the client cluster
        Returns:
            str: quota of the client
        """
        client_index = self.find_client_cluster_index(client_cluster_name)
        quota_element = self.get_elements(self.storage_clients_loc["quota_percentage"])[
            client_index
        ]
        return quota_element.text

    def get_quota_utilization_from_ui(self, client_cluster_name):
        """
        Get client's quota utilization percentage from Storage Client's page
        as calculated by the size of PVCs
        Args:
            client_cluster_name(str): name of the client cluster
        Returns:
            str: quota utilization of the client
        """
        client_index = self.find_client_cluster_index(client_cluster_name)
        utilization_element = self.get_elements(
            self.storage_clients_loc["client_quota"]
        )[client_index]
        return quota_element.text

    def edit_quota(
        self, client_cluster_name, new_value=None, new_units=False, increase_by_one=True
    ):
        """
        Edit client's storage quota

        Args:
            client_cluster_name(str): name of the client cluster
            new_value(int): new value of the quota
            new_units(bool): True if units need to be changed, False otherwise
            increase_by_one(bool): True if quota needs to be increased by 1, False otherwise

        Returns:
            True if quota change was successful
            False otherwise
        """
        client_index = self.find_client_cluster_index(client_cluster_name)
        self.do_click(
            self.get_elements(self.storage_clients_loc["client_kebab_menu"])[
                client_index
            ]
        )
        try:
            self.do_click(self.storage_clients_loc["edit_quota"])
        except WebDriverException as e:
            logger.info(e)
            logger.info("Quota changes not possble")
            return False
        if increase_by_one:
            self.do_click(self.storage_clients_loc["quota_increment"])
            logger.info("Quota increased by 1")
        else:
            if not new_value:
                logger.error("New quota value not provided")
                return False
            else:
                self.clear_with_ctrl_a_del(self.storage_clients_loc["new_quota"])
                self.do_send_keys(self.storage_clients_loc["new_quota"], text=new_value)
                logger.info(f"Quota value changed to {new_value}")
                if new_units:
                    self.do_click(self.storage_clients_loc["unit_change_button"])
                    self.do_click(self.storage_clients_loc["units_ti"])
                    logger.info("Quota units changed to Ti")
        try:
            self.do_click(self.storage_clients_loc["confirm_quota_change"])
            logger.info("Quota changes saved")
            return True
        except WebDriverException as e:
            logger.info(e)
            logger.info("Quota changes could not be saved")
            return False

    def get_available_storage_from_quota_edit_popup(self):
        """
        Get the value of available storage
        from Edit quota popup

        Returns:
            str: available storage
        """
        self.do_click(
            self.get_elements(self.storage_clients_loc["client_kebab_menu"])[0]
        )
        av_capacity_text = self.get_element_text(
            self.storage_clients_loc["available_storage"]
        )
        # Text is expected to be 'Available capacity (ocs-storagecluster): N TiB'
        split_capacity_text = av_capacity_text.split(" ")
        return f"{split_capacity_text[-2]} {split_capacity_text[-1]}"

    def validate_unlimited_quota_utilization_info(self):
        """
        Verify that for every client with unlimited quota
        utilization column only shows "-"
        """
        quota_elements = self.get_elements(self.storage_clients_loc["client_quota"])
        utilization_elements = self.get_elements(
            self.storage_clients_loc["quota_utilization"]
        )
        for i in len(quota_elements):
            if quota_elements[i].text == "Unlimited":
                assert (
                    utilization_elements[i].text == "-"
                ), f"Quota utilization is shown as {utilization_elements[i].text}"

    def validate_quota_utilization(self, index, value=0):
        """
        Verify that the quota utilization of the client
        has expected value

        Args:
            index(int): index of the client on clients page
            value(int): expected quota utilization value
        """
        utilization_elements = self.get_elements(
            self.storage_clients_loc["quota_utilization"]
        )
        value_str = f"{value}%"
        assert utilization_elements[index].text == value_str
