import logging

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version


logger = logging.getLogger(__name__)


class AddReplaceDeviceUI(PageNavigator):
    """
    InfraUI class for add capacity, device replacement, node replacement

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.infra_loc = locators[ocp_version]["infra"]

    def add_capacity_ui(self):
        """
        Add Capacity via UI.

        """
        self.navigate_installed_operators_page()
        self.do_click(self.infra_loc["ocs_operator"])
        self.do_click(self.infra_loc["storage_cluster_tab"])
        self.do_click(self.infra_loc["kebab_storage_cluster"])
        self.do_click(self.infra_loc["add_capacity_button"])
        self.do_click(self.infra_loc["select_sc_add_capacity"])
        self.do_click(self.infra_loc[self.storage_class])
        self.do_click(self.infra_loc["confirm_add_capacity"])

    def verify_pod_status(self, pod_names, pod_state="Running"):
        """
        Verify pod status

        Args:
            pod_names (list): list of pod names
            pod_state (string): the desired pod state

        """
        for pod_name in pod_names:
            self.navigate_pods_page()
            self.do_send_keys(locator=self.infra_loc["filter_pods"], text=pod_name)
            logger.info(f"Verify {pod_name} move to {pod_state} state")
            assert self.check_element_text(
                pod_state
            ), f"{pod_name} does not on {pod_state} state"
            # WA clear item-filter line
            self.navigate_installed_operators_page()
