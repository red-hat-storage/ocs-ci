import logging

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.views import ODF_OPERATOR


logger = logging.getLogger(__name__)


class AddReplaceDeviceUI(PageNavigator):
    """
    InfraUI class for add capacity, device replacement, node replacement

    """

    def __init__(self, driver):
        super().__init__(driver)

    def add_capacity_ui(self):
        """
        Add Capacity via UI.

        """
        self.add_capacity_ui = locators[self.ocp_version]["add_capacity"]
        self.navigate_installed_operators_page()
        if self.operator_name is ODF_OPERATOR:
            self.do_click(self.add_capacity_ui["odf_operator"])
            self.do_click(self.add_capacity_ui["storage_system_tab"])
        else:
            self.do_click(self.add_capacity_ui["ocs_operator"])
            self.do_click(self.add_capacity_ui["storage_cluster_tab"])
        self.do_click(self.add_capacity_ui["kebab_storage_cluster"])
        self.do_click(self.add_capacity_ui["add_capacity_button"])
        self.do_click(
            self.add_capacity_ui["select_sc_add_capacity"], enable_screenshot=True
        )
        self.do_click(self.add_capacity_ui[self.storage_class], enable_screenshot=True)
        self.do_click(
            self.add_capacity_ui["confirm_add_capacity"], enable_screenshot=True
        )

    def verify_pod_status(self, pod_names, pod_state="Running"):
        """
        Verify pod status

        Args:
            pod_names (list): list of pod names
            pod_state (string): the desired pod state

        """
        for pod_name in pod_names:
            self.navigate_pods_page()
            self.do_send_keys(
                locator=self.add_capacity_ui["filter_pods"], text=pod_name
            )
            logger.info(f"Verify {pod_name} move to {pod_state} state")
            assert self.check_element_text(
                pod_state
            ), f"{pod_name} does not on {pod_state} state"
            # WA clear item-filter line
            self.navigate_installed_operators_page()
