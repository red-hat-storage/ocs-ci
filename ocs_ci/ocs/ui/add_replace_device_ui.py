import logging
import time

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.views import ODF_OPERATOR


logger = logging.getLogger(__name__)


class AddReplaceDeviceUI(PageNavigator):
    """
    InfraUI class for add capacity, device replacement, node replacement

    """

    def __init__(self):
        super().__init__()

    def add_capacity_ui(self):
        """
        Add Capacity via UI

        """
        self.navigate_installed_operators_page()
        if self.operator_name is ODF_OPERATOR:
            self.do_click(self.add_capacity_ui_loc["odf_operator"])
            self.do_click(self.add_capacity_ui_loc["storage_system_tab"])
        else:
            self.do_click(self.add_capacity_ui_loc["ocs_operator"])
            self.do_click(self.add_capacity_ui_loc["storage_cluster_tab"])
        time.sleep(1)
        logger.info("Click on kebab menu of Storage Systems")
        self.do_click(self.add_capacity_ui_loc["kebab_storage_cluster"])
        self.take_screenshot()
        logger.info("Click on Add Capacity button under the kebab menu")
        self.wait_until_expected_text_is_found(
            locator=self.add_capacity_ui_loc["add_capacity_button"],
            timeout=10,
            expected_text="Add Capacity",
        )
        self.take_screenshot()
        self.do_click(self.add_capacity_ui_loc["add_capacity_button"])
        self.take_screenshot()
        self.do_click(
            self.add_capacity_ui_loc["confirm_add_capacity"], enable_screenshot=True
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
                locator=self.add_capacity_ui_loc["filter_pods"], text=pod_name
            )
            logger.info(f"Verify {pod_name} move to {pod_state} state")
            assert self.check_element_text(
                pod_state
            ), f"{pod_name} does not on {pod_state} state"
            # WA clear item-filter line
            self.navigate_installed_operators_page()
