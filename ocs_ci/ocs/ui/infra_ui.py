import logging

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.framework import config
from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


class InfraUI(PageNavigator):
    """
    InfraUI Class

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.infra_loc = locators[ocp_version]["infra"]
        if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            self.storage_class = "thin_sc"
        elif config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            self.storage_class = "gp2_sc"

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
