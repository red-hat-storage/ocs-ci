from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import IncorrectUiOptionRequested
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationTabBar,
    CreateResourceForm,
)


class StorageSystemTab(DataFoundationTabBar, CreateResourceForm):
    """
    Storage System tab Class
    Content of Data Foundation/Storage Systems tab

    """

    def __init__(self):
        DataFoundationTabBar.__init__(self)
        CreateResourceForm.__init__(self)
        self.rules = {
            constants.UI_INPUT_RULES_STORAGE_SYSTEM[
                "rule1"
            ]: self._check_max_length_backing_store_rule,
            constants.UI_INPUT_RULES_STORAGE_SYSTEM[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_STORAGE_SYSTEM[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_STORAGE_SYSTEM[
                "rule4"
            ]: self._check_storage_system_not_used_before_rule,
        }
        self.name_input_loc = self.sc_loc["sc-name"]

    def fill_backing_storage_form(self, backing_store_type: str, btn_text: str):
        """
        Storage system creation form consists from several forms, showed one after another when mandatory fields filled
        and Next btn clicked.
        Function to fill first form in order to create new Backing store.


        Args:
            backing_store_type (str): options available when filling backing store form (1-st form)
            btn_text (str): text of the button to be clicked after the form been filled ('Next', 'Back', 'Cancel')
        """
        option_1 = "Use an existing StorageClass"
        option_2 = "Create a new StorageClass using local storage devices"
        option_3 = "Connect an external storage platform"
        if backing_store_type not in [option_1, option_2, option_3]:
            raise IncorrectUiOptionRequested(
                f"Choose one of the existed option: '{[option_1, option_2, option_3]}'",
                lambda: self.take_screenshot(),
            )

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        self.do_click(
            format_locator(self.sc_loc["backing_store_type"], backing_store_type)
        )

        btn_1 = "Next"
        btn_2 = "Back"
        btn_3 = "Cancel"
        if btn_text not in [btn_1, btn_2, btn_3]:
            raise IncorrectUiOptionRequested(
                f"Choose one of the existed option: '{[btn_1, btn_2, btn_3]}'",
                lambda: self.take_screenshot(),
            )

        self.do_click(format_locator(self.sc_loc["button_with_txt"], btn_text))

    def _check_storage_system_not_used_before_rule(self, rule_exp) -> bool:
        """
        Checks whether the storage system name allowed to use again.

        This function executes an OpenShift command to retrieve the names of all existing storage systems
        in all namespaces.
        It then checks whether the name of the existed storage system would be allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if not allowed to use duplicated storage system name, False otherwise.
        """
        existing_storage_systems_names = str(
            OCP().exec_oc_cmd(
                "get storageclass --all-namespaces -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_storage_systems_names, rule_exp
        )

    def nav_storagecluster_storagesystem_details(self):
        """
        Initial page - Data Foundation / Storage Systems tab
        Navigate to StorageSystem details

        """
        if not config.DEPLOYMENT.get("external_mode"):
            logger.info(
                "Click on 'ocs-storagecluster-storagesystem' link from Storage Systems page"
            )
            self.do_click(
                self.validation_loc["ocs-storagecluster-storagesystem"],
                enable_screenshot=True,
            )
        else:
            logger.info(
                "Click on 'ocs-external-storagecluster-storagesystem' link "
                "from Storage Systems page for External Mode Deployment"
            )
            self.do_click(
                self.validation_loc["ocs-external-storagecluster-storagesystem"],
                enable_screenshot=True,
            )

        from ocs_ci.ocs.ui.page_objects.storage_system_details import (
            StorageSystemDetails,
        )

        return StorageSystemDetails()
