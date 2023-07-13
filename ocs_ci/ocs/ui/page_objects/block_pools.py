from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CephHealthException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import CreateResourceForm
from ocs_ci.ocs.ui.page_objects.storage_system_details import StorageSystemDetails
from ocs_ci.ocs.ui.page_objects.storage_system_tab import StorageSystemTab


class BlockPools(StorageSystemDetails, CreateResourceForm):
    def __init__(self):
        StorageSystemTab.__init__(self)
        CreateResourceForm.__init__(self)
        self.name_input_loc = self.validation_loc["blockpool_name"]
        self.rules = {
            constants.UI_INPUT_RULES_BLOCKING_POOL[
                "rule1"
            ]: self._check_max_length_backing_store_rule,
            constants.UI_INPUT_RULES_BLOCKING_POOL[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_BLOCKING_POOL[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_BLOCKING_POOL[
                "rule4"
            ]: self._check_blockpool_not_used_before_rule,
        }

    def _check_blockpool_not_used_before_rule(self, rule_exp) -> bool:
        """
        Checks whether the blockpool name allowed to use again.

        This function executes an OpenShift command to retrieve the names of all existing blockpools in all namespaces.
        It then checks whether the name of the existed namespace store would be allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if not allowed to use duplicated blockpool name, False otherwise.
        """

        existing_blockpool_names = str(
            OCP().exec_oc_cmd(
                "get CephBlockPool --all-namespaces -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_blockpool_names, rule_exp
        )

    def verify_cephblockpool_status(self, status_exp: str = "Ready"):
        logger.info(f"Verifying the status of '{constants.DEFAULT_CEPHBLOCKPOOL}'")
        cephblockpool_status = self.get_element_text(
            self.validation_loc[f"{constants.DEFAULT_CEPHBLOCKPOOL}-status"]
        )
        if not status_exp == cephblockpool_status:
            raise CephHealthException(
                f"cephblockpool status error | expected status:Ready \n "
                f"actual status:{cephblockpool_status}"
            )
