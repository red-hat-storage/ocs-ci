from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationDefaultTab,
    CreateResourceForm,
)


class BackingStoreTab(DataFoundationDefaultTab, CreateResourceForm):
    def __init__(self):
        DataFoundationDefaultTab.__init__(self)
        CreateResourceForm.__init__(self)
        self.rules = {
            constants.UI_INPUT_RULES_BACKING_STORE[
                "rule1"
            ]: self._check_max_length_backing_store_rule,
            constants.UI_INPUT_RULES_BACKING_STORE[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_BACKING_STORE[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_BACKING_STORE[
                "rule4"
            ]: self._check_backingstore_name_not_used_before_per_namespace_rule,
        }
        self.name_input_loc = self.validation_loc["backingstore_name"]

    def _check_backingstore_name_not_used_before_per_namespace_rule(self, rule_exp):
        """
        Checks if a backing store name per namespace is not allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            A boolean value indicating whether the check passed or not.
        """
        existing_backingstore_names = str(
            OCP().exec_oc_cmd(
                f"get backingstore -n {config.ENV_DATA['cluster_namespace']} -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_backingstore_names, rule_exp
        )
