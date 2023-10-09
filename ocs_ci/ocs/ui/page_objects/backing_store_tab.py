from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    CreateResourceForm,
)
from ocs_ci.ocs.ui.page_objects.object_storage import ObjectStorage
from ocs_ci.ocs.ui.page_objects.resource_list import logger


class BackingStoreTab(ObjectStorage, CreateResourceForm):
    def __init__(self):
        ObjectStorage.__init__(self)
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

    def nav_to_backing_store(self, backing_store_name: str):
        """
        Navigate to backing store page

        Args:
            backing_store_name (str): backing store name

        """
        logger.info(f"Navigate to backing store {backing_store_name}")
        self.nav_to_resource_via_name(backing_store_name)

    def validate_backing_store_ready(self) -> bool:
        """
        Validate backing store is Ready
        Returns:
            bool: True if backing store is Ready, False otherwise
        """
        logger.info("Verifying the status of noobaa backing store is Ready")
        backingstore_status = self.get_element_text(
            self.validation_loc["bucketclass-status"]
        )
        is_ready = backingstore_status == "Ready"
        if not is_ready:
            logger.warning(
                f"Backing store status is {backingstore_status} and not Ready"
            )
        return is_ready

    def nav_backing_store_list_breadcrumb(self):
        """
        Navigate to backing store breadcrumbs

        """
        logger.info("Click on backingstore breadcrumb")
        self.do_click(
            (self.validation_loc["backingstorage-breadcrumb"]), enable_screenshot=True
        )
