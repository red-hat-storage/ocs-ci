from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CephHealthException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.page_objects.ceph_block_pool import CephBlockPool
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import CreateResourceForm
from ocs_ci.ocs.ui.page_objects.resource_list import ResourceList
from ocs_ci.ocs.ui.page_objects.edit_label_form import EditLabelForm
from ocs_ci.utility import version


class StoragePools(CreateResourceForm, EditLabelForm, ResourceList):
    """
    Class to represent the Storage Pools page and its functionalities.
    """

    def __init__(self, pool_existed: list = None):
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
        self.block_pool_existed = pool_existed

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
        """
        Verifies the status of the default cephblockpool

        Args:
            status_exp (str): Expected status of the cephblockpool, default is "Ready

        """
        logger.info(f"Verifying the status of '{constants.DEFAULT_CEPHBLOCKPOOL}'")
        cephblockpool_status = self.get_element_text(
            self.validation_loc[f"{constants.DEFAULT_CEPHBLOCKPOOL}-status"]
        )
        if not status_exp == cephblockpool_status:
            raise CephHealthException(
                f"cephblockpool status error | expected status:Ready \n "
                f"actual status:{cephblockpool_status}"
            )

    def verify_cephfs_status(self, status_exp: str = "Ready"):
        """
        Verifies the status of the default cephfilesystem

        Raises:
            CephHealthException: If the cephfilesystem status is not as expected

        """
        logger.info(f"Verifying the status of '{constants.DEFAULT_CEPHFS_DATA_POOL}'")
        cephfs_status = self.get_element_text(
            self.validation_loc[f"{constants.DEFAULT_CEPHFS_DATA_POOL}-status"]
        )
        if not status_exp == cephfs_status:
            raise CephHealthException(
                f"cephfilesystem status error | expected status:Ready \n "
                f"actual status:{cephfs_status}"
            )

    def delete_block_pool(self, block_pool_name: str, cannot_be_deleted: bool = False):
        """
        Deletes the block pool, does not verify the deletion, but verifies the alert if the block pool cannot be deleted

        Args:
            block_pool_name (str): Name of the block pool to be deleted
            cannot_be_deleted (bool): Whether the block pool cannot be deleted

        Returns:
            bool: True if the block pool delete via UI performed, False otherwise
        """
        logger.info(f"Deleting the block pool: {block_pool_name}")
        self.select_search_by("name")
        self.search(block_pool_name)

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        resource_actions = format_locator(
            self.generic_locators["actions_of_resource_from_list"],
            block_pool_name,
            block_pool_name,
        )
        self.do_click(resource_actions, enable_screenshot=True)
        self.do_click(self.generic_locators["delete_resource"], enable_screenshot=True)

        if cannot_be_deleted:
            logger.info(
                f"Block pool {block_pool_name} cannot be deleted. Verifying alert"
            )
            self.check_element_presence(
                self.bp_loc["pool_cannot_be_deleted_warning"][::-1]
            )
            warning_text = self.get_element_text(
                self.bp_loc["pool_cannot_be_deleted_warning"]
            )
            logger.info(f"Warning text: {warning_text}. Close warning modal")
            self.do_click(self.generic_locators["close_modal_btn"])
            return False

        logger.info(f"Confirm {block_pool_name} Deletion")
        self.do_click(self.generic_locators["confirm_action"], enable_screenshot=True)
        return True

    def is_block_pool_exist(self, block_pool_name: str):
        """
        Checks if the block pool exists in the list

        Args:
            block_pool_name (str): Name of the block pool
        """
        logger.info(f"Checking if the block pool {block_pool_name} exists")

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        block_pool_from_list = format_locator(
            self.generic_locators["resource_from_list_by_name"], block_pool_name
        )

        return self.check_element_presence(block_pool_from_list[::-1], timeout=10)

    def proceed_resource_creation(self):
        super().proceed_resource_creation()
        if self.ocs_version_semantic >= version.VERSION_4_17:
            self.do_click(self.bp_loc["pool_type_block"])

    def navigate_to_block_pool(self, block_pool_name: str):
        """
        Navigate to the specific block pool details page

        Args:
            block_pool_name (str): Name of the block pool

        Returns:
            BlockPoolDetails: BlockPoolDetails page object

        """
        logger.info(
            f"Navigate to the specific block pool details page {block_pool_name}"
        )
        self.nav_to_resource_via_name(block_pool_name)

        return CephBlockPool()
