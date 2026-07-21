from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CephHealthException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import logger
from ocs_ci.ocs.ui.helpers_ui import format_locator
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
        logger.info(f"Deleting the pool: {block_pool_name}")
        self.select_search_by("name")
        self.clear_search()
        self.search(block_pool_name)

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

        block_pool_from_list = format_locator(
            self.generic_locators["resource_from_list_by_name"], block_pool_name
        )

        return self.check_element_presence(block_pool_from_list[::-1], timeout=10)

    def proceed_resource_creation(self, volume_type="block"):
        """
        Proceeds to resource creation form and selects the volume type.

        Args:
            volume_type (str): 'block' or 'filesystem'. Default is 'block'.
        """
        super().proceed_resource_creation()
        if self.ocs_version_semantic >= version.VERSION_4_17:
            if (
                volume_type == "filesystem"
                and self.ocs_version_semantic >= version.VERSION_4_22
            ):
                self.do_click(self.bp_loc["pool_type_filesystem"])
            else:
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

    def select_data_protection(self, protection_type):
        """
        Selects data protection policy on the pool creation form.

        Args:
            protection_type (str): 'replication' or 'erasure_coding'
        """
        locator_key = f"policy_{protection_type}"
        logger.info(f"Selecting data protection policy: {protection_type}")
        self.do_click(self.bp_loc[locator_key], enable_screenshot=True)

    def get_available_ec_schemes(self):
        """
        Retrieves the list of available EC schemes from the scheme selection table.

        Returns:
            list[dict]: Each dict has 'scheme' (str), 'overhead' (str),
                        and 'recommended' (bool).
        """
        self.wait_for_element_to_be_visible(
            self.bp_loc["ec_scheme_table_rows"], timeout=15
        )
        rows = self.get_elements(self.bp_loc["ec_scheme_table_rows"])
        schemes = []
        for row in rows:
            scheme_cell = row.find_element(
                self.bp_loc["ec_row_scheme_cell"][1],
                self.bp_loc["ec_row_scheme_cell"][0],
            )
            scheme_text = scheme_cell.text.strip()
            has_badge = bool(
                row.find_elements(
                    self.bp_loc["ec_recommended_badge"][1],
                    self.bp_loc["ec_recommended_badge"][0],
                )
            )
            recommended = has_badge and "Recommended" in scheme_cell.text
            overhead_cell = row.find_element(
                self.bp_loc["ec_row_overhead_cell"][1],
                self.bp_loc["ec_row_overhead_cell"][0],
            )
            scheme_name = scheme_text.replace("Recommended", "").strip()
            schemes.append(
                {
                    "scheme": scheme_name,
                    "overhead": overhead_cell.text.strip(),
                    "recommended": recommended,
                }
            )
        logger.info(f"Available EC schemes: {schemes}")
        return schemes

    def select_ec_scheme(self, scheme):
        """
        Selects an erasure coding scheme from the scheme table.

        Args:
            scheme (str): EC scheme identifier (e.g., '4+2')
        """
        logger.info(f"Selecting EC scheme: {scheme}")
        locator = format_locator(self.bp_loc["ec_scheme_radio"], scheme)
        self.do_click(locator, enable_screenshot=True)

    def get_recommended_ec_scheme(self):
        """
        Returns the scheme string marked as 'Recommended'.

        Returns:
            str or None: The recommended scheme string, or None if not found.
        """
        schemes = self.get_available_ec_schemes()
        for s in schemes:
            if s["recommended"]:
                return s["scheme"]
        return None

    def create_ec_pool(
        self,
        pool_name=None,
        volume_type="block",
        ec_scheme=None,
        compression=True,
    ):
        """
        Create an erasure coded storage pool via UI.

        Args:
            pool_name (str): Pool name suffix. Auto-generated if None.
            volume_type (str): 'block' or 'filesystem'.
            ec_scheme (str): EC scheme like '4+2'. If None, selects Recommended.
            compression (bool): Enable compression checkbox.

        Returns:
            tuple: (full_pool_name, pool_status_ready)
        """
        if pool_name is None:
            pool_name = create_unique_resource_name("test", "ec-pool")

        logger.info(
            f"Creating EC pool: name={pool_name}, type={volume_type}, "
            f"scheme={ec_scheme}, compression={compression}"
        )

        self.proceed_resource_creation(volume_type=volume_type)

        self.do_send_keys(self.bp_loc["pool_name_input"], pool_name)

        self.select_data_protection("erasure_coding")

        if ec_scheme is None:
            schemes = self.get_available_ec_schemes()
            recommended = [s["scheme"] for s in schemes if s["recommended"]]
            if not recommended:
                raise ValueError(
                    "No recommended EC scheme found and ec_scheme not specified. "
                    f"Available schemes: {[s['scheme'] for s in schemes]}"
                )
            ec_scheme = recommended[0]
        self.select_ec_scheme(ec_scheme)

        if compression:
            self.do_click(self.bp_loc["conpression_checkbox"])

        self.do_click(self.bp_loc["pool_confirm_create"])

        wait_for_text_result = self.wait_until_expected_text_is_found(
            self.bp_loc["pool_state_inside_pool"], "Ready", timeout=60
        )

        full_pool_name = pool_name
        if volume_type == "filesystem":
            full_pool_name = f"ocs-storagecluster-cephfilesystem-{pool_name}"

        if wait_for_text_result:
            logger.info(f"EC pool {full_pool_name} created and is Ready")
        else:
            logger.warning(f"EC pool {full_pool_name} created but did not reach Ready")

        return full_pool_name, wait_for_text_result

    def verify_pool_ec_scheme_in_list(self, pool_name, expected_scheme):
        """
        Verifies the pool's EC scheme display in the Storage pools list.

        Args:
            pool_name (str): Full pool name as shown in the list.
            expected_scheme (str): Expected scheme like '4+2'.

        Returns:
            bool: True if the scheme is displayed correctly.
        """
        logger.info(
            f"Verifying EC scheme for pool {pool_name} in list: "
            f"expecting {expected_scheme}"
        )
        replicas_locator = format_locator(
            self.bp_loc["pool_replicas_in_list"], pool_name
        )
        replicas_text = self.get_element_text(replicas_locator)
        logger.info(f"Pool {pool_name} replicas text: '{replicas_text}'")

        has_ec = "Erasure coding" in replicas_text
        has_scheme = expected_scheme in replicas_text
        return has_ec and has_scheme
