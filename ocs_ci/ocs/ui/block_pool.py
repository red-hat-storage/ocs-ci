import logging
import time

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from selenium.webdriver.common.by import By
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.exceptions import PoolStateIsUnknow
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.ocs.ui.page_objects.block_and_file import BlockAndFile
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


class BlockPoolUI(PageNavigator):
    """
    User Interface Selenium for Block Pools page

    """

    def __init__(self):
        super().__init__()

    def create_pool(self, replica, compression, pool_type_block=True):
        """
        Create block pool via UI

        Args:
            pool_type_block: True if type of storage pool is block, False otherwise
            replica (int): replica size usually 2,3
            compression (bool): True to enable compression otherwise False

        Return:
            array: pool name (str) pool status (bool) #pool can be created with failure status

        """
        pool_name = create_unique_resource_name("test", "rbd-pool")
        self.navigate_block_pool_page()
        self.do_click(self.bp_loc["create_block_pool"])
        if pool_type_block and self.ocs_version_semantic >= version.VERSION_4_17:
            self.do_click(self.bp_loc["pool_type_block"])
        self.do_send_keys(self.bp_loc["new_pool_name"], pool_name)
        self.do_click(self.bp_loc["first_select_replica"])
        if replica == 2:
            self.do_click(self.bp_loc["second_select_replica_2"])
        else:
            self.do_click(self.bp_loc["second_select_replica_3"])
        if compression is True:
            self.do_click(self.bp_loc["conpression_checkbox"])
        self.do_click(self.bp_loc["pool_confirm_create"])
        wait_for_text_result = self.wait_until_expected_text_is_found(
            self.bp_loc["pool_state_inside_pool"], "Ready", timeout=30
        )
        if not pool_type_block:
            pool_name = f"ocs-storagecluster-cephfilesystem-{pool_name}"
        if wait_for_text_result is True:
            logger.info(f"Pool {pool_name} was created and it is in Ready state")
            return [pool_name, True]
        else:
            logger.info(f"Pool {pool_name} was created but did not reach Ready state")
            return [pool_name, False]

    def check_pool_existence(self, pool_name):
        """
        Check if pool appears in the block pool list

        Args:
            pool_name (str): Name of the pool to check

        Return:
            bool: True if pool is in the list of pools page, otherwise False

        """
        self.navigate_block_pool_page()
        self.page_has_loaded()
        pool_existence = self.wait_until_expected_text_is_found(
            (f"a[data-test={pool_name}]", By.CSS_SELECTOR), pool_name, 5
        )
        logger.info(f"Pool name {pool_name} existence is {pool_existence}")
        return pool_existence

    def delete_pool(self, pool_name):
        """
        Delete pool from pool page

        Args:
            pool_name (str): The name of the pool to be deleted

        Returns:
            bool: True if pool is not found in pool list, otherwise false

        """

        self.navigate_block_pool_page()
        self.page_has_loaded()
        self.do_click((f"{pool_name}", By.LINK_TEXT))
        self.do_click(self.bp_loc["actions_inside_pool"])
        self.do_click(self.bp_loc["delete_pool_inside_pool"])
        self.do_click(self.bp_loc["confirm_delete_inside_pool"])
        # wait for pool to deleted
        time.sleep(2)
        return not self.check_pool_existence(pool_name)

    def edit_pool_parameters(self, pool_name, replica=3, compression=True):
        """
        Edit an already existing pool

        Args:
            pool_name (str): The name of the pool to change.
            replica (int): size of replica. Available in OCS now 2,3.
            compression (bool): True if enable compression. False otherwise.

        """
        self.navigate_block_pool_page()
        self.page_has_loaded()
        self.do_click([f"{pool_name}", By.LINK_TEXT])
        self.do_click(self.bp_loc["actions_inside_pool"])
        self.do_click(self.bp_loc["edit_pool_inside_pool"])
        self.do_click(self.bp_loc["replica_dropdown_edit"])
        if replica == 2:
            self.do_click(self.bp_loc["second_select_replica_2"])
        else:
            self.do_click(self.bp_loc["second_select_replica_3"])
        compression_checkbox_status = self.get_checkbox_status(
            self.bp_loc["compression_checkbox_edit"]
        )
        if compression != compression_checkbox_status:
            self.do_click(self.bp_loc["compression_checkbox_edit"])
        self.do_click(self.bp_loc["save_pool_edit"])

    def reach_pool_limit(self, replica, compression):
        """
        Add pools till pool fails because of pg limit.

        Args:
             replica (int): size of pool.
             compression (bool): True for enabling compression. Otherwise False.

        """
        pool_list = []
        ceph_pod = pod.get_ceph_tools_pod()
        count = 0
        while count < 50:
            count += 1
            pool_name, pool_status = self.create_pool(replica, compression)
            pool_list.append(pool_name)
            if pool_status is True:
                ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph status")
                total_pg_count = ceph_status["pgmap"]["num_pgs"]
                logger.info(f"Total pg count is {total_pg_count}")
                continue
            else:
                wait_for_text_result = self.wait_until_expected_text_is_found(
                    self.bp_loc["pool_state_inside_pool"], "Failure", timeout=10
                )
                if wait_for_text_result is True:
                    logger.info(f"Pool {pool_name} is in failure state")
                    self.take_screenshot()
                    ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph status")
                    total_pg_count = ceph_status["pgmap"]["num_pgs"]
                    logger.info(f"Total pg count is {total_pg_count}")
                    for pool in pool_list:
                        self.delete_pool(pool)
                    break
                else:
                    pool_state = self.get_element_text(
                        self.bp_loc["pool_state_inside_pool"]
                    )
                    logger.info(f"pool condition is {pool_state}")
                    for pool in pool_list:
                        self.delete_pool(pool)
                    raise PoolStateIsUnknow(
                        f"pool {pool_name} is in unexpected state {pool_state}"
                    )

    def check_pool_status(self, pool_name):
        """
        Check the status of the pool

        Args:
            pool_name (str): Name of the pool to check

        Return:
            str: status of the pool

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        pool_status = self.get_element_text(self.validation_loc["blockpool_status"])
        logger.info(f"Pool name {pool_name} current status is {pool_status}")
        return pool_status

    def check_pool_volume_type(self, pool_name):
        """
        Check the volume type of the pool

        Args:
            pool_name (str): Name of the pool to check

        Return:
            str: Volume type of pool if set otherwise

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        pool_volume_type = self.get_element_text(self.bp_loc["block_pool_volume_type"])
        logger.info(f"Pool name {pool_name} existence is {pool_volume_type}")
        return pool_volume_type

    def check_pool_replicas(self, pool_name):
        """
        Check the number of replicas for the pool

        Args:
            pool_name (str): Name of the pool to check

        Return:
            int: Number of replicas for the pool

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        block_pool_replica = self.get_element_text(self.bp_loc["block_pool_replica"])
        logger.info(f"Pool name {pool_name} existence is {block_pool_replica}")
        return int(block_pool_replica)

    def check_pool_used_capacity(self, pool_name):
        """
        Check the total used capacity of the blockpool

        Args:
            pool_name (str): Name of the pool to check

        Return:
            str: The total used capacity of the blockpool

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        block_pool_used_capacity = self.get_element_text(
            self.bp_loc["block_pool_used_capacity"]
        )
        logger.info(
            f"Pool name {pool_name} used capacity is {block_pool_used_capacity}"
        )
        return block_pool_used_capacity

    def check_pool_avail_capacity(self, pool_name):
        """
        Check the available capacity of the blockpool

        Args:
            pool_name (str): Name of the pool to check

        Return:
            str: The total available capacity of blockpool

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        blockpool_avail_capacity = self.get_element_text(
            self.bp_loc["blockpool_avail_capacity"]
        )
        logger.info(f"Pool name {pool_name} existence is {blockpool_avail_capacity}")
        return blockpool_avail_capacity

    def check_pool_compression_status(self, pool_name):
        """
        Check what is the status of pool compression

        Args:
            pool_name (str): Name of the pool to check

        Return:
            bool: True if pool is in the Enabled, otherwise False

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        blockpool_compression_status = self.get_element_text(
            self.bp_loc["blockpool_compression_status"]
        )
        logger.info(
            f"Pool name {pool_name} existence is {blockpool_compression_status}"
        )
        if blockpool_compression_status == "Enabled":
            return True
        return False

    def check_pool_compression_ratio(self, pool_name):
        """
        Check the blockpool compression ratio

        Args:
            pool_name (str): Name of the pool to check

        Return:
            str: Compression ratio of blockpool

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        blockpool_compression_ratio = self.get_element_text(
            self.bp_loc["blockpool_compression_ratio"]
        )
        logger.info(
            f"Pool name {blockpool_compression_ratio} existence is {blockpool_compression_ratio}"
        )
        return blockpool_compression_ratio

    def check_pool_compression_eligibility(self, pool_name):
        """
        Check the pool the percentage of incoming data that is compressible

        Args:
            pool_name (str): Name of the pool to check

        Return:
            str: percentage of incoming compressible data

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        blockpool_compression_eligibility = self.get_element_text(
            self.bp_loc["blockpool_compression_eligibility"]
        )
        logger.info(
            f"Pool name {pool_name} existence is {blockpool_compression_eligibility}"
        )
        return blockpool_compression_eligibility

    def check_pool_compression_savings(self, pool_name):
        """
        Check the total savings gained from compression for this pool, including replicas

        Args:
            pool_name (str): Name of the pool to check

        Return:
            str: The total savings gained from compression for this pool, including replicas

        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        blockpool_compression_savings = self.get_element_text(
            self.bp_loc["blockpool_compression_savings"]
        )
        logger.info(
            f"Pool name {pool_name} existence is {blockpool_compression_savings}"
        )
        return blockpool_compression_savings

    def check_storage_class_attached(self, pool_name):
        """
        Checks the number of storage class attached to the pool.

        Args:
            pool_name (str): Name of the pool to check

        Return:
            int: The total number of storage classes attached to the pool.
        """
        self.navigate_block_pool_page()
        self.do_click((f"a[data-test={pool_name}]", By.CSS_SELECTOR))
        time.sleep(10)
        storage_class_attached = self.get_element_text(
            self.bp_loc["storage_class_attached"]
        )
        storage_class_attached = storage_class_attached.split(" ")[0]
        logger.info(
            f"Pool name {pool_name} has {storage_class_attached} storageclass attached to it."
        )
        return int(storage_class_attached)

    def pool_raw_capacity_loaded(self, pool_name):
        """
        Takes pool name and returns True if the raw capacity of the block pool is loaded
        or returns False if the capacity is not loaded.

        Args:
            pool_name (str): The name of the pool to be deleted

        Returns:
            bool: True if raw capacity of the blockpool is loaded, otherwise False

        """
        logger.info("Checking if the Block Pool Raw Capacity has loaded in UI")

        self.select_blockpool(pool_name)

        bf_obj = BlockAndFile()
        _, raw_capacity_loaded = bf_obj.get_raw_capacity_card_values()

        if raw_capacity_loaded:
            logger.info("Block Pool Raw Capacity has loaded in UI")
        else:
            logger.warning("Block Pool Raw Capacity has not loaded in UI")
        return raw_capacity_loaded

    def cross_check_raw_capacity(self, pool_name):
        """
        Takes pool name and returns True if the raw capacity of the block pool is same in GUI as obtained from CLI
        or returns False if the raw capacity of the block pool doesnt match with CLI

        Args:
            pool_name (str): The name of the pool to be deleted

        Returns:
            bool: True if raw capacity of the blockpool is is same in GUI as obtained from CLI, otherwise False

        """
        logger.info(
            "Checking if the Block Pool Raw Capacity is same in GUI as obtained from CLI"
        )
        if self.pool_raw_capacity_loaded(pool_name):
            cmd = f"rados df --pool={pool_name}"
            ct_pod = pod.get_ceph_tools_pod()
            df_op = ct_pod.exec_cmd_on_pod(command=cmd)
            logger.info(f"{df_op=}")
            # splitting the ouptut with spaces
            # the blockpool used capacity will always be at index 17 and 18
            # where the index 17 is the value and index 18 is the unit
            used_capacity_in_CLI, unit = df_op.split()[17:19]

            logger.info(
                f"Used raw capacity of {pool_name} is {used_capacity_in_CLI} {unit} as checked by CLI"
            )

            bf_obj = BlockAndFile()
            used_raw_capacity_in_UI, _ = bf_obj.get_raw_capacity_card_values()
            (
                used_capacity_in_UI,
                used_capacity_unit_in_UI,
            ) = used_raw_capacity_in_UI.split()

            logger.info(
                f"Used raw capacity of {pool_name} is {used_capacity_in_UI} {used_capacity_unit_in_UI} as checked by UI"
            )

            # rounding the used capacity values from ui and cli for better matching.
            # created custom round method because the cli is rounding the number after .5 and
            # python function rounds from .5
            # ex. cli is doing 157.5 to 157 where as python round function will do 158
            if (
                (self.custom_round(float(used_capacity_in_CLI)))
                == self.custom_round(float(used_capacity_in_UI))
            ) and (unit == used_capacity_unit_in_UI):
                logger.info("UI values did match as per CLI for the Raw Capacity")
                return True
            else:
                logger.error(
                    f"UI value (i.e {used_raw_capacity_in_UI}) did not match as per CLI for the Raw Capacity"
                )
                return False

    def select_blockpool(self, pool_name):
        """
        Selects and clicks on the blockpool according to the blockpool name passed.

        Args:
            pool_name (str): Block pool name that is to be selected.

        Returns:
            True (bool): Successfull selection of the blockpool

        """
        self.navigate_block_pool_page()
        self.page_has_loaded()
        self.do_click(
            locator=format_locator(self.generic_locators["blockpool_name"], pool_name)
        )
        return True

    def custom_round(self, number):
        """Rounds a number down for values ending in exactly ".5".

        Args:
        number: The number to round.

        Returns:
        The rounded number (down for values ending in ".5").
        """
        # Convert the number to a string to check the decimal part
        number_str = str(number)

        # Check if the decimal part exists and ends in ".5"
        if "." in number_str and number_str.endswith(".5"):
            # Round down if it ends in ".5"
            return int(number)
        else:
            # Use regular rounding for other cases
            return round(number)
