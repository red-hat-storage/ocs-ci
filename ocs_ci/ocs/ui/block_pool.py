import logging
import time

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from selenium.webdriver.common.by import By
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.exceptions import PoolStateIsUnknow, PoolIdNotFound
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs.cluster import get_pool_id
from ocs_ci.utility.utils import convert_bytes_into_right_measure

logger = logging.getLogger(__name__)


class BlockPoolUI(PageNavigator):
    """
    User Interface Selenium for Block Pools page

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.bp_loc = locators[ocp_version]["block_pool"]
        self.sc_loc = locators[ocp_version]["storageclass"]

    def create_pool(self, replica, compression):
        """
        Create block pool via UI

        Args:
            replica (int): replica size usually 2,3
            compression (bool): True to enable compression otherwise False

        Return:
            array: pool name (str) pool status (bool) #pool can be created with failure status

        """
        pool_name = create_unique_resource_name("test", "rbd-pool")
        self.navigate_block_pool_page()
        self.do_click(self.bp_loc["create_block_pool"])
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
            self.bp_loc["pool_state_inside_pool"], "Ready", timeout=15
        )
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
        self.navigate_installed_operators_page()
        self.navigate_block_pool_page()
        self.page_has_loaded(retries=15)
        pool_existence = self.wait_until_expected_text_is_found(
            (f"a[data-test-operand-link={pool_name}]", By.CSS_SELECTOR), pool_name, 5
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

        self.navigate_installed_operators_page()
        self.navigate_block_pool_page()
        self.page_has_loaded(retries=15)
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
        self.navigate_installed_operators_page()
        self.navigate_block_pool_page()
        self.page_has_loaded(retries=15)
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

    def check_ui_pool_efficiency_parameters_against_prometheus(
        self, pool_name, expected_compression_saving
    ):
        """
        Get pool compression efficiency from UI and compare them against prometheus metrics

        Args:
            pool_name (str): The pool name to be checked.
            expected_compression_saving (str): The expected compression saving in string like "1.5 GiB"

        Returns:
            (bool): True if UI and prometheus parameters are equal to expected_compression_saving - else False.

        """

        # Navigate to pool page
        self.navigate_installed_operators_page()
        self.navigate_block_pool_page()
        self.do_click([f"{pool_name}", By.LINK_TEXT])
        self.page_has_loaded(retries=15)

        # Get efficiency card values
        eff_elements = self.driver.find_elements_by_class_name(
            "ceph-storage-efficiency-card__item-text"
        )
        eff_value_array = []
        for element in eff_elements:
            eff_value_array.append(element.text)
        compression_eligibility, compression_ratio, compression_saving = eff_value_array
        compression_saving_value, compression_saving_unit = compression_saving.split(
            " "
        )
        logger.info(
            f"compression_saving_value {compression_saving_value} "
            f"compression_saving_unit {compression_saving_unit}"
        )
        logger.info(
            f"compression_eligibility {compression_eligibility} "
            f"compression_ratio {compression_ratio} "
            f" compression_saving {compression_saving}"
        )

        # Get pool id and query Prometheus
        pool_id = get_pool_id(pool_name)
        if pool_id is None:
            raise PoolIdNotFound(f"Pool {pool_name} didn't return pool id")
        new_prom = PrometheusAPI()
        ceph_pool_compress_bytes_used_data = new_prom.query(
            f'ceph_pool_compress_bytes_used{{pool_id="{pool_id}"}}'
        )
        ceph_pool_compress_under_bytes_data = new_prom.query(
            f'ceph_pool_compress_under_bytes{{pool_id="{pool_id}"}}'
        )

        # Calculate compression savings
        ceph_pool_compress_bytes_used_value = ceph_pool_compress_bytes_used_data[0].get(
            "value"
        )
        ceph_pool_compress_under_bytes_value = ceph_pool_compress_under_bytes_data[
            0
        ].get("value")
        final_saving = int(ceph_pool_compress_under_bytes_value[1]) - int(
            ceph_pool_compress_bytes_used_value[1]
        )
        logger.info(f"final saving in bytes {final_saving}")

        # Convert final saving to right measure
        prometheus_final_saving_human = convert_bytes_into_right_measure(final_saving)
        logger.info(f"Final saving in ui measure {prometheus_final_saving_human}")

        # Calculate compression ratio
        final_ratio = int(ceph_pool_compress_under_bytes_value[1]) / int(
            ceph_pool_compress_bytes_used_value[1]
        )
        logger.info(f"final ratio {final_ratio}")

        # Compare all parameters
        (
            expected_compression_value,
            expected_compression_unit,
        ) = expected_compression_saving.split(" ")

        return_value_are_equal = True
        prom_saving_value, prom_final_unit = prometheus_final_saving_human.split(" ")
        if (
            format(float(compression_saving_value), ".2f")
            != format(float(expected_compression_value), ".2f")
            or compression_saving_unit != expected_compression_unit
        ):
            return_value_are_equal = False
            logger.info(
                f"UI compression saving are not equal to expected saving: "
                f"UI compression value is {compression_saving_value} "
                f"and expected compression value is {expected_compression_value}. "
                f"UI compression unit is {compression_saving_unit} "
                f"and prometheus compression unit is {expected_compression_unit}"
            )

        if (
            format(float(compression_saving_value), ".2f")
            != format(float(prom_saving_value), ".2f")
            or compression_saving_unit != prom_final_unit
        ):
            return_value_are_equal = False
            logger.info(
                f"UI compression saving are not equal to prometheus saving: "
                f"UI compression value is {compression_saving_value} "
                f"and prometheus compression value is {prom_saving_value}. "
                f"UI compression unit is {compression_saving_unit} "
                f"and prometheus compression unit is {prom_final_unit}"
            )
        if compression_eligibility != "100%":
            return_value_are_equal = False
            logger.info(
                f"UI compression eligibility is {compression_eligibility} but should be 100%"
            )

        final_ratio = int(final_ratio)
        first_ratio_value, second_ratio_value = compression_ratio.split(":")
        final_ui_ratio = int(int(first_ratio_value) / int(second_ratio_value))
        if final_ratio != final_ui_ratio:
            return_value_are_equal = False
            logger.info(
                f"UI ratio is {final_ui_ratio} where prometheus ratio is {final_ratio}"
            )
        return return_value_are_equal
