import time

from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.storage_system_details import StorageSystemDetails
from ocs_ci.ocs.ui.workload_ui import PvcCapacityDeploymentList, compare_mem_usage
from tests.ui.test_capacity_breakdown_ui import logger


class BlockAndFile(StorageSystemDetails):
    def __init__(self):
        StorageSystemDetails.__init__(self)

    def select_capacity_resource(self, resource_name: str, namespace_name: str = None):
        """
        Initial page - Data Foundation / Storage Systems tab / StorageSystem details / Block and File
        Select the capacity resource from the dropdown

        Args:
            resource_name (str): resource name to select
            namespace_name (str): namespace name to select
        """
        self.select_requested_capacity_dropdown(resource_name)
        # avoid selenium.common.exceptions.ElementClickInterceptedException, give 5 sec before element updates
        time.sleep(5)
        if namespace_name:
            self.select_namespace_for_pvcs(namespace_name)

    def select_namespace_for_pvcs(self, namespace_name: str):
        """
        Initial page - Data Foundation / Storage Systems tab / StorageSystem details / Block and File
        Select the namespace for PVs from the dropdown

        Args:
            namespace_name (str): Namespace name to select. Namespace should be unique,
            otherwise the first one will be selected
        """
        logger.info(f"Select the namespace for PVs from the dropdown: {namespace_name}")

        self.do_click(self.validation_loc["req_capacity_dropdown_namespace"])
        self.do_send_keys(
            self.validation_loc["req_capacity_dropdown_namespace_input"],
            namespace_name,
            timeout=60,
        )
        self.do_click(
            format_locator(
                self.validation_loc["req_capacity_dropdown_namespace_input_select"],
                namespace_name,
            )
        )

    def select_requested_capacity_dropdown(self, dropdown_val: str):
        """
        Initial page - Data Foundation / Storage Systems tab / StorageSystem details / Block and File
        Select the requested capacity from the dropdown

        Args:
            dropdown_val (str): Dropdown value to select
        """
        logger.info(f"Select the requested capacity from the dropdown: {dropdown_val}")

        if (
            self.get_element_text(self.validation_loc["req_capacity_dropdown_selected"])
            != dropdown_val
        ):
            self.do_click(
                self.validation_loc["req_capacity_dropdown_btn_one"],
                enable_screenshot=True,
            )
            self.do_click(
                format_locator(
                    self.validation_loc["req_capacity_dropdown_list_option"],
                    dropdown_val,
                ),
                enable_screenshot=True,
            )

    def read_capacity_breakdown(self):
        """
        Initial page - Data Foundation / Storage Systems tab / StorageSystem details / ocs-storagecluster-cephblockpool
        Read the capacity breakdown from the table

        Returns:
            dict: Dictionary of capacity breakdown
        """
        logger.info("Read the capacity breakdown from the table")

        num_capacity_labels = len(
            self.get_elements(self.validation_loc["capacity_breakdown_cards"])
        )
        card_to_size = dict()
        for i in range(1, num_capacity_labels + 1):
            loc_card_name = format_locator(
                self.validation_loc["capacity_breakdown_card"], str(i)
            )
            link = self.get_element_attribute(loc_card_name, "href").get("baseVal")
            card_name = link.split("/")[-1]
            loc_card_size = format_locator(
                self.validation_loc["capacity_breakdown_card_size"], str(i)
            )
            title_and_size = self.get_element_text(loc_card_size)
            # card name and shortening '...' removal
            card_size = title_and_size.split("...")[-1]
            card_to_size[card_name] = card_size

        logger.info(card_to_size)
        return card_to_size

    def check_pvc_to_namespace_ui_card(self, namespace, check_name: str):
        self.select_capacity_resource("PersistentVolumeClaims", namespace)
        pvc_to_size_dict = self.read_capacity_breakdown()

        if not all(
            [
                pvc_to_size_dict.get(pvc_name)
                for pvc_name in PvcCapacityDeploymentList().get_pvc_names_list()
            ]
        ):
            self.take_screenshot()
            self.copy_dom()
            return {
                check_name: f"pvc's from UI: {PvcCapacityDeploymentList().get_pvc_names_list()} "
                f"are not the same as expected pvc's: {pvc_to_size_dict}"
            }

        for data_struct in PvcCapacityDeploymentList():

            used_capacity_ui: str = pvc_to_size_dict[data_struct.pvc_obj.name]
            used_capacity_expected_int = data_struct.capacity_size

            if used_capacity_expected_int is None:
                self.take_screenshot()
                self.copy_dom()
                return {
                    check_name: f"Failed to extract a number from the value given at UI "
                    f"for the pvc: {data_struct.pvc_obj.name}"
                }

            if not compare_mem_usage(
                used_capacity_expected_int, used_capacity_ui, deviation_accepted=10
            ):
                self.take_screenshot()
                self.copy_dom()
                return {
                    check_name: f"pvc {data_struct.pvc_obj.name} capacity is not as expected. "
                    f"cli capacity: {data_struct.capacity_size}GiB | ui capacity {used_capacity_ui}"
                }
            else:
                logger.info(f"pvc {data_struct.pvc_obj.name} capacity is as expected.")
