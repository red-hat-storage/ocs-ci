import re
import time

from ocs_ci.framework import config
from ocs_ci.ocs.ui.helpers_ui import format_locator, logger
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.workload_ui import PvcCapacityDeploymentList, compare_mem_usage
from ocs_ci.utility.utils import TimeoutSampler


class BlockAndFile(PageNavigator):
    def __init__(self):
        super().__init__()

    def select_capacity_resource(self, resource_name: str, namespace_name: str = None):
        """
        Initial page - Storage / Storage cluster / Block and File
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
        Initial page - Storage / Storage cluster / Block and File tab
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
        Initial page - Data Foundation / Storage Cluster / Block and File
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
        Initial page - Data Foundation / Storage Cluster / Storage pools / ocs-storagecluster-cephblockpool
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
        """
        Initial page - Data Foundation / Storage Cluster / Storage pools / ocs-storagecluster-cephblockpool

        Method to check that the pvc's from the UI are the same as the expected pvc's.
        For each pvc, initially added, filled and saved in PvcCapacityDeploymentList the method checks
        that the pvc is displayed in the UI, and the value under each PVC name matches to expected.

        Important that the method is called after the pvc's are created and filled in PvcCapacityDeploymentList.

        Args:
            namespace (str): Namespace name to select
            check_name (str): Name of the check to be displayed in the report

        Returns:
            dict: Dictionary of the check results or None if all checks passed
        """
        self.select_capacity_resource("PersistentVolumeClaims", namespace)
        pvc_to_size_dict = self.read_capacity_breakdown()
        self.take_screenshot()
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

    def get_raw_capacity_card_values(self):
        """
        Initial page - Data Foundation / Storage Cluster / Block and File
        Get the raw capacity card values

        Returns:
            tuple: Used and available capacity values in format similar to "1.23 TiB"
        """
        logger.info("Get the raw capacity card values")

        used = self.get_element_text(
            format_locator(self.validation_loc["storage_capacity"], "Used")
        )
        available = self.get_element_text(
            format_locator(self.validation_loc["storage_capacity"], "Available")
        )

        return used, available

    def get_estimated_days_from_consumption_trend(self):
        """
        This will fetch information from DataFoundation>>Storage>>Block and File page>>Consumption trend card

        Returns:
            tuple: (get_est_days_from_element, get_avg_from_element)

        """

        get_est_days_from_element = self.get_element_text(
            self.validation_loc["locate_estimated_days_along_with_value"]
        )
        get_avg_from_element = self.get_element_text(
            self.validation_loc["locate_average_of_storage_consumption"]
        )
        return (get_est_days_from_element, get_avg_from_element)

    def odf_storagesystems_consumption_trend(self):
        """
        Function to verify changes and validate elements on ODF storage consumption trend for ODF 4.17
        This will navigate through below order
        DataFoundation>>Storage>>storagecluster_storagesystem_details>>Block and File page
        Further it looks for the Consumption trend card

        Returns:
            tuple: tpl_of_days_and_avg  ex: (Estimated days, Average)

        """

        if not config.ENV_DATA["mcg_only_deployment"]:
            for tpl_of_days_and_avg in TimeoutSampler(
                timeout=300,
                sleep=30,
                func=self.get_estimated_days_from_consumption_trend,
            ):

                if re.search(
                    r"(?=.*\d)(?=.*[a-zA-Z])", tpl_of_days_and_avg[0]
                ) and re.search(r"(?=.*\d)(?=.*[a-zA-Z])", tpl_of_days_and_avg[1]):
                    return tpl_of_days_and_avg
                else:
                    logger.warning("Dashboard is not yet ready yet after osd resize")
        else:
            logger.error("No data available for MCG-only deployments.")
            return None

    def get_est_days_from_ui(self):
        """
        Get the value of 'Estimated days until full' from the UI

        Returns:
            int: Estimated days until full from UI

        """

        collected_tpl_of_days_and_avg = self.odf_storagesystems_consumption_trend()
        est_days = re.search(r"\d+", collected_tpl_of_days_and_avg[0]).group()
        logger.info(f"'Estimated days until full' from the UI : {est_days}")
        return int(est_days)

    def get_avg_consumption_from_ui(self):
        """
        Get the value of 'Average storage consumption' from the UI

        Returns:
            float: Average of storage consumption per day

        """
        collected_tpl_of_days_and_avg = self.odf_storagesystems_consumption_trend()
        average = float(
            re.search(r"-?\d+\.*\d*", collected_tpl_of_days_and_avg[1]).group()
        )
        logger.info(f"'Average of storage consumption per day' from the UI : {average}")
        return average

    def verify_utilization_is_good(self):
        """
        Verify that the utilization status is 'Good' on the Block and File page

        Returns:
            bool: True if the utilization status is 'Good', False otherwise

        """
        logger.info("Verify that the utilization status is 'Good'")

        return self.check_element_text(
            "Storage pool utilization"
        ) and self.check_element_text("Utilization is good!")

    def get_storage_cluster_status(self):
        """
        Verify status of the Storage Cluster on ceph blockpool page, reading from the Status Card

        Returns:
            bool: True if status is Healthy, False otherwise

        """
        parent_element_loc = self.validation_loc[
            "storage-pool-storage-cluster-status-from-card"
        ]
        self.wait_for_element_to_be_visible(parent_element_loc)
        healthy_loc = (
            f"{parent_element_loc[0]}//*[text()='Healthy']",
            parent_element_loc[1],
        )
        return len(self.get_elements(healthy_loc)) > 0

    def resiliency_ok(self):
        """
        Verify resiliency status of the Storage Cluster on ceph blockpool pag is Healthy, reading from the Status Card

        Returns:
            bool: True if status is Healthy, False otherwise

        """
        parent_element_loc = self.validation_loc[
            "storage-pool-data-resiliency-status-from-card"
        ]
        self.wait_for_element_to_be_visible(parent_element_loc)
        healthy_loc = (
            f"{parent_element_loc[0]}//*[text()='Healthy']",
            parent_element_loc[1],
        )
        return len(self.get_elements(healthy_loc)) > 0
