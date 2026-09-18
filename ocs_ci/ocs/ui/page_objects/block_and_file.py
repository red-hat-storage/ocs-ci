import re
import time

from ocs_ci.framework import config
from ocs_ci.ocs.ui.helpers_ui import format_locator, logger
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.workload_ui import PvcCapacityDeploymentList, compare_mem_usage
from ocs_ci.utility.utils import TimeoutSampler
from selenium.common.exceptions import TimeoutException
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from dataclasses import dataclass


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

    def get_volume_health_card(self):
        """
        Get the Volume Health Card page object.

        Returns:
            VolumeHealthCard: Volume Health Card page object instance
        """

        logger.info("Accessing Volume Health Card page object")
        return VolumeHealthCard()


@dataclass
class RowData:
    """Data structure for Volume Health table row"""

    pvc_name: str
    node_name: str
    events_href: str
    pvc_href: str
    node_href: str


class VolumeHealthCard(BlockAndFile):
    """
    Page Object Model for Volume Health Card on Block and File tab.
    Provides methods to interact with healthy and unhealthy states.
    """

    def __init__(self):
        super().__init__()

    def is_card_present(self):
        """
        Check if Volume Health Card is present on the page.

        Returns:
            bool: True if card is present, False otherwise
        """
        logger.info("Checking Volume Health Card presence")
        result = len(self.get_elements(self.validation_loc["volume_health_card"])) > 0
        logger.info(f"Card present: {result}")
        if not result:
            self.take_screenshot("volume_health_card_not_found")
        return result

    def is_healthy(self):
        """
        Check if card shows healthy state (success icon visible).

        Returns:
            bool: True if healthy state shown, False otherwise
        """
        logger.info("Checking if card shows healthy state")
        result = (
            len(self.get_elements(self.validation_loc["volume_health_success_icon"]))
            > 0
        )
        logger.info(f"Card healthy: {result}")
        if not result:
            self.take_screenshot("card_not_healthy")
        return result

    def get_no_issues_text(self):
        """
        Get the 'no issues' text from healthy state.

        Returns:
            str: Text content (typically "No issues found.")
        """
        logger.info("Extracting 'no issues' text from card")
        self.wait_for_element_to_be_visible(
            self.validation_loc["volume_health_no_issues_text"], timeout=10
        )
        text = self.get_element_text(
            self.validation_loc["volume_health_no_issues_text"]
        )
        logger.info(f"Text: '{text}'")
        return text

    def click_view_all_pvcs(self):
        """
        Click the 'View all PVCs' link in healthy state.
        Navigates away to the PVC list page.
        """
        logger.info("Clicking 'View all PVCs' link")
        self.take_screenshot("before_click_view_all_pvcs")
        self.do_click(self.validation_loc["volume_health_view_all_pvcs_link"])
        logger.info("Navigated to PVC list page")
        self.take_screenshot("after_click_view_all_pvcs")

    def get_attention_text(self):
        """
        Get the attention banner text (e.g., "1 volume needs attention").

        Returns:
            str: Attention text
        """
        logger.info("Extracting attention banner text")
        self.wait_for_element_to_be_visible(
            self.validation_loc["volume_health_attention_text"], timeout=10
        )
        text = self.get_element_text(
            self.validation_loc["volume_health_attention_text"]
        )
        logger.info(f"Attention text: '{text}'")
        return text

    def get_attention_count(self):
        """
        Parse the number of unhealthy PVCs from attention text.

        Returns:
            int: Number of PVCs needing attention
        """
        logger.info("Parsing PVC count from attention text")
        text = self.get_attention_text()

        match = re.search(
            r"(\d+)\s+(?:volume|PersistentVolumeClaim)", text, re.IGNORECASE
        )
        if match:
            count = int(match.group(1))
        else:
            count = 0

        logger.info(f"Found {count} PVC(s) needing attention")
        return count

    def get_table_rows(self):
        """
        Get all table rows from unhealthy state table.

        Returns:
            list: List of WebElement objects (table rows)
        """
        logger.info("Fetching table rows from health card")
        rows = self.get_elements(self.validation_loc["volume_health_table_rows"])
        logger.info(f"Found {len(rows)} row(s)")
        return rows

    def get_row_data(self, row):
        """
        Parse data from a single table row.

        Args:
            row: WebElement representing a table row

        Returns:
            RowData: Parsed row data
        """
        pvc_locator = self.validation_loc["volume_health_table_pvc_link"]
        pvc_link = row.find_element(pvc_locator[1], pvc_locator[0])
        pvc_href = pvc_link.get_attribute("href") or ""
        pvc_name = (
            pvc_href.rstrip("/").split("/")[-1] if pvc_href else pvc_link.text.strip()
        )

        node_locator = self.validation_loc["volume_health_table_node_link"]
        node_link = row.find_element(node_locator[1], node_locator[0])
        node_href = node_link.get_attribute("href") or ""
        node_name = (
            node_href.rstrip("/").split("/")[-1]
            if node_href
            else node_link.text.strip()
        )

        events_locator = self.validation_loc["volume_health_table_events_link"]
        events_link = row.find_element(events_locator[1], events_locator[0])
        events_href = events_link.get_attribute("href") or ""

        logger.info(f"Parsing row: PVC={pvc_name}, Node={node_name}")

        return RowData(
            pvc_name=pvc_name,
            node_name=node_name,
            events_href=events_href,
            pvc_href=pvc_href,
            node_href=node_href,
        )

    def get_all_row_data(self):
        """
        Extract data from all table rows.

        Returns:
            list[RowData]: List of parsed row data
        """
        logger.info("Extracting all row data from table")
        rows = self.get_table_rows()
        all_data = [self.get_row_data(row) for row in rows]
        logger.info(f"Parsed {len(all_data)} row(s)")
        self.take_screenshot(f"table_parsed_{len(all_data)}_rows")
        return all_data

    def _is_pvc_in_table(self, pvc_name):
        """
        Helper method to check if PVC is in table (for wait polling).

        Uses an absolute locator to fetch all PVC link hrefs directly from
        the card root — avoids stale element errors that occur when iterating
        row WebElements and calling find_element on them after a re-render.

        Args:
            pvc_name (str): PVC name to check

        Returns:
            bool: True if PVC found in table
        """
        pvc_links = self.get_elements(
            self.validation_loc["volume_health_table_pvc_hrefs"]
        )
        for link in pvc_links:
            href = link.get_attribute("href") or ""
            name = href.rstrip("/").split("/")[-1]
            if name == pvc_name:
                return True
        return False

    def wait_for_pvc_in_table(self, pvc_name, timeout=60):
        """
        Wait for PVC to appear in Volume Health table.

        Args:
            pvc_name (str): PVC name to wait for
            timeout (int): Timeout in seconds (default: 60)

        Returns:
            bool: True if found, False if timeout
        """
        logger.info(f"Waiting for PVC '{pvc_name}' in table (timeout={timeout}s)")

        try:
            for is_present in TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=self._is_pvc_in_table,
                pvc_name=pvc_name,
            ):
                if is_present:
                    logger.info(f"PVC '{pvc_name}' found in health table")
                    return True
        except (TimeoutException, TimeoutExpiredError):
            logger.error(f"PVC '{pvc_name}' not found after {timeout}s")
            self.take_screenshot(f"pvc_{pvc_name}_not_in_table")
            self.copy_dom(f"pvc_{pvc_name}_not_in_table")
            return False

        return False

    def _is_pvc_not_in_table(self, pvc_name):
        """
        Helper method to check if PVC is NOT in table (for wait polling).

        Args:
            pvc_name (str): PVC name to check

        Returns:
            bool: True if PVC not found in table
        """
        rows = self.get_table_rows()
        if len(rows) == 0:
            return True

        for row in rows:
            row_data = self.get_row_data(row)
            if row_data.pvc_name == pvc_name:
                return False
        return True

    def wait_for_pvc_not_in_table(self, pvc_name, timeout=60):
        """
        Wait for PVC to disappear from Volume Health table (recovery).

        Args:
            pvc_name (str): PVC name to wait for removal
            timeout (int): Timeout in seconds (default: 60)

        Returns:
            bool: True if PVC cleared, False if timeout
        """
        logger.info(
            f"Waiting for PVC '{pvc_name}' to clear from table (timeout={timeout}s)"
        )

        try:
            for is_absent in TimeoutSampler(
                timeout=timeout,
                sleep=5,
                func=self._is_pvc_not_in_table,
                pvc_name=pvc_name,
            ):
                if is_absent:
                    logger.info(f"PVC '{pvc_name}' no longer in table")
                    return True
        except (TimeoutException, TimeoutExpiredError):
            logger.error(f"PVC '{pvc_name}' still present after {timeout}s")
            self.take_screenshot(f"pvc_{pvc_name}_still_in_table")
            self.copy_dom(f"pvc_{pvc_name}_still_in_table")
            return False

        return False

    def wait_for_healthy(self, timeout=300):
        """
        Wait for card to return to healthy state (success icon visible).

        Args:
            timeout (int): Timeout in seconds (default: 300)

        Returns:
            bool: True if healthy, False if timeout
        """
        logger.info(f"Waiting for card to return to healthy state (timeout={timeout}s)")

        try:
            for is_healthy in TimeoutSampler(
                timeout=timeout,
                sleep=10,
                func=self.is_healthy,
            ):
                if is_healthy:
                    logger.info("Card returned to healthy state")
                    return True
        except (TimeoutException, TimeoutExpiredError):
            logger.error(f"Card not healthy after {timeout}s")
            self.take_screenshot("card_not_healthy_timeout")
            self.copy_dom("card_not_healthy_timeout")
            return False

        return False

    def click_view_events(self, pvc_name):
        """
        Click 'View events' link for a specific PVC.
        Navigates away to the PVC events page.

        Args:
            pvc_name (str): PVC name to view events for
        """
        logger.info(f"Clicking 'View events' link for PVC '{pvc_name}'")
        self.take_screenshot(f"before_click_events_{pvc_name}")

        locator = format_locator(
            self.validation_loc["volume_health_view_events_link"], pvc_name
        )
        self.do_click(locator)

        logger.info(f"Navigated to events page for PVC '{pvc_name}'")
        self.take_screenshot(f"after_click_events_{pvc_name}")
