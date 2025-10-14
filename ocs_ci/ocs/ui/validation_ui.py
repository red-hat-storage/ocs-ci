import logging
import math
import pytest
import time
import pandas as pd
from ocs_ci.framework import config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.ocs.cluster import (
    get_used_and_total_capacity_in_gibibytes,
    get_age_of_cluster_in_days,
)
from ocs_ci.ocs.exceptions import UnexpectedODFAccessException, CephHealthException
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.block_pool import BlockPoolUI
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)


class ValidationUI(PageNavigator):
    """
    User Interface Validation Selenium

    """

    def __init__(self):
        super().__init__()
        self.err_list = list()

    def verify_object_service_page(self):
        """
        Verify Object Service Page UI

        """
        self.navigate_cluster_overview_page()
        self.do_click(self.validation_loc["object_service_tab"], enable_screenshot=True)
        platform = config.ENV_DATA.get("platform").lower()
        if platform in constants.ON_PREM_PLATFORMS:
            logger.info("Click on Object Service button")
            self.do_click(
                self.validation_loc["object_service_button"], enable_screenshot=True
            )
            logger.info("Click on Data Resiliency button")
            self.do_click(
                self.validation_loc["data_resiliency_button"], enable_screenshot=True
            )
        strings_object_service_tab = ["Total Reads", "Total Writes"]
        self.verify_page_contain_strings(
            strings_on_page=strings_object_service_tab, page_name="object_service"
        )

    def verify_persistent_storage_page(self):
        """
        Verify Persistent Storage Page

        """
        self.navigate_cluster_overview_page()
        self.do_click(
            self.validation_loc["persistent_storage_tab"], enable_screenshot=True
        )
        strings_object_service_tab = [
            "IOPS",
            "Latency",
            "Throughput",
            "Recovery",
            "Utilization",
            "Used Capacity Breakdown",
            "Raw Capacity",
        ]
        self.verify_page_contain_strings(
            strings_on_page=strings_object_service_tab, page_name="persistent_storage"
        )

    def verify_ocs_operator_tabs(self):
        """
        Verify OCS Operator Tabs

        """
        self.navigate_installed_operators_page()
        logger.info("Search OCS operator installed")
        self.do_send_keys(
            locator=self.validation_loc["search_ocs_installed"],
            text="OpenShift Container Storage",
        )
        logger.info("Click on ocs operator on Installed Operators")
        self.do_click(
            locator=self.validation_loc["ocs_operator_installed"],
            enable_screenshot=True,
        )

        logger.info("Verify Details tab on OCS operator")
        strings_details_tab = [
            "Description",
            "Succeeded",
            config.ENV_DATA["cluster_namespace"],
        ]
        self.verify_page_contain_strings(
            strings_on_page=strings_details_tab, page_name="details_tab"
        )

        logger.info("Verify Subscription tab on OCS operator")
        self.do_click(
            self.validation_loc["osc_subscription_tab"], enable_screenshot=True
        )
        strings_subscription_tab = [
            "Healthy",
            "openshift-storage",
        ]
        self.verify_page_contain_strings(
            strings_on_page=strings_subscription_tab, page_name="subscription_tab"
        )

        logger.info("Verify All instances tab on OCS operator")
        self.do_click(
            self.validation_loc["osc_all_instances_tab"], enable_screenshot=True
        )
        strings_all_instances_tab = ["Phase", "Ready", "Status"]
        self.verify_page_contain_strings(
            strings_on_page=strings_all_instances_tab, page_name="all_instances_tab"
        )

        logger.info("Verify Storage Cluster tab on OCS operator")
        self.do_click(
            self.validation_loc["osc_storage_cluster_tab"], enable_screenshot=True
        )
        strings_storage_cluster_tab = ["Phase", "Ready", "Status"]
        self.verify_page_contain_strings(
            strings_on_page=strings_storage_cluster_tab, page_name="storage_cluster_tab"
        )

        logger.info("Verify Backing Store tab on OCS operator")
        self.do_click(
            self.validation_loc["osc_backing_store_tab"], enable_screenshot=True
        )
        strings_backing_store_tab = ["Phase", "Ready", "Status"]
        self.verify_page_contain_strings(
            strings_on_page=strings_backing_store_tab, page_name="backing_store_tab"
        )

        logger.info("Verify Bucket Class tab on OCS operator")
        self.do_click(
            self.validation_loc["osc_bucket_class_tab"], enable_screenshot=True
        )
        strings_bucket_class_tab = ["Phase", "Ready", "Status"]
        self.verify_page_contain_strings(
            strings_on_page=strings_bucket_class_tab, page_name="bucket_class_tab"
        )

    def verify_page_contain_strings(self, strings_on_page, page_name):
        """
        Verify Page Contain Strings

        Args:
            strings_on_page (list): list of strings on page
            page_name (str): the name of the page

        """
        logger.info(f"verify {strings_on_page} exist on {page_name}")
        for string in strings_on_page:
            sample = TimeoutSampler(
                timeout=3,
                sleep=1,
                func=self.check_element_text,
                expected_text=string,
            )
            if not sample.wait_for_func_status(result=True):
                self.err_list.append(f"{string} string not found on {page_name}")

    def verification_ui(self):
        """
        Verification UI

        """
        self.verify_object_service_page()
        self.verify_persistent_storage_page()
        self.verify_ocs_operator_tabs()
        self.take_screenshot()
        for err in self.err_list:
            logger.error(err)
        assert len(self.err_list) == 0, f"{self.err_list}"

    def refresh_web_console(self):
        refresh_web_console_popup = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["warning-alert"],
            expected_text="Refresh web console",
            timeout=180,
        )
        if refresh_web_console_popup:
            logger.info(
                "Refresh web console option is now available, click on it to see the console changes"
            )
            self.do_click(
                self.validation_loc["refresh-web-console"],
                enable_screenshot=True,
            )
        else:
            logger.warning("Refresh web console pop-up was not found")

    def odf_console_plugin_check(self):
        """
        Function to verify if console plugin is enabled on UI or not,
        if not, this function will enable it so as to see ODF tab under Storage section

        """

        self.navigate_installed_operators_page()
        logger.info("Click on project dropdown")
        self.do_click(self.validation_loc["project-dropdown"])
        default_projects_is_checked = self.driver.find_element(
            By.XPATH, "//input[@type='checkbox']"
        )
        if default_projects_is_checked.get_attribute("data-checked-state") == "false":
            logger.info("Show default projects")
            self.do_click(self.validation_loc["show-default-projects"])
        logger.info("Search for '%s' project", config.ENV_DATA["cluster_namespace"])
        self.do_send_keys(
            self.validation_loc["project-search-bar"],
            text=config.ENV_DATA["cluster_namespace"],
        )
        logger.info("Select '%s' project", config.ENV_DATA["cluster_namespace"])
        time.sleep(2)
        self.do_click(
            self.dep_loc["choose_openshift-storage_project"], enable_screenshot=True
        )
        self.page_has_loaded(retries=25, sleep_time=1)
        logger.info(
            "Check if 'Plugin available' option is available on the Installed Operators page"
        )
        plugin_availability_check = self.wait_until_expected_text_is_found(
            locator=self.dep_loc["plugin-available"],
            expected_text="Plugin available",
            timeout=15,
        )
        if plugin_availability_check:
            logger.info(
                "Storage plugin is disabled, navigate to Operator details page further confirmation"
            )
            self.do_click(self.validation_loc["odf-operator"])
            self.page_has_loaded(retries=15, sleep_time=5)
            console_plugin_status = self.get_element_text(
                self.validation_loc["console_plugin_option"]
            )
            if console_plugin_status == "Disabled":
                logger.info(
                    "Storage plugin is disabled, Enable it to see ODF tab under Storage section"
                )
                self.do_click(self.validation_loc["console_plugin_option"])
                self.do_click(self.dep_loc["enable_console_plugin"])
                self.do_click(self.validation_loc["save_console_plugin_settings"])
                logger.info("Waiting for warning alert to refresh the web console")
                self.refresh_web_console()
                refresh_web_console_popup = self.wait_until_expected_text_is_found(
                    locator=self.validation_loc["warning-alert"],
                    expected_text="Refresh web console",
                )
                if refresh_web_console_popup:
                    logger.info(
                        "Refresh web console option is now available, click on it to see the changes"
                    )
                    self.do_click(
                        self.validation_loc["refresh-web-console"],
                        enable_screenshot=True,
                    )
            else:
                logger.info("Plugin availability check skipped")

    def odf_overview_ui(
        self,
    ):
        """
        Method to verify changes and validate elements on ODF Storage and Data Foundation web pages

        **Steps**

        1. Validate ODF console plugin is enabled, if not enable it
        2. Navigate to **Data Foundation** page via **PageNavigator** and **Data Foundation**
        3. If storage is on cluster or configuration is multi storagecluster:

            - Validate that the legend for **Available vs Used capacity** is present.
            - Verify that **Overview** navigates to the **Storage Cluster** page.
            - Ensure the **Used Raw Capacity** string is visible in the **System Capacity** card.
            - Verify that the **Block and File** tab is active.
            - Verify that utilization is good in the **Block and File** tab.
            - Verify that resiliency is OK in the **Block and File** tab.
            - Verify that **CephBlockPool** status is **Ready**.
            - Verify that **CephFS data pool** status is **Ready**.
            - Navigate to the default **CephBlockPool** using the search filter.
            - Verify that the **Storage Cluster** is healthy on the **CephBlockPool** page.
            - Verify that the **Performance Card** is present.
            - Navigate to the **Storage Pools** page via the breadcrumb link.
            - Navigate to the **ODF Backing Store** tab via the **Object Storage** tab or **PageNavigator**.
            - Verify that all tabs are working.

        4. If **external mode** or **multi–storagecluster**:

            - Navigate to **External Systems**
            - Verify if **Block and File** tab is active
            - verify content of **External Storage Cluster** / **Block and File** tab
            - Check if **PersistentVolumeClaims capacity breakdown** is available
            - Navigate to **Object** tab from **External cluster**

        5. Navigate to **Buckets page** via View **Buckets link**
        6. Navigate to **Backing Store** tab via **Buckets tab**
        7. Verify if **Backing Store** is present and link to **Backing Store** resource works
        8. Navigate to **Backing Store** tab via breadcrumb
        9. Navigate to **Bucket class** tab via **Backing Store** tab
        10. Navigate to the default **Bucket Class** details via **Bucket Class** tab
        11. Verify the status of a default **bucket class**
        12. Navigate to **Bucket class** via breadcrumb
        13. Navigate to **Namespace Store** tab via **Bucket Class** tab, verify if it works
        14. If not **external mode**:
            - Navigate to **Storage Cluster** via **Page navigator**
            - Verify if **Block and File** tab is active
        15. If **external mode**:
            - Navigate to **External Systems** page via **Page navigator**
            - Verify if **External Systems** page is opening
        16. Process results

        Raises:
            AssertionError: If one of the verifications fails

        """
        res_dict = {}
        external_mode = config.DEPLOYMENT.get("external_mode")
        storage_on_cluster = not external_mode
        multi_storagecluster = config.ENV_DATA.get("multi_storagecluster", False)

        log_step("Validate ODF console plugin is enabled, if not enable it")
        self.odf_console_plugin_check()

        log_step(
            "Navigate to Data Foundation page via PageNavigator and Data Foundation"
        )
        overview_page = self.nav_storage_data_foundation_overview_page()

        if storage_on_cluster or multi_storagecluster:
            log_step("Validate if legend for Available vs Used capacity is present")
            overview_page.available_vs_used_capacity_present()

            log_step("Verify if Overview navigates to Storage Cluster page")
            storage_cluster_page = overview_page.navigate_to_view_storage()
            res_dict["block_and_file_tab_is_active_1"] = (
                storage_cluster_page.validate_block_and_file_tab_active()
            )

            log_step("Ensure used raw capacity string in System Capacity card")
            res_dict["system_raw_capacity_check_bz_2185042"] = self.check_element_text(
                "Raw capacity"
            )

            log_step("Verify if Block and File tab is active")
            storage_cluster_page.validate_block_and_file_tab_active()

            block_and_file_tab = storage_cluster_page

            res_dict["block_and_file_utilization_good"] = (
                block_and_file_tab.verify_utilization_is_good()
            )
            res_dict["block_and_file_resiliency_ok"] = (
                block_and_file_tab.resiliency_ok()
            )

            storage_pools_tab = storage_cluster_page.nav_storage_pools_tab()

            log_step("Verify CephBlockPool status is Ready")
            try:
                storage_pools_tab.verify_cephblockpool_status()
                res_dict["cephblockpool_status_ready"] = True
            except (CephHealthException, WebDriverException) as e:
                logger.error(f"CephBlockPool status is not Ready: {e}")
                self.take_screenshot("cephblockpool_status_not_ready")
                res_dict["cephblockpool_status_ready"] = False

            log_step("Verify cephfs data pool status is Ready")
            try:
                storage_pools_tab.verify_cephfs_status()
                res_dict["cephfs_data_pool_status_ready"] = True
            except (CephHealthException, WebDriverException) as e:
                logger.error(f"CephFS Data Pool status is not Ready: {e}")
                self.take_screenshot("cephfs_data_pool_status_not_ready")
                res_dict["cephfs_data_pool_status_ready"] = False

            log_step("Navigate to default cephblockpool using searching filter")
            ceph_block_pool_page = storage_pools_tab.navigate_to_block_pool(
                constants.DEFAULT_CEPHBLOCKPOOL
            )
            res_dict["storage_cluster_healthy"] = (
                ceph_block_pool_page.block_pool_ready()
            )

            log_step("Verify if Performance Card is present")
            # BlockPoolUI is not a POM class, but helper class
            block_pool_helper = BlockPoolUI()
            res_dict["performance_card_header_present"] = (
                block_pool_helper.validate_performance_card_header_present()
            )

            ceph_block_pool_page.navigate_storage_pools_via_breadcrumb()
            res_dict["block_pool_navigation_works"] = True

            log_step(
                "Navigate ODF Backing store tab via Object Storage tab or PageNavigator"
            )

            log_step("Verify all tabs are working")
            try:
                storage_cluster_page.nav_object_tab()
                storage_cluster_page.nav_storage_pools_tab()
                storage_cluster_page.nav_topology_tab()
                res_dict["storage_cluster_tabs_work"] = True
            except WebDriverException:
                logger.error("One of the tabs is not working")
                res_dict["storage_cluster_tabs_work"] = False

        if external_mode or multi_storagecluster:
            log_step("Navigate to External Systems")
            storage_cluster_page = (
                overview_page.navigate_to_external_storage_systems().nav_to_external_storage_cluster()
            )
            log_step("Verify if Block and File tab is active")
            storage_cluster_page.validate_block_and_file_tab_active()
            log_step("verify content of External Storage Cluster / Block and File tab")

            logger.info(
                "Check if PersistentVolumeClaims capacity breakdown is available"
            )
            storage_cluster_page.select_capacity_resource(
                "PersistentVolumeClaims", config.ENV_DATA["cluster_namespace"]
            )
            pvc_to_size_dict = storage_cluster_page.read_capacity_breakdown()
            self.take_screenshot("bloack_and_file_req_capacity")
            res_dict["external_storage_cluster_req_cap_OK"] = bool(pvc_to_size_dict)

            logger.info("Navigate to Object tab from External storage cluster")
            try:
                storage_cluster_page.nav_object_tab()
                self.take_screenshot("objectr_tab_external_storage_cluster")
                self.wait_for_endswith_url("object")
                logger.info(f"Web Page title: {self.driver.title}")
                res_dict["external_storage_cluster_obj_tab_OK"] = True
            except WebDriverException:
                logger.error("External cluster / Object tab is not working")
                self.take_screenshot("objectr_tab_external_storage_cluster")
                res_dict["external_storage_cluster_obj_tab_OK"] = False

        buckets_tab = (
            PageNavigator()
            .nav_storage_data_foundation_overview_page()
            .navigate_to_view_buckets()
        )
        backing_store_tab = buckets_tab.nav_backing_store_tab()
        backing_store_tab.nav_to_backing_store(constants.DEFAULT_NOOBAA_BACKINGSTORE)

        log_step(
            "Verify if Backing Store is present and link to Backing Store resource works"
        )
        res_dict["backing_store_status_ready"] = (
            backing_store_tab.validate_backing_store_ready()
        )

        log_step("Navigate to Backing Store tab via breadcrumb")
        backing_store_tab.nav_backing_store_list_breadcrumb()

        log_step("Navigate to Bucket class tab")
        bucket_class_tab = backing_store_tab.nav_bucket_class_tab()

        log_step("Navigate to the default Bucket Class details via Bucket Class tab")
        bucket_class_tab.nav_to_bucket_class(constants.DEFAULT_NOOBAA_BUCKETCLASS)

        log_step(
            f"Verify the status of a default bucket class: '{constants.DEFAULT_NOOBAA_BUCKETCLASS}'"
        )
        res_dict["bucket_class_status"] = bucket_class_tab.validate_bucket_class_ready()

        log_step("Navigate to Bucket class via breadcrumb")
        bucket_class_tab.nav_bucket_class_breadcrumb()

        log_step(
            "Navigate to Namespace Store tab via Bucket Class tab, verify if it works"
        )

        namespace_store_tab = bucket_class_tab.nav_namespace_store_tab()
        res_dict["namespace_store_tab_works"] = (
            namespace_store_tab.is_namespace_store_tab_active()
        )

        if storage_on_cluster or multi_storagecluster:
            log_step("Navigate to ODF Storage Cluster via Page navigator")
            storage_cluster_page = PageNavigator().nav_storage_cluster_default_page()
            logger.info(f"Web Page title: {self.driver.title}")
            res_dict["block_and_file_tab_is_active_2"] = (
                storage_cluster_page.validate_block_and_file_tab_active()
            )

        if external_mode or multi_storagecluster:
            PageNavigator().nav_external_systems_page()
            try:
                self.wait_for_endswith_url("external-systems")
                logger.info(f"Web Page title: {self.driver.title}")
                res_dict["external_systems_page_OK"] = True
            except WebDriverException:
                logger.error("External Systems page is not opening")
                res_dict["external_systems_page_OK"] = False

        self.take_screenshot("summary_of_odf_ui_checks")
        log_step("Process results")
        res_pd = pd.DataFrame.from_dict(res_dict, orient="index", columns=["check"])
        logger.info(res_pd.to_markdown(headers="keys", index=True, tablefmt="grid"))

        if not all(res_dict.values()):
            failed_checks = [check for check, res in res_dict.items() if not res]
            pytest.fail(
                "Following checks failed. 1 - Pass, 0 - Fail. \n{}".format(
                    failed_checks
                )
            )

    def odf_storagesystems_ui(self):
        """
        Function to verify changes and validate elements on ODF Storage Systems tab for ODF 4.9

        """
        self.odf_console_plugin_check()
        storage_systems_page = (
            PageNavigator().nav_storage_cluster_default_page().nav_storage_systems_tab()
        )
        storage_system_details = (
            storage_systems_page.nav_storagecluster_storagesystem_details()
        )
        storage_system_details.nav_details_overview()
        storage_system_details.nav_details_object()

        if not config.ENV_DATA["mcg_only_deployment"]:
            storage_system_details.nav_block_and_file()
        if not (
            config.DEPLOYMENT.get("external_mode")
            or config.ENV_DATA["mcg_only_deployment"]
            or config.ENV_DATA["platform"].lower()
            in constants.HCI_PROVIDER_CLIENT_PLATFORMS
        ):
            storage_system_details.nav_cephblockpool_verify_statusready()

    def check_capacity_breakdown(self, project_name, pod_name):
        """
        Check Capacity Breakdown

        Args:
            project_name (str): The name of the project
            pod_name (str): The name of pod

        Returns:
            bool: True if project_name and pod_name exist on capacity_breakdown, False otherwise

        """
        self.navigate_cluster_overview_page()
        if self.ocp_version == "4.7":
            self.do_click(self.validation_loc["persistent_storage_tab"])
        self.choose_expanded_mode(
            mode=True, locator=self.validation_loc["capacity_breakdown_options"]
        )
        self.do_click(self.validation_loc["capacity_breakdown_projects"])
        self.take_screenshot()
        res = True
        sample = TimeoutSampler(
            timeout=30,
            sleep=2,
            func=self.check_element_text,
            expected_text=project_name,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(f"The project {project_name} not found on capacity_breakdown")
            res = False

        self.choose_expanded_mode(
            mode=True, locator=self.validation_loc["capacity_breakdown_options"]
        )
        self.do_click(self.validation_loc["capacity_breakdown_pods"])
        self.take_screenshot()

        sample = TimeoutSampler(
            timeout=30,
            sleep=2,
            func=self.check_element_text,
            expected_text=pod_name,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(f"The pod {pod_name} not found on capacity_breakdown")
            res = False
        return res

    def validate_unprivileged_access(self):
        """
        Function to verify the unprivileged users can't access ODF dashbaord
        """

        self.select_administrator_user()
        try:
            self.nav_storage_cluster_default_page()
        except TimeoutException:
            logger.info(
                "As expected, ODF dashboard is not available for the unprivileged user"
            )
        else:
            raise UnexpectedODFAccessException

    def verify_odf_without_ocs_in_installed_operator(self) -> tuple:
        """
        Function to validate ODF operator is present post ODF installation,
        expectation is only ODF operator should be present in Installed operators tab and
        OCS operator shouldn't be present. This function is only written for 4.9+ versions

        Returns: tuple: odf_operator_presence, ocs_operator_presence

        """
        logger.info("Navigating to Installed Operator Page")
        self.navigate_installed_operators_page()

        self.select_namespace(project_name=config.ENV_DATA["cluster_namespace"])

        logger.info("Searching for Openshift Data Foundation Operator")
        odf_operator_presence = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["odf-operator"],
            timeout=1,
            expected_text="OpenShift Data Foundation",
        )
        logger.info("Searching for Openshift Container Storage Operator")
        ocs_operator_presence = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["ocs-operator"],
            timeout=1,
            expected_text="OpenShift Container Storage",
        )
        return odf_operator_presence, ocs_operator_presence

    def verify_storage_clients_page(self):
        """
        Verify storage clients page in UI

        Returns:
        StorageClients: Storage Clients page object

        """
        self.refresh_web_console()
        storage_client_obj = self.nav_to_storageclients_page()
        strings_storage_clients_tab = ["Storage clients", "Name"]
        self.verify_page_contain_strings(
            strings_on_page=strings_storage_clients_tab, page_name="storage clients"
        )
        self.do_click(
            self.validation_loc["generate_client_onboarding_token_button"],
            enable_screenshot=True,
        )
        strings_object_service_tab = [
            "Client onboarding token",
            "How to use this token",
        ]
        self.verify_page_contain_strings(
            strings_on_page=strings_object_service_tab,
            page_name="client_onboarding_token_page",
        )
        return storage_client_obj

    def calculate_est_days_and_average_manually(self):
        """
        Calculates the 'Estimated days until full' manually by:
        1. Get the age of the cluster in days
        2. Get used capacity of the cluster
        3. Get total capacity of the cluster
        4. Calculate average consumption of the storage per day
        5. Calculate the 'Estimated days until full' by using average and available capacity.

        Returns:
            tuple: Estimated days until full which calculated manually

        """
        number_of_days = get_age_of_cluster_in_days()
        logger.info(f"Age of the cluster in days: {number_of_days}")
        tpl_of_used_and_total_capacity = get_used_and_total_capacity_in_gibibytes()
        used_capacity = tpl_of_used_and_total_capacity[0]
        logger.info(f"The used capacity from the cluster is: {used_capacity}")
        total_capacity = tpl_of_used_and_total_capacity[1]
        available_capacity = total_capacity - used_capacity
        logger.info(f"The available capacity from the cluster is: {available_capacity}")
        average = used_capacity / number_of_days
        logger.info(f"Average of storage consumption per day: {average}")
        estimated_days_calculated = available_capacity / average
        manual_est = math.floor(estimated_days_calculated)
        logger.info(f"Estimated days calculated are {manual_est}")
        return (manual_est, average)
