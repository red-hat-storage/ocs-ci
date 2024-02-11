import logging
import warnings
import time

import pandas as pd
import pytest
from selenium.common.exceptions import TimeoutException
from ocs_ci.ocs.exceptions import UnexpectedODFAccessException
from ocs_ci.ocs.ui.page_objects.backing_store_tab import BackingStoreTab
from ocs_ci.ocs.ui.page_objects.namespace_store_tab import NameSpaceStoreTab
from ocs_ci.ocs.ui.page_objects.overview_tab import OverviewTab
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.page_objects.storage_system_details import StorageSystemDetails
from ocs_ci.utility import version
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.framework.logger_helper import log_step


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
        strings_details_tab = ["Description", "Succeeded", "openshift-storage"]
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
            timeout=120,
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
        default_projects_is_checked = self.driver.find_element_by_xpath(
            "//input[@type='checkbox']"
        )
        if default_projects_is_checked.get_attribute("data-checked-state") == "false":
            logger.info("Show default projects")
            self.do_click(self.validation_loc["show-default-projects"])
        logger.info("Search for 'openshift-storage' project")
        self.do_send_keys(
            self.validation_loc["project-search-bar"], text="openshift-storage"
        )
        logger.info("Select 'openshift-storage' project")
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
        Method to verify changes and validate elements on ODF Overview tab for ODF 4.9

        Steps:
        1. Validate ODF console plugin is enabled, if not enable it
        2. Navigate to ODF Default first tab
        3. Verify if Overview tab is active
        4. Verify if Storage System popup works
        5. Ensure that Block and File status, on Storage System popup is Ready
        6. Navigate to Storage System details via Storage System popup
        7. Verify only one Block Pool present on Storage System details page - optional. No BlockPools in External mode
        8. Navigate Storage System via breadcrumb
        9. Verify if Overview tab is active
        10. Verify if System Capacity Card is present
        11. Navigate to Storage System details via System Capacity Card - optional. Card not presented in External mode
        12. Verify if Storage System details breadcrumb is present - optional. If step 11 was performed
        13. Navigate to ODF Overview tab via tab bar - optional. If step 11 was performed
        14. Verify if Performance Card is present and link works
        15. Navigate to Storage System details via Performance Card
        16. Verify if Storage System details breadcrumb is present and link works
        17. Navigate ODF Backing store tab via Object Storage tab or PageNavigator
        18. Verify if Backing Store is present and link to Backing Store resource works
        19. Navigate to Backing Store tab via breadcrumb
        20. Navigate to Bucket class tab
        21. Navigate to the default Bucket Class details via Bucket Class tab
        22. Verify the status of a default Bucket Class
        23. Navigate to Bucket class via breadcrumb
        24. Navigate to Namespace Store tab via Bucket Class tab
        25. Navigate to ODF Overview tab via tab bar
        """
        res_dict = {}

        log_step("Validate ODF console plugin is enabled, if not enable it")
        self.odf_console_plugin_check()

        log_step("Navigate to ODF Default first tab")
        odf_overview_tab = self.nav_odf_default_page().nav_overview_tab()

        log_step("Verify if Overview tab is active")
        res_dict[
            "overview_tab_is_active_1"
        ] = odf_overview_tab.validate_overview_tab_active()

        log_step("Verify if Storage System popup works")
        res_dict[
            "storage_system_status_popup_present"
        ] = odf_overview_tab.wait_storagesystem_popup()
        odf_overview_tab.open_storage_popup_from_status_card()

        log_step("Ensure that Block and File status, on Storage System popup is Ready")
        is_block_and_file_healthy = odf_overview_tab.validate_block_and_file_ready()

        if not is_block_and_file_healthy:
            logger.critical("Block and File service is unhealthy, not a test failure")
            pytest.skip("Block and File service is unhealthy, not a test failure")

        log_step("Navigate to Storage System details via Storage System popup")
        storage_system_details_page = (
            odf_overview_tab.nav_storage_system_details_from_storage_status_popup()
        )

        if not config.DEPLOYMENT["external_mode"]:
            log_step(
                "Verify only one Block Pool present on Storage System details page"
            )
            res_dict[
                "blockpools_tabs_bz_2096513"
            ] = storage_system_details_page.check_only_one_block_pools_tab()

        log_step("Navigate Storage System via breadcrumb")
        storage_systems_tab = (
            storage_system_details_page.nav_storage_systems_via_breadcrumb()
        )

        log_step("Verify if Overview tab is active")
        odf_overview_tab = storage_systems_tab.nav_overview_tab()

        log_step("Verify if System Capacity Card is present")
        res_dict[
            "system_capacity_card_present"
        ] = odf_overview_tab.validate_system_capacity_card_present()

        if not config.DEPLOYMENT["external_mode"]:
            log_step("Navigate to Storage System details via System Capacity Card")
            storage_system_details_page = (
                odf_overview_tab.nav_storage_system_details_via_system_capacity_card()
            )

            log_step(
                "Verify if Storage System details breadcrumb is present and link works"
            )
            res_dict[
                "storagesystem-details-via-system-capacity-card-link-works"
            ] = (
                storage_system_details_page.is_storage_system_details_breadcrumb_present()
            )

            storage_systems_tab = (
                storage_system_details_page.nav_storage_systems_via_breadcrumb()
            )

            log_step("Navigate to ODF Overview tab via tab bar")
            odf_overview_tab = storage_systems_tab.nav_overview_tab()

        log_step("Verify if Performance Card is present and link works")
        res_dict[
            "performance_card_header_present"
        ] = odf_overview_tab.validate_performance_card_header_present()

        log_step("Navigate to Storage System details via Performance Card")
        storage_system_details_page = (
            odf_overview_tab.nav_storage_systems_details_via_performance_card()
        )

        log_step(
            "Verify if Storage System details breadcrumb is present and link works"
        )
        res_dict[
            "storagesystem-details-via-performance-card-link-works"
        ] = storage_system_details_page.is_storage_system_details_breadcrumb_present()

        storage_system_details_page.nav_storage_systems_via_breadcrumb()

        log_step(
            "Navigate ODF Backing store tab via Object Storage tab or PageNavigator"
        )
        # Starting from ODF 4.13 Object Storage is implemented as a separate page
        if self.ocp_version_semantic <= version.VERSION_4_13:
            logger.info("Click on Backing Store")
            self.do_click((self.validation_loc["backingstore"]))
            backing_store_tab = BackingStoreTab()
            backing_store_tab.nav_to_backing_store(
                constants.DEFAULT_NOOBAA_BACKINGSTORE
            )
        else:
            backing_store_tab = (
                StorageSystemDetails().nav_object_storage().nav_backing_store_tab()
            )
            backing_store_tab.nav_to_backing_store(
                constants.DEFAULT_NOOBAA_BACKINGSTORE
            )

        log_step(
            "Verify if Backing Store is present and link to Backing Store resource works"
        )
        res_dict[
            "backing_store_status_ready"
        ] = backing_store_tab.validate_backing_store_ready()

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
        if self.ocp_version_semantic <= version.VERSION_4_13:
            logger.info("Click on Namespace Store")
            self.do_click(
                (self.validation_loc["namespace-store"]), enable_screenshot=True
            )
            namespace_store_tab = NameSpaceStoreTab()
        else:
            namespace_store_tab = bucket_class_tab.nav_namespace_store_tab()
        res_dict[
            "namespace_store_tab_works"
        ] = namespace_store_tab.is_namespace_store_tab_active()

        log_step("Navigate to ODF Overview tab via tab bar")
        # Starting from ODF 4.13 Object Storage is implemented as a separate page and navigate via Overview tab
        # is not possible
        if self.ocp_version_semantic <= version.VERSION_4_13:
            self.do_click(
                locator=self.validation_loc["odf-overview"], enable_screenshot=True
            )
            odf_overview_tab = OverviewTab()
        else:
            odf_overview_tab = (
                namespace_store_tab.nav_odf_default_page().nav_overview_tab()
            )

        res_dict[
            "overview_tab_is_active_2"
        ] = odf_overview_tab.validate_overview_tab_active()
        logger.info("Navigated back to ODF tab under Storage. Check results below:")

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
            PageNavigator().nav_odf_default_page().nav_storage_systems_tab()
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
            == constants.HCI_PROVIDER_CLIENT_PLATFORMS
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

    def validate_storage_cluster_ui(self):
        """

        Function to validate Storage Cluster on UI for ODF 4.9 and above

        """
        if self.ocp_version_semantic >= version.VERSION_4_9:
            self.navigate_installed_operators_page()
            logger.info("Search and select openshift-storage namespace")
            self.select_namespace(project_name="openshift-storage")
            logger.info(
                "Click on Storage System under Provided APIs on Installed Operators Page"
            )
            self.do_click(self.validation_loc["storage-system-on-installed-operators"])
            logger.info(
                "Click on 'ocs-storagecluster-storagesystem' on Operator details page"
            )
            self.do_click(
                self.validation_loc["ocs-storagecluster-storgesystem"],
                enable_screenshot=True,
            )
            logger.info("Click on Resources")
            self.do_click(self.validation_loc["resources-tab"], enable_screenshot=True)
            logger.info("Checking Storage Cluster status on CLI")
            storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
            storage_cluster = StorageCluster(
                resource_name=storage_cluster_name,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            assert storage_cluster.check_phase("Ready")
            logger.info("Storage Cluster Status Check on UI")
            storage_cluster_status_check = self.wait_until_expected_text_is_found(
                locator=self.validation_loc["storage_cluster_readiness"],
                expected_text="Ready",
                timeout=600,
            )
            assert (
                storage_cluster_status_check
            ), "Storage Cluster Status reported on UI is not 'Ready'"
            logger.info(
                "Storage Cluster Status reported on UI is 'Ready', verification successful"
            )
            logger.info("Click on 'ocs-storagecluster'")
            self.do_click(
                self.validation_loc["ocs-storagecluster"], enable_screenshot=True
            )
            logger.info("Test passed!")
        else:
            warnings.warn("Not supported for OCP version less than 4.9")

    def validate_unprivileged_access(self):
        """
        Function to verify the unprivileged users can't access ODF dashbaord
        """

        self.select_administrator_user()
        try:
            self.nav_odf_default_page()
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

        :returns: tuple of two boolean values, first value is for ODF operator presence and
        second value is for OCS operator presence
        """
        logger.info("Navigating to Installed Operator Page")
        self.navigate_installed_operators_page()

        self.select_namespace(project_name="openshift-storage")

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
