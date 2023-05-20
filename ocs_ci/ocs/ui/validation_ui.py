import logging
import warnings
import time

from selenium.common.exceptions import TimeoutException
from ocs_ci.ocs.exceptions import UnexpectedODFAccessException
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.utility import version
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from selenium.common.exceptions import NoSuchElementException


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
        if (
            self.ocp_version_semantic >= version.VERSION_4_9
            and self.ocs_version_semantic >= version.VERSION_4_9
        ):
            self.navigate_installed_operators_page()
            logger.info("Click on project dropdown")
            self.do_click(self.validation_loc["project-dropdown"])
            default_projects_is_checked = self.driver.find_element_by_xpath(
                "//input[@type='checkbox']"
            )
            if (
                default_projects_is_checked.get_attribute("data-checked-state")
                == "false"
            ):
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
            self.page_has_loaded(retries=25, sleep_time=10)
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
                    logger.info("Console plugin status Enabled")
            else:
                logger.info("Plugin availability check skipped")

    def odf_overview_ui(
        self,
    ):
        """
        Function to verify changes and validate elements on ODF Overview tab for ODF 4.9

        """

        self.odf_console_plugin_check()
        odf_overview_page = self.nav_odf_default_page().nav_overview_tab()
        if odf_overview_page.wait_storagesystem_popup():
            logger.info("Click on 'Storage System' under Status card on Overview page")
            self.do_click(self.validation_loc["storagesystem-status-card"])
            block_and_file_health_message_check = (
                self.wait_until_expected_text_is_found(
                    locator=self.validation_loc["block-and-file-health-message"],
                    timeout=5,
                    expected_text="Block and File service is unhealthy",
                )
            )
            if block_and_file_health_message_check:
                logger.critical("Block and File service is unhealthy")
                warnings.warn("Block and File service is unhealthy")
            else:
                pass
            logger.info(
                "Click on storage system hyperlink from Storage System pop-up "
                "under Status Card on Data Foundation Overview page"
            )
            if config.DEPLOYMENT["external_mode"]:
                self.do_click(
                    self.validation_loc[
                        "storage-system-external-status-card-hyperlink"
                    ],
                    enable_screenshot=True,
                )
            else:
                self.do_click(
                    self.validation_loc["storage-system-status-card-hyperlink"],
                    enable_screenshot=True,
                )
            # verify that only one BlockPools tab is present. BZ #2096513
            blockpools_tabs = self.get_elements(self.validation_loc["blockpools"])
            if len(blockpools_tabs) > 1:
                logger.critical("Multiple BlockPools tabs were found. BZ #2096513")
                warnings.warn("Multiple BlockPools tabs were found. BZ #2096513")
            logger.info("Click on StorageSystems breadcrumb")
            self.do_click((self.validation_loc["storagesystems"]))
            logger.info("Navigate back to ODF Overview page")
            self.do_click((self.validation_loc["odf-overview"]))
        else:
            logger.critical(
                "Storage system under Status card on Data Foundation Overview tab is missing"
            )
            raise NoSuchElementException

        system_capacity_check = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["system-capacity"],
            expected_text="System Capacity",
        )
        if system_capacity_check:
            logger.info(
                "System Capacity Card found on OpenShift Data Foundation Overview page"
            )
        else:
            logger.critical(
                "System Capacity Card not found on OpenShift Data Foundation Overview page"
            )
            raise NoSuchElementException
        logger.info(
            "Navigate to System Capacity Card and Click on storage system hyperlink"
        )
        self.do_click(self.validation_loc["odf-capacityCardLink"])
        navigate_to_storagesystem_details_page = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["storagesystem-details"],
            timeout=15,
            expected_text="StorageSystem details",
        )
        if navigate_to_storagesystem_details_page:
            logger.info(
                "Successfully navigated to 'StorageSystem details' page from System Capacity Card"
            )
        else:
            logger.critical(
                "Couldn't navigate to 'StorageSystem details' page from System Capacity Card"
            )
            raise NoSuchElementException
        logger.info("Click on StorageSystems breadcrumb")
        self.do_click((self.validation_loc["storagesystems"]))
        logger.info("Navigate back to ODF Overview page")
        self.do_click((self.validation_loc["odf-overview"]))
        logger.info(
            "Now search for 'Performance' Card on Data Foundation Overview page"
        )
        performance_card = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["performance-card"],
            expected_text="Performance",
            timeout=15,
        )
        if performance_card:
            self.do_click(self.validation_loc["odf-performanceCardLink"])
        else:
            logger.critical(
                "Couldn't find 'Performance' card on Data Foundation Overview page"
            )
            raise NoSuchElementException
        navigate_to_storagesystem_details_page = self.wait_until_expected_text_is_found(
            locator=self.validation_loc["storagesystem-details"],
            timeout=15,
            expected_text="StorageSystem details",
        )
        if navigate_to_storagesystem_details_page:
            logger.info(
                "Successfully navigated to 'StorageSystem details' page from Performance Card"
            )
        else:
            logger.critical(
                "Couldn't navigate to 'StorageSystem details' page from Performance Card"
            )
            raise NoSuchElementException
        logger.info("Now again click on StorageSystems breadcrumb")
        self.do_click((self.validation_loc["storagesystems"]))
        logger.info("Click on Backing Store")
        self.do_click((self.validation_loc["backingstore"]))
        logger.info("Click on Backing Store Hyperlink")
        self.do_click(
            (self.validation_loc["backingstore-link"]), enable_screenshot=True
        )
        logger.info("Verifying the status of 'noobaa-default-backing-store'")
        backingstore_status = self.get_element_text(
            self.validation_loc["backingstore-status"]
        )
        assert "Ready" == backingstore_status, (
            f"backingstore status error | expected status:Ready \n "
            f"actual status:{backingstore_status}"
        )
        logger.info("Verification of backingstore status is successful!")
        logger.info("Click on backingstore breadcrumb")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                self.validation_loc["backingstorage-breadcrumb-odf-4-10"],
                enable_screenshot=True,
            )
        else:
            self.do_click((self.validation_loc["backingstorage-breadcrumb"]))
        logger.info("Click on Bucket Class")
        self.do_click((self.validation_loc["bucketclass"]))
        logger.info("Click on Bucket Class Hyperlink")
        self.do_click((self.validation_loc["bucketclass-link"]), enable_screenshot=True)
        logger.info("Verifying the status of 'noobaa-default-bucket-class'")
        bucketclass_status = self.get_element_text(
            self.validation_loc["bucketclass-status"]
        )
        assert "Ready" == bucketclass_status, (
            f"bucketclass status error | expected status:Ready \n "
            f"actual status:{bucketclass_status}"
        )
        logger.info("Verification of bucketclass status is successful!")
        logger.info("Click on bucketclass breadcrumb")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                (self.validation_loc["bucketclass-breadcrumb-odf-4-10"]),
                enable_screenshot=True,
            )
        else:
            self.do_click(
                (self.validation_loc["bucketclass-breadcrumb"]), enable_screenshot=True
            )
        logger.info("Click on Namespace Store")
        self.do_click((self.validation_loc["namespace-store"]), enable_screenshot=True)
        logger.info("Navigate again to ODF Overview page")
        self.do_click((self.validation_loc["odf-overview"]), enable_screenshot=True)
        logger.info(
            "Successfully navigated back to ODF tab under Storage, test successful!"
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
        if self.ocp_version_semantic > version.VERSION_4_9:
            self.navigate_installed_operators_page()
            logger.info("Search and select openshift-storage namespace")
            self.do_click(self.validation_loc["pvc_project_selector"])
            self.do_send_keys(
                self.validation_loc["search-project"], text="openshift-storage"
            )
            self.wait_for_namespace_selection(project_name="openshift-storage")
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
            logger.info("Storage Cluster Status Check")
            storage_cluster_status_check = self.wait_until_expected_text_is_found(
                locator=self.validation_loc["storage_cluster_readiness"],
                expected_text="Ready",
                timeout=600,
            )
            assert (
                storage_cluster_status_check
            ), "Storage Cluster Status reported on UI is not 'Ready', Timeout 1200 seconds exceeded"
            logger.info(
                "Storage Cluster Status reported on UI is 'Ready', verification successful"
            )
            logger.info("Click on 'ocs-storagecluster")
            self.do_click(
                self.validation_loc["ocs-storagecluster"], enable_screenshot=True
            )
        else:
            warnings.warn("Not supported for OCP version less than 4.9")

    def validate_unprivileged_access(self):
        """
        Function to verify the unprivileged users can't access ODF dashbaord
        """
        self.do_click(self.validation_loc["developer_dropdown"])
        self.do_click(self.validation_loc["select_administrator"], timeout=5)
        try:
            self.nav_odf_default_page()
        except TimeoutException:
            logger.info(
                "As expected, ODF dashboard is not available for the unprivileged user"
            )
        else:
            raise UnexpectedODFAccessException

    def verify_odf_without_ocs_in_installed_operator(self) -> bool:
        """
        Function to validate ODF operator is present post ODF installation,
        expectation is only ODF operator should be present in Installed operators tab and
        OCS operator shouldn't be present. This function is only written for 4.9+ versions

        Returns:
        True: If only odf operator is present in the UI
        False: If ocs operator is also present in the UI
        """
        logger.info("Navigating to Installed Operator Page")
        self.navigate_installed_operators_page()
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
        return odf_operator_presence and not ocs_operator_presence
