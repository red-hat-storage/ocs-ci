from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs.ui.base_ui import BaseUI, logger
from ocs_ci.ocs.ui.views import ODF_OPERATOR, OCS_OPERATOR
from ocs_ci.utility import version
from ocs_ci.utility.utils import TimeoutSampler


class PageNavigator(BaseUI):
    """
    Page Navigator Class

    """

    def __init__(self):
        super().__init__()

        self.operator_name = (
            ODF_OPERATOR
            if self.ocs_version_semantic >= version.VERSION_4_9
            else OCS_OPERATOR
        )
        if config.DEPLOYMENT.get("local_storage", False):
            self.storage_class = "localblock_sc"
        elif config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            if self.ocs_version_semantic >= version.VERSION_4_13:
                self.storage_class = "thin-csi_sc"
            else:
                self.storage_class = "thin_sc"
        elif config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            aws_sc = config.DEPLOYMENT.get("customized_deployment_storage_class")
            if aws_sc == "gp3-csi":
                self.storage_class = "gp3-csi_sc"
            elif aws_sc == "gp2-csi":
                self.storage_class = "gp2-csi_sc"
            else:
                if self.running_ocp_semantic_version >= version.VERSION_4_12:
                    self.storage_class = "gp2-csi_sc"
                else:
                    self.storage_class = "gp2_sc"
        elif config.ENV_DATA["platform"].lower() == constants.AZURE_PLATFORM:
            if self.ocp_version_semantic >= version.VERSION_4_11:
                self.storage_class = "managed-csi_sc"
            else:
                self.storage_class = "managed-premium_sc"
        elif config.ENV_DATA["platform"].lower() == constants.GCP_PLATFORM:
            if self.ocs_version_semantic < version.VERSION_4_12:
                self.storage_class = "standard_sc"
            else:
                self.storage_class = "standard_csi_sc"
        self.page_has_loaded(5, 2, self.page_nav["page_navigator_sidebar"])

    def navigate_OCP_home_page(self):
        """
        Navigate to Home Page
        """
        logger.info("Navigate to OCP Home Page")
        self.driver.get(get_ocp_url())
        self.page_has_loaded(retries=10, sleep_time=1)

    def navigate_storage(self):
        logger.info("Navigate to ODF tab under Storage section")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])

        from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
            DataFoundationDefaultTab,
        )

        return DataFoundationDefaultTab()

    def navigate_cluster_overview_page(self):
        """
        Navigate to Cluster Overview Page

        """
        logger.info("Navigate to Cluster Overview Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["overview_page"])

    def nav_odf_default_page(self):
        """
        Navigate to OpenShift Data Foundation default page
        Default Data foundation page is Overview at ODF 4.13
        """

        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["odf_tab_new"], timeout=90)
        self.page_has_loaded(retries=15)
        logger.info("Successfully navigated to ODF tab under Storage section")

        from ocs_ci.ocs.ui.page_objects.overview_tab import OverviewTab

        default_tab = OverviewTab()
        logger.info(f"Default page is {self.driver.title}")
        return default_tab

    def nav_object_storage(self):
        """
        Navigate to Object Storage Page

        Returns:
            ObjectService: ObjectService page object
        """
        self.navigate_storage()
        self.do_click(locator=self.page_nav["object_storage"], timeout=90)

        from ocs_ci.ocs.ui.page_objects.object_service import ObjectService

        return ObjectService()

    def nav_object_storage_page(self):
        """
        Navigate to Object Storage page

        """

        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(locator=self.page_nav["object_storage_page"], timeout=90)
        self.page_has_loaded(retries=15)
        from ocs_ci.ocs.ui.page_objects.object_service import ObjectService

        return ObjectService()

    def navigate_quickstarts_page(self):
        """
        Navigate to Quickstarts Page

        """
        self.navigate_cluster_overview_page()
        logger.info("Navigate to Quickstarts Page")
        self.do_click(locator=self.page_nav["quickstarts"], enable_screenshot=True)

    def navigate_projects_page(self):
        """
        Navigate to Projects Page

        """
        logger.info("Navigate to Projects Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["projects_page"], enable_screenshot=False)

    def navigate_search_page(self):
        """
        Navigate to Search Page

        """
        logger.info("Navigate to Projects Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["search_page"], enable_screenshot=False)

    def navigate_explore_page(self):
        """
        Navigate to Explore Page

        """
        logger.info("Navigate to Explore Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["explore_page"], enable_screenshot=False)

    def navigate_events_page(self):
        """
        Navigate to Events Page

        """
        logger.info("Navigate to Events Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["events_page"], enable_screenshot=False)

    def navigate_operatorhub_page(self):
        """
        Navigate to OperatorHub Page

        """
        logger.info("Navigate to OperatorHub Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Operators"])
        self.do_click(
            locator=self.page_nav["operatorhub_page"], enable_screenshot=False
        )

    def navigate_installed_operators_page(self):
        """
        Navigate to Installed Operators Page

        """
        logger.info("Navigate to Installed Operators Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Operators"])
        self.page_has_loaded(retries=25, sleep_time=5)
        self.do_click(
            self.page_nav["installed_operators_page"], enable_screenshot=False
        )
        self.page_has_loaded(retries=25, sleep_time=5)
        if self.ocp_version_full >= version.VERSION_4_9:
            self.do_click(self.page_nav["drop_down_projects"])
            self.do_click(self.page_nav["choose_all_projects"])

    def navigate_to_ocs_operator_page(self):
        """
        Navigate to the OCS Operator management page
        """
        self.navigate_installed_operators_page()
        logger.info("Select openshift-storage project")
        self.do_click(
            self.generic_locators["project_selector"], enable_screenshot=False
        )
        self.do_click(
            self.generic_locators["select_openshift-storage_project"],
            enable_screenshot=False,
        )

        logger.info("Enter the OCS operator page")
        self.do_click(self.generic_locators["ocs_operator"], enable_screenshot=False)

    def navigate_persistentvolumes_page(self):
        """
        Navigate to Persistent Volumes Page

        """
        logger.info("Navigate to Persistent Volumes Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.page_nav["persistentvolumes_page"], enable_screenshot=False
        )

    def navigate_persistentvolumeclaims_page(self):
        """
        Navigate to Persistent Volume Claims Page

        """
        logger.info("Navigate to Persistent Volume Claims Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.page_nav["persistentvolumeclaims_page"],
            enable_screenshot=True,
        )

    def navigate_storageclasses_page(self):
        """
        Navigate to Storage Classes Page

        """
        logger.info("Navigate to Storage Classes Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.page_nav["storageclasses_page"], enable_screenshot=False
        )

    def navigate_volumesnapshots_page(self):
        """
        Navigate to Storage Volume Snapshots Page

        """
        logger.info("Navigate to Storage Volume Snapshots Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.page_nav["volumesnapshots_page"], enable_screenshot=False
        )

    def navigate_volumesnapshotclasses_page(self):
        """
        Navigate to Volume Snapshot Classes Page

        """
        logger.info("Navigate to Volume Snapshot Classes Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.page_nav["volumesnapshotclasses_page"], enable_screenshot=False
        )

    def navigate_volumesnapshotcontents_page(self):
        """
        Navigate to Volume Snapshot Contents Page

        """
        logger.info("Navigate to Volume Snapshot Contents Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.page_nav["volumesnapshotcontents_page"],
            enable_screenshot=False,
        )

    def navigate_object_buckets_page(self):
        """
        Navigate to Object Buckets Page

        """

        return self.nav_object_storage().nav_object_buckets_tab()

    def navigate_object_bucket_claims_page(self):
        """
        Navigate to Object Bucket Claims Page

        """

        return self.nav_object_storage().nav_object_buckets_claims_tab()

    def navigate_alerting_page(self):
        """
        Navigate to Alerting Page

        """
        logger.info("Navigate to Alerting Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Monitoring"])
        self.do_click(locator=self.page_nav["alerting_page"], enable_screenshot=False)

    def navigate_metrics_page(self):
        """
        Navigate to Metrics Page

        """
        logger.info("Navigate to Metrics Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Monitoring"])
        self.do_click(locator=self.page_nav["metrics_page"], enable_screenshot=False)

    def navigate_dashboards_page(self):
        """
        Navigate to Dashboards Page

        """
        logger.info("Navigate to Dashboards Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Monitoring"])
        self.do_click(locator=self.page_nav["dashboards_page"], enable_screenshot=False)

    def navigate_pods_page(self):
        """
        Navigate to Pods Page

        """
        logger.info("Navigate to Pods Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Workloads"])
        self.do_click(locator=self.page_nav["Pods"], enable_screenshot=False)

    def navigate_block_pool_page(self):
        """
        Navigate to block pools page

        """
        logger.info("Navigate to block pools page")
        storage_system_details = (
            self.nav_odf_default_page()
            .nav_storage_systems_tab()
            .nav_storagecluster_storagesystem_details()
        )
        storage_system_details.nav_ceph_blockpool()
        logger.info("Now at Block pool page")

    def wait_for_namespace_selection(self, project_name):
        """
        If you have already navigated to namespace drop-down, this function waits for namespace selection on UI.
        It would be useful to avoid test failures in case of delays/latency in populating the list of projects under the
        namespace drop-down.
        The timeout is hard-coded to 10 seconds in the below function call which is more than sufficient.

        Args:
            project_name (str): Name of the project to be selected

        Returns:
            bool: True if the project is found, raises NoSuchElementException otherwise with a log message
        """

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        if self.ocp_version_full in (version.VERSION_4_10, version.VERSION_4_11):
            default_projects_is_checked = self.driver.find_element_by_xpath(
                "//span[@class='pf-c-switch__toggle']"
            )

            if (
                default_projects_is_checked.get_attribute("data-checked-state")
                == "false"
            ):
                logger.info("Show default projects")
                self.do_click(self.page_nav["show-default-projects"])
        else:
            default_projects_is_checked = self.driver.find_element_by_css_selector(
                "input[class='pf-c-switch__input']"
            )
            if (
                default_projects_is_checked.get_attribute("data-checked-state")
                == "false"
            ):
                logger.info("Show default projects")
                self.do_click(self.page_nav["show-default-projects"])

        logger.info(f"Wait and select namespace {project_name}")
        wait_for_project = self.wait_until_expected_text_is_found(
            locator=format_locator(self.pvc_loc["test-project-link"], project_name),
            expected_text=f"{project_name}",
            timeout=10,
        )
        if wait_for_project:
            self.do_click(
                format_locator(self.pvc_loc["test-project-link"], project_name)
            )
            logger.info(f"Namespace {project_name} selected")
        else:
            raise NoSuchElementException(f"Namespace {project_name} not found on UI")

    def verify_current_page_resource_status(self, status_to_check, timeout=30):
        """
        Compares a given status string to the one shown in the resource's UI page

        Args:
            status_to_check (str): The status that will be compared with the one in the UI
            timeout (int): How long should the check run before moving on

        Returns:
            bool: True if the resource was found, False otherwise
        """

        def _retrieve_current_status_from_ui():
            resource_status = WebDriverWait(self.driver, timeout).until(
                ec.visibility_of_element_located(
                    self.generic_locators["resource_status"][::-1]
                )
            )
            logger.info(f"Resource status is {resource_status.text}")
            return resource_status

        logger.info(
            f"Verifying that the resource has reached a {status_to_check} status"
        )
        try:
            for resource_ui_status in TimeoutSampler(
                timeout,
                3,
                _retrieve_current_status_from_ui,
            ):
                if resource_ui_status.text.lower() == status_to_check.lower():
                    return True
        except TimeoutExpiredError:
            logger.error(
                "The resource did not reach the expected state within the time limit."
            )
            return False
