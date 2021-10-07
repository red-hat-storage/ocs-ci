import logging

from selenium.webdriver.common.by import By

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility import version
from ocs_ci.utility.utils import get_ocp_version, TimeoutSampler
from ocs_ci.framework import config
from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


class ValidationUI(PageNavigator):
    """
    User Interface Validation Selenium

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.ocp_version = get_ocp_version()
        self.err_list = list()
        self.validation_loc = locators[self.ocp_version]["validation"]

    def verify_object_service_page(self):
        """
        Verify Object Service Page UI

        """
        self.navigate_overview_page()
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
        self.navigate_overview_page()
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


    def test_odf_dashboard_ui(self,
                              ):
        """
        Test to verify ODF dashboard changes in ODF 4.9

        """

        self.ocp_version = get_ocp_version()
        self.page_nav = locators[self.ocp_version]["page"]
        self.dashboard_items = locators[self.ocp_version]["dashboard"]
        self.dep_loc = locators[self.ocp_version]["deployment"]

        self.navigate_operatorhub_page()
        self.navigate_installed_operators_page()
        self.choose_expanded_mode(
            mode=True, locator=self.dep_loc["drop_down_projects"]
        )
        self.do_click(
            self.dep_loc["enable_default_porjects"], enable_screenshot=True
        )
        self.do_click(
            self.dep_loc["choose_openshift-storage_project"], enable_screenshot=True
        )
        self.choose_expanded_mode(
            mode=True, locator=self.dep_loc["drop_down_projects"]
        )
        self.do_click(self.dep_loc["choose_all_projects"], enable_screenshot=True)


        self.navigate_odf_overview_page()

        logger.info("Click on 'Storage Systems' tab")
        self.do_click(self.dashboard_items["storage_systems"], enable_screenshot=True)
        logger.info("Verifying the status of ocs-storagecluster-storagesystem")
        storagesystem_status = self.get_element_text(self.dashboard_items["ocs-storagecluster-storagesystem-status"])
        storage_system = OCP(kind=constants.STORAGESYSTEM, namespace=namespace)
        storage_system_data = storage_system.get()

        namespace = config.ENV_DATA["cluster_namespace"]
        ocs_version = version.get_semantic_ocs_version_from_config()

        if ocs_version >= version.VERSION_4_9:
            logger.info("Verifying storage system status")
            storage_system = OCP(kind=constants.STORAGESYSTEM, namespace=namespace)
            storage_system_data = storage_system.get()
            storage_system_status = {}
            for condition in storage_system_data["items"][0]["status"]["conditions"]:
                storage_system_status[condition["type"]] = condition["status"]
            log.debug(f"storage system status: {storage_system_status}")
            assert storage_system_status == constants.STORAGE_SYSTEM_STATUS, (
                f"Storage System status is not in expected state. Expected {constants.STORAGE_SYSTEM_STATUS}"
                f" but found {storage_system_status}"
            )

        assert "Ready" == storagesystem_status, (
            f"status error|expected status:Ready \n "
            f"actual status:{storagesystem_status}")
        logger.info("Click on ocs-storagecluster-storagesystem link under Storage Systems page")
        self.do_click(self.dashboard_items["ocs-storagecluster-storagesystem"], enable_screenshot=True)
        logger.info("Click on Overview tab")
        self.do_click(self.dashboard_items["overview"])
        logger.info("Click on 'Block and File' tab")
        self.do_click(self.dashboard_items["blockandfile"])
        logger.info("Click on 'BlockPools' tab")
        self.do_click(self.dashboard_items["blockpools"])
        logger.info("Verifying the status of ocs-storagecluster-cephblockpool")
        cephblockpool_status = self.get_element_text(self.dashboard_items["ocs-storagecluster-cephblockpool-status"])
        assert "Ready" == cephblockpool_status, (
            f"status error|expected status:Ready \n "
            f"actual status:{cephblockpool_status}")
        logger.info("Click on 'ocs-storagecluster-cephblockpool' link under BlockPools tab")
        self.do_click(self.dashboard_items["ocs-storagecluster-cephblockpool"])



    def check_capacity_breakdown(self, project_name, pod_name):
        """
        Check Capacity Breakdown

        Args:
            project_name (str): The name of the project
            pod_name (str): The name of pod

        Returns:
            bool: True if project_name and pod_name exist on capacity_breakdown, False otherwise

        """
        self.navigate_overview_page()
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
