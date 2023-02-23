from pathlib import Path
import datetime
import logging
import os
import gc
import time
import zipfile

from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from semantic_version.base import Version
from urllib.parse import urlparse
from webdriver_manager.chrome import ChromeDriverManager

from ocs_ci.framework import config
from ocs_ci.framework import config as ocsci_config
from ocs_ci.helpers.helpers import get_current_test_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    NotSupportedProxyConfiguration,
    TimeoutExpiredError,
    PageNotLoaded,
    CephHealthException,
)
from ocs_ci.ocs.ui.views import OCS_OPERATOR, ODF_OPERATOR
from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.templating import Templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility import version
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_kubeadmin_password,
    get_ocp_version,
)

logger = logging.getLogger(__name__)


class BaseUI:
    """
    Base Class for UI Tests

    """

    def __init__(self):
        self.driver = SeleniumDriver()
        if self.__class__.__name__ != BaseUI.__name__:
            logger.info(f"You are on {repr(self)}")
        base_ui_logs_dir = os.path.join(
            os.path.expanduser(ocsci_config.RUN["log_dir"]),
            f"ui_logs_dir_{ocsci_config.RUN['run_id']}",
        )
        logger.info(f"UI logs directory class {base_ui_logs_dir}")
        self.screenshots_folder = os.path.join(
            base_ui_logs_dir,
            "screenshots_ui",
            get_current_test_name(),
        )
        self.dom_folder = os.path.join(
            base_ui_logs_dir,
            "dom",
            get_current_test_name(),
        )
        if not os.path.isdir(self.screenshots_folder):
            Path(self.screenshots_folder).mkdir(parents=True, exist_ok=True)
        logger.info(f"screenshots pictures:{self.screenshots_folder}")

        if not os.path.isdir(self.dom_folder):
            Path(self.dom_folder).mkdir(parents=True, exist_ok=True)
        logger.info(f"screenshots pictures:{self.dom_folder}")

    def __repr__(self):
        return f"{self.__class__.__name__} Web Page"

    def do_click(self, locator, timeout=30, enable_screenshot=False, copy_dom=False):
        """
        Click on Button/link on OpenShift Console

        locator (set): (GUI element needs to operate on (str), type (By))
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.
        enable_screenshot (bool): take screenshot
        copy_dom (bool): copy page source of the webpage
        """
        if (
            version.get_semantic_version(get_ocp_version(), True)
            <= version.VERSION_4_11
        ):
            try:
                wait = WebDriverWait(self.driver, timeout)
                element = wait.until(
                    ec.element_to_be_clickable((locator[1], locator[0]))
                )
                screenshot = (
                    ocsci_config.UI_SELENIUM.get("screenshot") and enable_screenshot
                )
                if screenshot:
                    self.take_screenshot()
                element.click()
                if copy_dom:
                    self.copy_dom()
            except TimeoutException as e:
                self.take_screenshot()
                self.copy_dom()
                logger.error(e)
                raise TimeoutException
        else:
            self.page_has_loaded()
            wait = WebDriverWait(self.driver, timeout)
            element = wait.until(
                ec.visibility_of_element_located((locator[1], locator[0]))
            )
            element.click()

    def do_click_by_id(self, id, timeout=30):
        return self.do_click((id, By.ID), timeout)

    def do_send_keys(self, locator, text, timeout=30):
        """
        Send text to element on OpenShift Console

        locator (set): (GUI element needs to operate on (str), type (By))
        text (str): Send text to element
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        if (
            version.get_semantic_version(get_ocp_version(), True)
            <= version.VERSION_4_11
        ):
            try:
                wait = WebDriverWait(self.driver, timeout)
                element = wait.until(
                    ec.presence_of_element_located((locator[1], locator[0]))
                )
                element.send_keys(text)
            except TimeoutException as e:
                self.take_screenshot()
                logger.error(e)
                raise TimeoutException
        else:
            self.page_has_loaded()
            wait = WebDriverWait(self.driver, timeout)
            element = wait.until(
                ec.visibility_of_element_located((locator[1], locator[0]))
            )
            element.send_keys(text)

    def is_expanded(self, locator, timeout=30):
        """
        Check whether an element is in an expanded or collapsed state

        Args:
            locator (set): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        return:
            bool: True if element expended, False otherwise

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        return True if element.get_attribute("aria-expanded") == "true" else False

    def choose_expanded_mode(self, mode, locator):
        """
        Select the element mode (expanded or collapsed)

        mode (bool): True if element expended, False otherwise
        locator (set): (GUI element needs to operate on (str), type (By))

        """
        current_mode = self.is_expanded(locator=locator, timeout=100)
        if mode != current_mode:
            self.do_click(locator=locator, enable_screenshot=False)

    def get_checkbox_status(self, locator, timeout=30):
        """
        Checkbox Status

        Args:
            locator (set): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        return:
            bool: True if element is Enabled, False otherwise

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        return element.is_selected()

    def select_checkbox_status(self, status, locator):
        """
        Select checkbox status (enable or disable)

        status (bool): True if checkbox enable, False otherwise
        locator (set): (GUI element needs to operate on (str), type (By))

        """
        current_status = self.get_checkbox_status(locator=locator)
        if status != current_status:
            self.do_click(locator=locator)

    def check_element_text(self, expected_text, element="*"):
        """
        Check if the text matches the expected text.

        Args:
            expected_text (string): The expected text.

        return:
            bool: True if the text matches the expected text, False otherwise

        """
        element_list = self.driver.find_elements_by_xpath(
            f"//{element}[contains(text(), '{expected_text}')]"
        )
        return len(element_list) > 0

    def get_element_text(self, locator):
        """
        Get the inner text of an element in locator.

        Args:
            locator (set): (GUI element needs to operate on (str), type (By)).

        Return:
            str: The text captured.
        """
        return self.driver.find_element(by=locator[1], value=locator[0]).text

    def get_elements(self, locator):
        """
        Get an elements list. Useful to count number of elements presented on page, etc.

        Args:
            locator (set): (GUI element needs to operate on (str), type (By)).

        Return:
            list: The list of WebElements
        """
        return self.driver.find_elements(by=locator[1], value=locator[0])

    def page_has_loaded(self, retries=5, sleep_time=1):
        """
        Waits for page to completely load by comparing current page hash values.
        Not suitable for pages that use frequent dynamically content (less than sleep_time)

        Args:
            retries (int): How much time in sleep_time to wait for page to load
            sleep_time (int): Time to wait between every pool of dom hash

        """

        def get_page_hash():
            """
            Get dom html hash
            """
            dom = self.driver.find_element_by_tag_name("html").get_attribute(
                "innerHTML"
            )
            dom_hash = hash(dom.encode("utf-8"))
            return dom_hash

        page_hash = "empty"
        page_hash_new = ""

        # comparing old and new page DOM hash together to verify the page is fully loaded
        retry_counter = 0
        while page_hash != page_hash_new:
            if retry_counter > 0:
                logger.info(f"page not loaded yet: {self.driver.current_url}")
            retry_counter += 1
            page_hash = get_page_hash()
            time.sleep(sleep_time)
            page_hash_new = get_page_hash()
            if retry_counter == retries:
                raise PageNotLoaded(
                    f"Current URL did not finish loading in {retries * sleep_time}"
                )
        logger.info(f"page loaded: {self.driver.current_url}")

    def refresh_page(self):
        """
        Refresh Web Page

        """
        self.driver.refresh()

    def navigate_backward(self):
        """
        Navigate to a previous Web Page

        """
        self.driver.back()

    def scroll_into_view(self, locator):
        """
        Scroll element into view

        """
        actions = ActionChains(self.driver)
        element = self.driver.find_element(locator[1], locator[0])
        actions.move_to_element(element).perform()

    def take_screenshot(self):
        """
        Take screenshot using python code

        """
        time.sleep(1)
        filename = os.path.join(
            self.screenshots_folder,
            f"{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')}.png",
        )
        logger.debug(f"Creating screenshot: {filename}")
        self.driver.save_screenshot(filename)
        time.sleep(0.5)

    def copy_dom(self):
        """
        Get page source of the webpage

        """
        filename = os.path.join(
            self.dom_folder,
            f"{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')}_DOM.txt",
        )
        logger.info(f"Copy DOM file: {filename}")
        html = self.driver.page_source
        with open(filename, "w") as f:
            f.write(html)

    def do_clear(self, locator, timeout=30):
        """
        Clear the existing text from UI

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element until timeout (sec) occurs

        """
        wait = WebDriverWait(self.driver, timeout)
        element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        element.clear()

    def wait_until_expected_text_is_found(self, locator, expected_text, timeout=60):
        """
        Method to wait for a expected text to appear on the UI (use of explicit wait type),
        this method is helpful in working with elements which appear on completion of certain action and
        ignores all the listed exceptions for the given timeout.

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By))
            expected_text (str): Text which needs to be searched on UI
            timeout (int): Looks for a web element repeatedly until timeout (sec) occurs

        Returns:
            bool: Returns True if the expected element text is found, False otherwise

        """
        wait = WebDriverWait(
            self.driver,
            timeout=timeout,
            poll_frequency=1,
        )
        try:
            wait.until(
                ec.text_to_be_present_in_element(
                    (locator[1], locator[0]), expected_text
                )
            )
            return True
        except TimeoutException:
            self.take_screenshot()
            logger.warning(
                f"Locator {locator[1]} {locator[0]} did not find text {expected_text}"
            )
            return False

    def check_element_presence(self, locator, timeout=5):
        """
        Check if an web element is present on the web console or not.


        Args:
             locator (tuple): (GUI element needs to operate on (str), type (By))
             timeout (int): Looks for a web element repeatedly until timeout (sec) occurs
        Returns:
            bool: True if the element is found, returns False otherwise and raises NoSuchElementException

        """
        try:
            ignored_exceptions = (
                NoSuchElementException,
                StaleElementReferenceException,
            )
            wait = WebDriverWait(
                self.driver,
                timeout=timeout,
                ignored_exceptions=ignored_exceptions,
                poll_frequency=1,
            )
            wait.until(ec.presence_of_element_located(locator))
            return True
        except (NoSuchElementException, StaleElementReferenceException):
            logger.error("Expected element not found on UI")
            self.take_screenshot()
            return False
        except TimeoutException:
            logger.error(f"Timedout while waiting for element with {locator}")
            self.take_screenshot()
            return False

    def wait_for_endswith_url(self, endswith, timeout=60):
        """
        Wait for endswith url to load

        Args:
            endswith (string): url endswith string for which we need to wait
            timeout (int): Timeout in seconds

        """
        wait = WebDriverWait(self.driver, timeout=timeout)
        wait.until(ec.url_matches(endswith))


class PageNavigator(BaseUI):
    """
    Page Navigator Class

    """

    ocp_version = get_ocp_version()
    running_ocp_semantic_version = version.get_semantic_ocp_running_version()
    ocp_version_full = version.get_semantic_ocp_version_from_config()
    ocs_version_semantic = version.get_semantic_ocs_version_from_config()
    ocp_version_semantic = version.get_semantic_ocp_version_from_config()

    page_nav = locators[ocp_version]["page"]
    generic_locators = locators[ocp_version]["generic"]
    validation_loc = locators[ocp_version]["validation"]
    dep_loc = locators[ocp_version]["deployment"]

    operator_name = (
        ODF_OPERATOR if ocs_version_semantic >= version.VERSION_4_9 else OCS_OPERATOR
    )
    if config.DEPLOYMENT.get("local_storage", False):
        storage_class = "localblock_sc"
    elif config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
        storage_class = "thin_sc"
    elif config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
        aws_sc = config.DEPLOYMENT.get("customized_deployment_storage_class")
        if aws_sc == "gp3-csi":
            storage_class = "gp3-csi_sc"
        elif aws_sc == "gp2-csi":
            storage_class = "gp2-csi_sc"
        else:
            if running_ocp_semantic_version >= version.VERSION_4_12:
                storage_class = "gp2-csi_sc"
            else:
                storage_class = "gp2_sc"
    elif config.ENV_DATA["platform"].lower() == constants.AZURE_PLATFORM:
        if ocp_version_semantic >= version.VERSION_4_11:
            storage_class = "managed-csi_sc"
        else:
            storage_class = "managed-premium_sc"
    elif config.ENV_DATA["platform"].lower() == constants.GCP_PLATFORM:
        if ocs_version_semantic < version.VERSION_4_12:
            storage_class = "standard_sc"
        else:
            storage_class = "standard_csi_sc"

    def __init__(self):
        super().__init__()

    def navigate_storage(self):
        logger.info("Navigate to ODF tab under Storage section")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        return DataFoundationDefaultTab()

    def navigate_cluster_overview_page(self):
        """
        Navigate to Cluster Overview Page

        """
        logger.info("Navigate to Cluster Overview Page")
        if Version.coerce(self.ocp_version) >= Version.coerce("4.8"):
            self.choose_expanded_mode(mode=False, locator=self.page_nav["Home"])
            self.navigate_storage()
        else:
            self.choose_expanded_mode(mode=True, locator=self.page_nav["Home"])
        self.do_click(locator=self.page_nav["overview_page"])

    def nav_odf_default_page(self):
        """
        Navigate to OpenShift Data Foundation default page
        If OCP version >= 4.13 Storage / Data foundation opens Topology
        If OCP version > 4.10 and OCP version < 4.13 Storage / Data foundation opens Overview
        If OCP version < 4.10 Storage / OpenShift Data Foundation opens Overview
        """

        ocs_version = version.get_semantic_ocs_version_from_config()

        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        if (
            self.ocp_version_full >= version.VERSION_4_10
            and ocs_version >= version.VERSION_4_10
        ):
            self.do_click(locator=self.page_nav["odf_tab_new"], timeout=90)
        else:
            self.do_click(locator=self.page_nav["odf_tab"], timeout=90)
        self.page_has_loaded(retries=15)
        logger.info("Successfully navigated to ODF tab under Storage section")
        if (
            # Uncomment next line and remove line bellow when
            # ODF/Ceph topology in OpenShift Console merged and released
            # self.ocp_version_full >= version.VERSION_4_13
            self.ocp_version_full
            > version.VERSION_4_13
        ):
            default_tab = TopologyTab()
            logger.info(f"Default page is {self.driver.title}")
        else:
            default_tab = OverviewTab()
            logger.info(f"Default page is {self.driver.title}")
        return default_tab

    def navigate_quickstarts_page(self):
        """
        Navigate to Quickstarts Page

        """
        self.navigate_cluster_overview_page()
        logger.info("Navigate to Quickstarts Page")
        self.scroll_into_view(self.page_nav["quickstarts"])
        self.do_click(locator=self.page_nav["quickstarts"], enable_screenshot=False)

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
        logger.info("Navigate to Object Buckets Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.page_nav["object_buckets_page"], enable_screenshot=False
        )

    def navigate_object_bucket_claims_page(self):
        """
        Navigate to Object Bucket Claims Page

        """
        logger.info("Navigate to Object Bucket Claims Page")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.page_nav["object_bucket_claims_page"], enable_screenshot=False
        )

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
        self.navigate_to_ocs_operator_page()
        self.do_click(locator=self.page_nav["block_pool_link"])

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

        pvc_loc = locators[self.ocp_version]["pvc"]
        logger.info(f"Wait and select namespace {project_name}")
        wait_for_project = self.wait_until_expected_text_is_found(
            locator=format_locator(pvc_loc["test-project-link"], project_name),
            expected_text=f"{project_name}",
            timeout=10,
        )
        if wait_for_project:
            self.do_click(format_locator(pvc_loc["test-project-link"], project_name))
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


class DataFoundationTabBar(PageNavigator):
    def __init__(self):
        super().__init__()

    def nav_storage_systems_tab(self):
        """
        Navigate to Storage Systems tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Storage Systems")
        self.do_click(self.validation_loc["storage_systems"], enable_screenshot=True)
        self.page_has_loaded(retries=15, sleep_time=2)
        return StorageSystemTab()

    def nav_overview_tab(self):
        """
        Navigate to Overview tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Overview")
        # pay attention Overview loc will show twice if Home Page nav extended
        self.do_click(locator=self.page_nav["overview_page"])
        return OverviewTab()

    def nav_backing_store_tab(self):
        """
        Navigate to Backing Store tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Backing Store tab")
        self.do_click(locator=self.validation_loc["osc_backing_store_tab"])
        return BackingStoreTab()

    def nav_bucket_class_tab(self):
        """
        Navigate to Bucket class tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Bucket class tab")
        self.do_click(locator=self.validation_loc["osc_bucket_class_tab"])
        return BackingStoreTab()

    def nav_namespace_store_tab(self):
        """
        Navigate to Namespace Store tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Namespace Store tab")
        self.do_click(locator=self.validation_loc["namespacestore_page"])
        return NameStoreTab()

    # noinspection PyUnreachableCode
    def nav_topology_tab(self):
        """
        Navigate to ODF Topology tab. Accessible from any Data Foundation tabs
        """
        raise NotImplementedError(
            "ODF Topology view not ready. Feature awaited at ODF 4.13"
        )
        return TopologyTab()


class DataFoundationDefaultTab(DataFoundationTabBar):

    """
    Default Foundation default Tab: TopologyTab | OverviewTab
    """

    validation_loc = locators[PageNavigator.ocp_version]["validation"]

    def __init__(self):
        DataFoundationTabBar.__init__(self)


class TopologyTab(DataFoundationDefaultTab):
    """
    Topology tab Class
    Content of Data Foundation/Topology tab (default for ODF 4.13 and above)
    """

    def __init__(self):
        super().__init__()


class OverviewTab(DataFoundationDefaultTab):
    """
    Overiview tab Class
    Content of Data Foundation/Overview tab (default for ODF bellow 4.12)
    """

    def __init__(self):
        DataFoundationDefaultTab.__init__(self)

    def open_quickstarts_page(self):
        logger.info("Navigate to Quickstarts Page")
        self.scroll_into_view(self.page_nav["quickstarts"])
        self.do_click(locator=self.page_nav["quickstarts"], enable_screenshot=False)

    def wait_storagesystem_popup(self) -> bool:
        logger.info(
            "Wait and check for Storage System under Status card on Overview page"
        )
        return self.wait_until_expected_text_is_found(
            locator=self.validation_loc["storagesystem-status-card"],
            timeout=30,
            expected_text="Storage System",
        )


class StorageSystemTab(DataFoundationTabBar):
    """
    Storage System tab Class
    Content of Data Foundation/Storage Systems tab

    """

    def __init__(self):
        DataFoundationTabBar.__init__(self)

    def nav_storagecluster_storagesystem_details(self):
        """
        Initial page - Data Foundation / Storage Systems tab
        Navigate to StorageSystem details

        """
        if not config.DEPLOYMENT.get("external_mode"):
            logger.info(
                "Click on 'ocs-storagecluster-storagesystem' link from Storage Systems page"
            )
            self.do_click(
                self.validation_loc["ocs-storagecluster-storagesystem"],
                enable_screenshot=True,
            )
        else:
            logger.info(
                "Click on 'ocs-external-storagecluster-storagesystem' link "
                "from Storage Systems page for External Mode Deployment"
            )
            self.do_click(
                self.validation_loc["ocs-external-storagecluster-storagesystem"],
                enable_screenshot=True,
            )
        return StorageSystemDetails()


class StorageSystemDetails(StorageSystemTab):
    def __init__(self):
        StorageSystemTab.__init__(self)

    def nav_details_overview(self):
        logger.info("Click on Overview tab")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                self.validation_loc["overview_odf_4_10"], enable_screenshot=True
            )
        else:
            self.do_click(self.validation_loc["overview"], enable_screenshot=True)

    def nav_details_object(self):
        """
        Accessible only at StorageSystems / StorageSystem details / Overview
        ! At 'StorageSystems / StorageSystem details / BlockPools' Object page is not accessible
        """
        logger.info("Click on 'Object' tab")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                self.validation_loc["object-odf-4-10"], enable_screenshot=True
            )
        else:
            self.do_click(self.validation_loc["object"], enable_screenshot=True)

    def nav_block_and_file(self):
        """
        Accessible only at StorageSystems / StorageSystem details / Overview
        ! At 'StorageSystems / StorageSystem details / BlockPools' Block and file page is not accessible
        """
        logger.info("Click on 'Block and File' tab")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                self.validation_loc["blockandfile-odf-4-10"], enable_screenshot=True
            )
        else:
            self.do_click(self.validation_loc["blockandfile"], enable_screenshot=True)

    def nav_cephblockpool_verify_statusready(self):
        """
        Initial page - Data Foundation / Storage Systems tab / StorageSystem details
        Navigate to ocs-storagecluster-cephblockpool
        Verify cephblockpool status is 'Ready'

        Raises:
            CephHealthException if cephblockpool_status != 'Ready'
        """
        self.nav_ceph_blockpool()
        self.verify_cephblockpool_status()

    def nav_ceph_blockpool(self):
        logger.info("Click on 'BlockPools' tab")
        if (
            self.ocp_version_semantic == version.VERSION_4_11
            and self.ocs_version_semantic == version.VERSION_4_10
        ):
            self.do_click(
                self.validation_loc["blockpools-odf-4-10"],
                enable_screenshot=True,
            )
        else:
            self.do_click(self.validation_loc["blockpools"], enable_screenshot=True)
        self.page_has_loaded(retries=15, sleep_time=2)

    def verify_cephblockpool_status(self, status_exp: str = "Ready"):

        logger.info(f"Verifying the status of '{constants.DEFAULT_CEPHBLOCKPOOL}'")
        cephblockpool_status = self.get_element_text(
            self.validation_loc[f"{constants.DEFAULT_CEPHBLOCKPOOL}-status"]
        )
        if not status_exp == cephblockpool_status:
            raise CephHealthException(
                f"cephblockpool status error | expected status:Ready \n "
                f"actual status:{cephblockpool_status}"
            )

    def get_blockpools_compression_status_from_storagesystem(self) -> tuple:
        """
        Initial page - Data Foundation / Storage Systems tab / StorageSystem details / ocs-storagecluster-cephblockpool
        Get compression status from storagesystem details and ocs-storagecluster-cephblockpool

        Returns:
            tuple: String representation of 'Compression status' from StorageSystem details page and
            String representation of 'Compression status' from ocs-storagecluster-cephblockpool page

        """

        logger.info(
            f"Get the 'Compression status' of '{constants.DEFAULT_CEPHBLOCKPOOL}'"
        )
        compression_status_blockpools_tab = self.get_element_text(
            self.validation_loc["storagesystem-details-compress-state"]
        )
        logger.info(
            f"Click on '{constants.DEFAULT_CEPHBLOCKPOOL}' link under BlockPools tab"
        )
        self.do_click(
            self.validation_loc[constants.DEFAULT_CEPHBLOCKPOOL],
            enable_screenshot=True,
        )
        compression_status_blockpools_details = self.get_element_text(
            self.validation_loc["storagecluster-blockpool-details-compress-status"]
        )
        return compression_status_blockpools_tab, compression_status_blockpools_details

    def navigate_backward(self):
        BaseUI.navigate_backward(self)
        return StorageSystemTab()


class BackingStoreTab(DataFoundationDefaultTab):
    def __init__(self):
        DataFoundationTabBar.__init__(self)


class BucketClassTab(DataFoundationDefaultTab):
    def __init__(self):
        DataFoundationTabBar.__init__(self)


class NameStoreTab(DataFoundationDefaultTab):
    def __init__(self):
        DataFoundationTabBar.__init__(self)


def screenshot_dom_location(type_loc="screenshot"):
    """
    Get the location for copy DOM/screenshot

    Args:
        type_loc (str): if type_loc is "screenshot" the location for copy screeenshot else DOM

    """
    base_ui_logs_dir = os.path.join(
        os.path.expanduser(ocsci_config.RUN["log_dir"]),
        f"ui_logs_dir_{ocsci_config.RUN['run_id']}",
    )
    logger.info(f"UI logs directory function {base_ui_logs_dir}")
    if type_loc == "screenshot":
        return os.path.join(
            base_ui_logs_dir,
            "screenshots_ui",
            get_current_test_name(),
        )
    else:
        return os.path.join(
            base_ui_logs_dir,
            "dom",
            get_current_test_name(),
        )


def copy_dom():
    """
    Copy DOM using python code

    """
    dom_folder = screenshot_dom_location(type_loc="dom")
    if not os.path.isdir(dom_folder):
        Path(dom_folder).mkdir(parents=True, exist_ok=True)
    time.sleep(1)
    filename = os.path.join(
        dom_folder,
        f"{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')}_DOM.txt",
    )
    logger.info(f"Copy DOM file: {filename}")
    html = SeleniumDriver().page_source
    with open(filename, "w") as f:
        f.write(html)
    time.sleep(0.5)


def take_screenshot():
    """
    Take screenshot using python code

    Args:
        driver (Selenium WebDriver)

    """
    screenshots_folder = screenshot_dom_location(type_loc="screenshot")
    if not os.path.isdir(screenshots_folder):
        Path(screenshots_folder).mkdir(parents=True, exist_ok=True)
    time.sleep(1)
    filename = os.path.join(
        screenshots_folder,
        f"{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')}.png",
    )
    logger.debug(f"Creating screenshot: {filename}")
    SeleniumDriver().save_screenshot(filename)
    time.sleep(0.5)


def garbage_collector_webdriver():
    """
    Garbage Collector for webdriver objs

    """
    collected_objs = gc.get_objects()
    for obj in collected_objs:
        if str(type(obj)) == constants.WEB_DRIVER_CHROME_OBJ_TYPE:
            try:
                obj.quit()
            except WebDriverException as e:
                logger.error(e)


class SeleniumDriver(WebDriver):

    # noinspection PyUnresolvedReferences
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(SeleniumDriver, cls).__new__(cls)
            cls.instance.driver = cls._set_driver()
        return cls.instance.driver

    @classmethod
    def _set_driver(cls) -> WebDriver:
        browser = ocsci_config.UI_SELENIUM.get("browser_type")
        if browser == "chrome":
            logger.info("chrome browser")
            chrome_options = Options()

            ignore_ssl = ocsci_config.UI_SELENIUM.get("ignore_ssl")
            if ignore_ssl:
                chrome_options.add_argument("--ignore-ssl-errors=yes")
                chrome_options.add_argument("--ignore-certificate-errors")
                chrome_options.add_argument("--allow-insecure-localhost")
                if config.ENV_DATA.get("import_clusters_to_acm"):
                    # Dev shm should be disabled when sending big amonut characters,
                    # like the cert sections of a kubeconfig
                    chrome_options.add_argument("--disable-dev-shm-usage")
                capabilities = chrome_options.to_capabilities()
                capabilities["acceptInsecureCerts"] = True

            # headless browsers are web browsers without a GUI
            headless = ocsci_config.UI_SELENIUM.get("headless")
            if headless:
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("window-size=1920,1400")

            # use proxy server, if required
            if (
                config.DEPLOYMENT.get("proxy")
                or config.DEPLOYMENT.get("disconnected")
                or config.ENV_DATA.get("private_link")
            ) and config.ENV_DATA.get("client_http_proxy"):
                client_proxy = urlparse(config.ENV_DATA.get("client_http_proxy"))
                # there is a big difference between configuring not authenticated
                # and authenticated proxy server for Chrome:
                # * not authenticated proxy can be configured via --proxy-server
                #   command line parameter
                # * authenticated proxy have to be provided through customly
                #   created Extension and it doesn't work in headless mode!
                if not client_proxy.username:
                    # not authenticated proxy
                    logger.info(
                        f"Configuring not authenticated proxy ('{client_proxy.geturl()}') for browser"
                    )
                    chrome_options.add_argument(
                        f"--proxy-server={client_proxy.geturl()}"
                    )
                elif not headless:
                    # authenticated proxy, not headless mode
                    # create Chrome extension with proxy settings
                    logger.info(
                        f"Configuring authenticated proxy ('{client_proxy.geturl()}') for browser"
                    )
                    _templating = Templating()
                    manifest_json = _templating.render_template(
                        constants.CHROME_PROXY_EXTENSION_MANIFEST_TEMPLATE, {}
                    )
                    background_js = _templating.render_template(
                        constants.CHROME_PROXY_EXTENSION_BACKGROUND_TEMPLATE,
                        {"proxy": client_proxy},
                    )
                    pluginfile = "/tmp/proxy_auth_plugin.zip"
                    with zipfile.ZipFile(pluginfile, "w") as zp:
                        zp.writestr("manifest.json", manifest_json)
                        zp.writestr("background.js", background_js)
                    chrome_options.add_extension(pluginfile)
                else:
                    # authenticated proxy, headless mode
                    logger.error(
                        "It is not possible to configure authenticated proxy "
                        f"('{client_proxy.geturl()}') for browser in headless mode"
                    )
                    raise NotSupportedProxyConfiguration(
                        "Unable to configure authenticated proxy in headless browser mode!"
                    )

            chrome_browser_type = ocsci_config.UI_SELENIUM.get("chrome_type")
            driver = webdriver.Chrome(
                ChromeDriverManager(chrome_type=chrome_browser_type).install(),
                options=chrome_options,
            )
        else:
            raise ValueError(f"No Support on {browser}")
        return driver


@retry(
    exception_to_check=(TimeoutException, WebDriverException),
    tries=3,
    delay=3,
    backoff=2,
    func=garbage_collector_webdriver,
)
def login_ui(console_url=None, username=None, password=None):
    """
    Login to OpenShift Console

    Args:
        console_url (str): ocp console url
        username(str): User which is other than admin user,
        password(str): Password of user other than admin user

    return:
        driver (Selenium WebDriver)

    """
    default_console = False
    if not console_url:
        console_url = get_ocp_url()
        default_console = True
    logger.info("Get password of OCP console")
    if password is None:
        password = get_kubeadmin_password()
        password = password.rstrip()
    ocp_version = get_ocp_version()
    login_loc = locators[ocp_version]["login"]
    driver = SeleniumDriver()
    wait = WebDriverWait(driver, 40)
    driver.maximize_window()
    driver.implicitly_wait(10)
    driver.get(console_url)
    # Validate proceeding to the login console before taking any action:
    proceed_to_login_console(driver)
    try:
        if username is not None:
            element = wait.until(
                ec.element_to_be_clickable(
                    (
                        login_loc["username_my_htpasswd"][1],
                        login_loc["username_my_htpasswd"][0],
                    )
                )
            )
        else:
            element = wait.until(
                ec.element_to_be_clickable(
                    (
                        login_loc["kubeadmin_login_approval"][1],
                        login_loc["kubeadmin_login_approval"][0],
                    )
                )
            )
        element.click()
    except TimeoutException as e:
        take_screenshot()
        copy_dom()
        logger.error(e)
    element = wait.until(
        ec.element_to_be_clickable((login_loc["username"][1], login_loc["username"][0]))
    )
    take_screenshot()
    copy_dom()
    if username is None:
        username = constants.KUBEADMIN
    element.send_keys(username)
    element = wait.until(
        ec.element_to_be_clickable((login_loc["password"][1], login_loc["password"][0]))
    )
    element.send_keys(password)
    element = wait.until(
        ec.element_to_be_clickable(
            (login_loc["click_login"][1], login_loc["click_login"][0])
        )
    )
    element.click()
    if default_console is True and username is constants.KUBEADMIN:
        WebDriverWait(driver, 60).until(ec.title_is(login_loc["ocp_page"]))
    if username is not constants.KUBEADMIN:
        element = wait.until(ec.element_to_be_clickable((login_loc["skip_tour"])))
        element.click()
    return driver


def close_browser(driver):
    """
    Close Selenium WebDriver

    Args:
        driver (Selenium WebDriver)

    """
    logger.info("Close browser")
    take_screenshot()
    copy_dom()
    driver.quit()
    time.sleep(10)
    garbage_collector_webdriver()


def proceed_to_login_console(driver: WebDriver):
    """
    Proceed to the login console, if needed to confirm this action in a page that appears before.
    This is required to be as a solo function, because the driver initializes in the login_ui function.
    Function needs to be called just before login

    Args:
        driver (Selenium WebDriver)

    Returns:
        None

    """
    login_loc = locators[get_ocp_version()]["login"]
    if driver.title == login_loc["pre_login_page_title"]:
        proceed_btn = driver.find_element(
            by=login_loc["proceed_to_login_btn"][1],
            value=login_loc["proceed_to_login_btn"][0],
        )
        proceed_btn.click()
        WebDriverWait(driver, 60).until(ec.title_is(login_loc["login_page_title"]))
