import ipaddress
import random
import re
import string
from pathlib import Path
import datetime
import logging
import os
import gc
import time
import zipfile
from functools import reduce

import pandas as pd
import pytest
from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urlparse
from webdriver_manager.chrome import ChromeDriverManager

from ocs_ci.framework import config
from ocs_ci.framework import config as ocsci_config
from ocs_ci.helpers.helpers import get_current_test_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    NotSupportedProxyConfiguration,
    TimeoutExpiredError,
    CephHealthException,
    IncorrectUIOptionRequested,
)
from ocs_ci.ocs.ui.views import OCS_OPERATOR, ODF_OPERATOR
from ocs_ci.ocs.ocp import get_ocp_url, OCP
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

        self.ocp_version = get_ocp_version()
        self.running_ocp_semantic_version = version.get_semantic_ocp_running_version()
        self.ocp_version_full = version.get_semantic_ocp_version_from_config()
        self.ocs_version_semantic = version.get_semantic_ocs_version_from_config()
        self.ocp_version_semantic = version.get_semantic_ocp_version_from_config()

        self.page_nav = self.deep_get(locators, self.ocp_version, "page")
        self.generic_locators = self.deep_get(locators, self.ocp_version, "generic")
        self.validation_loc = self.deep_get(locators, self.ocp_version, "validation")
        self.dep_loc = self.deep_get(locators, self.ocp_version, "deployment")
        self.pvc_loc = self.deep_get(locators, self.ocp_version, "pvc")
        self.bp_loc = self.deep_get(locators, self.ocp_version, "block_pool")
        self.sc_loc = self.deep_get(locators, self.ocp_version, "storageclass")
        self.ocs_loc = self.deep_get(locators, self.ocp_version, "ocs_operator")
        self.bucketclass = self.deep_get(locators, self.ocp_version, "bucketclass")
        self.mcg_stores = self.deep_get(locators, self.ocp_version, "mcg_stores")
        self.acm_page_nav = self.deep_get(locators, self.ocp_version, "acm_page")
        self.obc_loc = self.deep_get(locators, self.ocp_version, "obc")
        self.add_capacity_ui_loc = self.deep_get(
            locators, self.ocp_version, "add_capacity"
        )

    def __repr__(self):
        return f"{self.__class__.__name__} Web Page"

    @classmethod
    def deep_get(cls, dictionary, *keys):
        return reduce(lambda d, key: d.get(key) if d else None, keys, dictionary)

    def do_click(
        self,
        locator,
        timeout=30,
        enable_screenshot=False,
        copy_dom=False,
        avoid_stale=False,
    ):
        """
        Click on Button/link on OpenShift Console

        locator (tuple): (GUI element needs to operate on (str), type (By))
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.
        enable_screenshot (bool): take screenshot
        copy_dom (bool): copy page source of the webpage
        avoid_stale (bool): if got StaleElementReferenceException, caused by reference to stale, cached element,
        refresh the page once and try click again
        * don't use when refreshed page expected to be different from initial page, or loose input values
        """

        def _do_click(_locator, _timeout=30, _enable_screenshot=False, _copy_dom=False):
            self.page_has_loaded()
            screenshot = (
                ocsci_config.UI_SELENIUM.get("screenshot") and enable_screenshot
            )
            if screenshot:
                self.take_screenshot()
            if _copy_dom:
                self.copy_dom()

            wait = WebDriverWait(self.driver, timeout)
            try:
                if (
                    version.get_semantic_version(get_ocp_version(), True)
                    <= version.VERSION_4_11
                ):
                    element = wait.until(
                        ec.element_to_be_clickable((locator[1], locator[0]))
                    )
                else:
                    element = wait.until(
                        ec.visibility_of_element_located((locator[1], locator[0]))
                    )
                element.click()
            except TimeoutException as e:
                self.take_screenshot()
                self.copy_dom()
                logger.error(e)
                raise TimeoutException(
                    f"Failed to find the element ({locator[1]},{locator[0]})"
                )

        try:
            _do_click(locator, timeout, enable_screenshot, copy_dom)
        except StaleElementReferenceException:
            if avoid_stale:
                logger.info("Refresh page to avoid reference to a stale element")
                self.driver.refresh()
                _do_click(locator, timeout, enable_screenshot, copy_dom)
            else:
                raise

    def do_click_by_id(self, id, timeout=30):
        return self.do_click((id, By.ID), timeout)

    def do_click_by_xpath(self, xpath, timeout=30):
        """
        Function to click on a web element using XPATH
        Args:
            xpath (str): xpath to interact with web element
            timeout (int): timeout until which an exception won't be raised

        Returns:
                Clicks on the web element found

        """
        return self.do_click((xpath, By.XPATH), timeout)

    def find_an_element_by_xpath(self, locator):
        """
        Function to find an element using xpath

        Args:
            locator (str): locator of the element to be found

        Returns:
            an object of the type WebElement

        """
        element = self.driver.find_element_by_xpath(locator)
        return element

    def do_send_keys(self, locator, text, timeout=30):
        """
        Send text to element on OpenShift Console

        locator (tuple): (GUI element needs to operate on (str), type (By))
        text (str): Send text to element
        timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        """
        self.page_has_loaded()
        wait = WebDriverWait(self.driver, timeout)
        try:
            if (
                version.get_semantic_version(get_ocp_version(), True)
                <= version.VERSION_4_11
            ):
                element = wait.until(
                    ec.presence_of_element_located((locator[1], locator[0]))
                )
            else:
                element = wait.until(
                    ec.visibility_of_element_located((locator[1], locator[0]))
                )
            element.send_keys(text)
        except TimeoutException as e:
            self.take_screenshot()
            self.copy_dom()
            logger.error(e)
            raise TimeoutException(
                f"Failed to find the element ({locator[1]},{locator[0]})"
            )
        return element

    def is_expanded(self, locator, timeout=30):
        """
        Check whether an element is in an expanded or collapsed state

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By))
            timeout (int): Looks for a web element repeatedly until timeout (sec) happens.

        return:
            bool: True if element expended, False otherwise

        """
        wait = WebDriverWait(self.driver, timeout)
        try:
            element = wait.until(ec.element_to_be_clickable((locator[1], locator[0])))
        except TimeoutException:
            # element_to_be_clickable() doesn't work as expected so just to harden
            # we are using presence_of_element_located
            element = wait.until(
                ec.presence_of_element_located((locator[1], locator[0]))
            )
        return True if element.get_attribute("aria-expanded") == "true" else False

    def choose_expanded_mode(self, mode, locator):
        """
        Select the element mode (expanded or collapsed)

        mode (bool): True if element expended, False otherwise
        locator (tuple): (GUI element needs to operate on (str), type (By))

        """
        current_mode = self.is_expanded(locator=locator, timeout=1000)
        if mode != current_mode:
            self.do_click(locator=locator, enable_screenshot=False)

    def get_checkbox_status(self, locator, timeout=30):
        """
        Checkbox Status

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By))
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
        locator (tuple): (GUI element needs to operate on (str), type (By))

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
            locator (tuple): (GUI element needs to operate on (str), type (By)).

        Return:
            str: The text captured.
        """
        return self.driver.find_element(by=locator[1], value=locator[0]).text

    def get_elements(self, locator):
        """
        Get an elements list. Useful to count number of elements presented on page, etc.

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By)).

        Return:
            list: The list of WebElements
        """
        return self.driver.find_elements(by=locator[1], value=locator[0])

    def wait_for_element_to_be_visible(self, locator, timeout=30):
        """
        Wait for element to be visible. Use when Web element is not have to be clickable (icons, disabled btns, etc.)
        Method does not fail when Web element not found

        Args:
             locator (tuple): (GUI element needs to operate on (str), type (By)).
             timeout (int): Looks for a web element until timeout (sec) occurs
        """
        wait = WebDriverWait(self.driver, timeout)
        return wait.until(ec.visibility_of_element_located((locator[1], locator[0])))

    def get_element_attribute(self, locator, attribute, safe: bool = False):
        """
        Get attribute from WebElement

        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By)).
            attribute (str): the value of this attribute will be extracted from WebElement
            safe(bool): if True exception will not raise when element not found. Default option - not safe

        Returns:
            str: value of the attribute of requested and found WebElement
        """
        web_elements = self.get_elements(locator)
        if safe:
            if not len(web_elements):
                return
        return web_elements[0].get_attribute(attribute)

    def wait_for_element_attribute(
        self, locator, attribute, attribute_value, timeout, sleep
    ):
        """
        Method to wait attribute have specific value. Fails the test if attribure value not equal to expected
        Args:
            locator (tuple): (GUI element needs to operate on (str), type (By)).
            attribute (str): the value of this attribute will be extracted from WebElement
            attribute_value (str): the value attribute (can be None as well)
            timeout (int): timeout in seconds
            sleep (int): sleep interval in seconds
        """
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self.get_element_attribute,
            locator=locator,
            attribute=attribute,
            safe=True,
        ):
            if sample == attribute_value:
                break

    def page_has_loaded(
        self, retries=5, sleep_time=2, module_loc=("html", By.TAG_NAME)
    ):
        """
        Waits for page to completely load by comparing current page hash values.
        Not suitable for pages that use frequent dynamically content (less than sleep_time)

        Args:
            retries (int): How much time in sleep_time to wait for page to load
            sleep_time (int): Time to wait between every pool of dom hash
            module_loc (tuple): locator of the module of the page awaited to be loaded
        """

        @retry(TimeoutException)
        def get_page_hash():
            """
            Get dom html hash
            """
            self.check_element_presence(module_loc[::-1])
            dom = self.get_element_attribute(module_loc, "innerHTML")
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
                logger.error(
                    f"Current URL did not finish loading in {retries * sleep_time}"
                )
                self.take_screenshot()
                return
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
        self.page_has_loaded(5, 5, self.page_nav["page_navigator_sidebar"])

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
        logger.info("Successfully navigated to ODF tab under Storage section")
        default_tab = OverviewTab()
        logger.info(f"Default page is {self.driver.title}")
        return default_tab

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
        self.page_has_loaded(retries=15, sleep_time=2)

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
        from ocs_ci.ocs.ui.mcg_ui import ObcUI

        return ObcUI()

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
            PageNavigator()
            .nav_odf_default_page()
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


class CreateResourceForm(PageNavigator):
    def __init__(self):
        self.status_error = "error status"
        self.status_indeterminate = "indeterminate status"
        self.status_success = "success status"
        self.result_col = ["rule", "check_func", "check_status"]
        self.test_results = pd.DataFrame(columns=self.result_col)
        super().__init__()

    def _report_failed(self, error_text):
        """
        Reports a failed test by logging an error message,
        taking a screenshot of the page and copying the DOM.

        Args:
            error_text (str): The error message to log.
        """
        logger.error(error_text)
        self.take_screenshot()
        self.copy_dom()

    def proceed_resource_creation(self):
        """
        Method to proceed to resource creation form, when Create button is visible
        """
        self.page_has_loaded()
        self.wait_for_element_to_be_visible(
            self.generic_locators["create_resource_button"]
        )
        self.do_click(self.generic_locators["create_resource_button"])

    def check_error_messages(self):
        """
        Performs a series of checks to verify if the error messages for the input fields
        meet the expected requirements. It clicks on the "create resource" button and verifies
        the existence of all expected rules in the input field. It then checks the error messages
        for each input field based on the expected rules and raises a failure if the actual
        error message does not match the expected message.
        Finally, it navigates back to the previous page.
        """
        self.page_has_loaded()
        self._verify_input_requirements()
        self.navigate_backward()
        logger.info("all error improvements checks done")

    def _verify_input_requirements(self):
        """
        Verify that all input requirements are met.
        """
        rules_texts_ok = self._check_all_rules_exist(
            self.generic_locators["text_input_popup_rules"]
        )
        self.test_results.loc[len(self.test_results)] = [
            None,
            self._check_all_rules_exist.__name__,
            rules_texts_ok,
        ]

        for rule, func in self.rules.items():
            res = func(rule)
            self.test_results.loc[len(self.test_results)] = [rule, func.__name__, res]

        logger.info(
            "\n"
            + self.test_results.to_markdown(
                headers="keys", index=False, tablefmt="grid"
            )
        )

        if not self.test_results[self.result_col[2]].all():
            failed_cases = self.test_results[~self.test_results[self.result_col[2]]]
            pytest.fail(
                "Error message improvements check failed\n"
                f"{failed_cases.to_markdown(headers='keys', index=False, tablefmt='grid')}"
            )

    def _check_all_rules_exist(self, input_loc: tuple):
        """
        Clicks on the input validator icon, retrieves the rules from the input location,
        and checks whether they match the list of expected rules. Returns True if they match,
        False otherwise.

        Args:
            input_loc (tuple): The locator of the input field containing the rules.

        Returns:
            bool: True if the list of rules in the input field matches the expected list,
            False otherwise.
        """
        self.do_click(self.validation_loc["input_value_validator_icon"])
        rules_elements = self.get_elements(input_loc)
        rules_texts_statuses = [rule.text for rule in rules_elements]
        rules_texts = [rule.split("\n: ")[0] for rule in rules_texts_statuses]
        if sorted(rules_texts) != sorted(self.rules.keys()):
            self._report_failed(
                f"Rules are not identical to the list of expected rules\n"
                f"Expected: {self.rules.keys()}\n"
                f"Actual: {rules_texts}"
            )
            return False
        return True

    def _check_rule_case(self, rule: str, input_text: str, status_exp: str) -> bool:
        """
        Check if a rule case passes for a given input text and expected status.

        Args:
            rule (str): The expected rule to be checked.
            input_text (str): The input text to be tested.
            status_exp (str): The expected status for the rule.

        Returns:
            bool: True if the check passed, False otherwise.
        """
        logger.info(f"check input '{input_text}', rule '{rule}'")
        try:
            self._send_input_and_update_popup(input_text)
            check_pass = self._check_input_rule_and_status(rule, status_exp)
        except TimeoutException or NoSuchElementException as e:
            logger.error(f"Got exception on check rule '{rule}'\n{e}")
            check_pass = False
        finally:
            self._remove_text_from_input()

        return check_pass

    @retry(TimeoutException)
    def _remove_text_from_input(self) -> bool:
        """
        Remove all text from a specified input element.

        Returns:
            bool: True if the input element is successfully cleared, False otherwise.
        """
        self.wait_for_element_to_be_visible(self.name_input_loc, 30)
        elements = self.get_elements(self.name_input_loc)
        input_el = elements[0]
        input_len = len(str(input_el.get_attribute("value")))

        # timeout in seconds will be equal to a number of symbols to be removed, but not less than 30s
        timeout = input_len if input_len > 30 else 30
        timeout = time.time() + timeout
        if len(elements):
            while len(str(input_el.get_attribute("value"))) != 0:
                if time.time() < timeout:
                    # to remove text from the input independently where the caret is use both delete and backspace
                    input_el.send_keys(Keys.BACKSPACE, Keys.DELETE)
                    time.sleep(0.05)
                else:
                    raise TimeoutException("time to clear input os out")
        else:
            logger.error("test input locator not found")
            return False
        return True

    def _check_input_text_length(
        self, rule_exp: str, text_length: int, status_expected: str
    ) -> bool:
        """
        A method that checks the length of the input text based on a rule and expected status.

        Args:
            rule_exp (str): the expected rule to be applied to the input text.
            text_length (int): the number of characters of the input text to be generated and tested.
            status_expected (str): the expected status after applying the rule on the input text.
        Returns:
            check_pass (bool): a boolean value indicating whether the input text satisfies the expected rule and status.
        """
        random_text_input = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=text_length)
        )
        self._send_input_and_update_popup(random_text_input)
        logger.info(
            f"rule '{rule_exp}'. "
            f"number of characters '{text_length}'. "
            f"status verify '{status_expected}'. "
            f"input '{random_text_input}. "
            f"check starts...'"
        )
        check_pass = self._check_input_rule_and_status(rule_exp, status_expected)

        self._remove_text_from_input()
        return check_pass

    def _send_input_and_update_popup(self, text_input: str):
        """
        Sends an input to the name input field, updates the validation popup, and reloads it.

        Args:
            text_input (str): The text input to send to the name input field.
        """
        try:
            self.do_send_keys(self.name_input_loc, text_input)
        except TimeoutException:
            logger.warning(
                "failed to send text to input. repeat send keys and update validation popup"
            )
            self._remove_text_from_input()
            self.do_send_keys(self.name_input_loc, text_input)
        # reload popup to process all input, but not a part
        self.do_click(self.validation_loc["input_value_validator_icon"])
        self.do_click(self.validation_loc["input_value_validator_icon"])

    def _check_input_rule_and_status(self, rule_exp, status_expected) -> bool:
        """
        Check the input rule and status against the expected values.

        Args:
            rule_exp (str): The expected input rule.
            status_expected (str): The expected status of the input rule.

        Returns:
            bool: True if the check passes, False otherwise.
        """
        check_pass = True

        def get_rule_actual():
            time_sleep = 2
            logger.debug(f"sleep {time_sleep} get browser render new popup")
            time.sleep(time_sleep)
            for _ in range(3):
                _rules_elements = self.get_elements(
                    self.generic_locators["text_input_popup_rules"]
                )
                logger.debug(f"sleep {time_sleep} get browser render new popup")
                time.sleep(time_sleep)
                if len(_rules_elements) > 0:
                    break
            else:
                logger.error("no rules found after 3 attempts")
            return [rule.text for rule in _rules_elements if rule_exp in rule.text]

        rule_actual = get_rule_actual()

        if len(rule_actual) > 1:
            self._report_failed(f"rule duplicated -> {rule_actual}'")
            check_pass = False
        elif len(rule_actual) < 1:
            self.page_has_loaded(retries=5, sleep_time=5)
            # reload popup to process all input one more time. May not appear if input is large - automation issue
            self.do_click(self.validation_loc["input_value_validator_icon"])
            if not len(get_rule_actual()):
                self.do_click(self.validation_loc["input_value_validator_icon"])
            rule_actual = get_rule_actual()
            if len(rule_actual) < 1:
                self._report_failed(f"rule not found -> {rule_actual}'")
                check_pass = False
        status_actual = rule_actual[0].split("\n: ")[1].replace(";", "")
        if status_expected not in status_actual:
            self._report_failed(
                f"status expected '{status_expected}'. status actual '{status_actual}'. check failed"
            )
            check_pass = False
        else:
            logger.info(
                f"status expected '{status_expected}'. status actual '{status_actual}'. check passed"
            )
        return check_pass

    def _check_start_end_char_rule(self, rule_exp) -> bool:
        """
        Check that the input field follows the rule that only alphanumeric lowercase characters are allowed and
        the first and last characters of the input field are also alphanumeric lowercase characters.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if all the checks pass, False otherwise.
        """
        alphanumeric_lowercase = string.ascii_lowercase + string.digits
        params_list = [
            (
                rule_exp,
                random.choice(string.ascii_uppercase),
                random.choice(string.ascii_uppercase),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(string.ascii_uppercase),
                random.choice(alphanumeric_lowercase),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(alphanumeric_lowercase),
                random.choice(string.ascii_uppercase),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(string.punctuation),
                random.choice(alphanumeric_lowercase),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(alphanumeric_lowercase),
                random.choice(string.punctuation),
                self.status_error,
            ),
            (
                rule_exp,
                random.choice(alphanumeric_lowercase),
                random.choice(alphanumeric_lowercase),
                self.status_success,
            ),
        ]

        return all(self._check_start_end_char_case(*params) for params in params_list)

    def _check_start_end_char_case(
        self, rule: str, start_letter: str, end_letter: str, status_exp: str
    ) -> bool:
        """Checks that an input string with a specific start and end character meets a given input rule.

        Args:
            rule (str): The input rule to check.
            start_letter (str): The start character for the input string.
            end_letter (str): The end character for the input string.
            status_exp (str): The expected status of the input string, either 'success' or 'error'.

        Returns:
            bool: True if the input string meets the input rule and has the expected status, False otherwise.
        """
        random_name = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        text_input = start_letter + random_name + end_letter
        self._send_input_and_update_popup(text_input)
        check_pass = self._check_input_rule_and_status(rule, status_exp)
        self._remove_text_from_input()
        if not check_pass:
            logger.error(f"check failed with input '{text_input}'")
        else:
            logger.info(f"check passed with input '{text_input}'")
        return check_pass

    def _check_only_lower_case_numbers_periods_hyphens_rule(self, rule_exp) -> bool:
        """
        Check if only the input text containing lowercase letters, digits, periods,
        and hyphens allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: indicating whether all test cases passed.
        """
        allowed_chars = string.ascii_lowercase + string.digits + "-"

        random_name = "".join(random.choices(allowed_chars, k=10))
        random_name = "a" + random_name + "z"
        name_with_consecutive_period = random_name[:4] + ".." + random_name[6:]

        uppercase_letters = "".join(random.choices(string.ascii_uppercase, k=2))
        name_with_uppercase_letters = (
            random_name[:4] + uppercase_letters + random_name[6:]
        )

        name_with_no_ascii = random_name[:4] + "æå" + random_name[6:]

        params_list = [
            (rule_exp, name_with_consecutive_period, self.status_error),
            (rule_exp, name_with_uppercase_letters, self.status_error),
            (rule_exp, name_with_no_ascii, self.status_error),
            (rule_exp, random_name, self.status_success),
        ]

        return all(self._check_rule_case(*params) for params in params_list)

    def _check_max_length_backing_store_rule(self, rule_exp):
        """
        Check if the length of the backing store name is less than or equal to the maximum allowed length.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the rule was not violated, False otherwise.
        """

        logger.info(f"checking the input rule '{rule_exp}'")
        max_length_exp = int(re.search(r"\d+(\.\d+)?", rule_exp).group())
        params_list = [
            (rule_exp, max_length_exp - 1, self.status_success),
            (rule_exp, max_length_exp, self.status_success),
            (rule_exp, max_length_exp + 1, self.status_error),
        ]

        return all(self._check_input_text_length(*params) for params in params_list)

    def _check_resource_name_not_exists_rule(
        self, existing_resource_names: str, rule_exp: str
    ) -> bool:
        """
        Checks that an existing resource name cannot be used.

        Args:
            existing_resource_names (str): A string containing a list of existing resource names.
            rule_exp (str): A string representing a rule to be checked.

        Returns:
            bool: True if not allowed to use duplicated resource name, False otherwise.
        """
        name_exist = existing_resource_names.split()[0].strip()
        index_to_replace = random.randint(0, len(name_exist) - 1)
        char_to_replace = name_exist[index_to_replace]
        random_char = random.choice(
            string.ascii_lowercase.replace(char_to_replace, "") + string.digits
        )
        name_does_not_exist = (
            name_exist[:index_to_replace]
            + random_char
            + name_exist[index_to_replace + 1 :]
        )
        params_list = [
            (rule_exp, name_exist, self.status_error),
            (rule_exp, name_does_not_exist, self.status_success),
        ]
        return all(self._check_rule_case(*params) for params in params_list)


class DataFoundationTabBar(PageNavigator):
    def __init__(self):
        super().__init__()

    def nav_storage_systems_tab(self):
        """
        Navigate to Storage Systems tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Storage Systems")
        self.do_click(self.validation_loc["storage_systems"], enable_screenshot=True)
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
        return BucketClassTab()

    def nav_namespace_store_tab(self):
        """
        Navigate to Namespace Store tab. Accessible from any Data Foundation tabs
        """
        logger.info("Navigate to Data Foundation - Namespace Store tab")
        self.do_click(locator=self.validation_loc["namespacestore_page"])
        return NameSpaceStoreTab()

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
    Overview tab Class
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


class StorageSystemTab(DataFoundationTabBar, CreateResourceForm):
    """
    Storage System tab Class
    Content of Data Foundation/Storage Systems tab

    """

    def __init__(self):
        DataFoundationTabBar.__init__(self)
        CreateResourceForm.__init__(self)
        self.rules = {
            constants.UI_INPUT_RULES_STORAGE_SYSTEM[
                "rule1"
            ]: self._check_max_length_backing_store_rule,
            constants.UI_INPUT_RULES_STORAGE_SYSTEM[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_STORAGE_SYSTEM[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_STORAGE_SYSTEM[
                "rule4"
            ]: self._check_storage_system_not_used_before_rule,
        }
        self.name_input_loc = self.sc_loc["sc-name"]

    def fill_backing_storage_form(self, backing_store_type: str, btn_text: str):
        """
        Storage system creation form consists from several forms, showed one after another when mandatory fields filled
        and Next btn clicked.
        Function to fill first form in order to create new Backing store.


        Args:
            backing_store_type (str): options available when filling backing store form (1-st form)
            btn_text (str): text of the button to be clicked after the form been filled ('Next', 'Back', 'Cancel')
        """
        option_1 = "Use an existing StorageClass"
        option_2 = "Create a new StorageClass using local storage devices"
        option_3 = "Connect an external storage platform"
        if backing_store_type not in [option_1, option_2, option_3]:
            raise IncorrectUIOptionRequested(
                f"Choose one of the existed option: '{[option_1, option_2, option_3]}'"
            )

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        self.do_click(
            format_locator(self.sc_loc["backing_store_type"], backing_store_type)
        )

        btn_1 = "Next"
        btn_2 = "Back"
        btn_3 = "Cancel"
        if btn_text not in [btn_1, btn_2, btn_3]:
            raise IncorrectUIOptionRequested(
                f"Choose one of the existed option: '{[btn_1, btn_2, btn_3]}'"
            )

        self.do_click(format_locator(self.sc_loc["button_with_txt"], btn_text))

    def _check_storage_system_not_used_before_rule(self, rule_exp) -> bool:
        """
        Checks whether the storage system name allowed to use again.

        This function executes an OpenShift command to retrieve the names of all existing storage systems
        in all namespaces.
        It then checks whether the name of the existed storage system would be allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if not allowed to use duplicated storage system name, False otherwise.
        """
        existing_storage_systems_names = str(
            OCP().exec_oc_cmd(
                "get storageclass --all-namespaces -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_storage_systems_names, rule_exp
        )

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
        self.page_has_loaded(5, 5)
        self.nav_ceph_blockpool().verify_cephblockpool_status()

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
        return BlockPools()

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


class BlockPools(StorageSystemDetails, CreateResourceForm):
    def __init__(self):
        StorageSystemTab.__init__(self)
        CreateResourceForm.__init__(self)
        self.name_input_loc = self.validation_loc["blockpool_name"]
        self.rules = {
            constants.UI_INPUT_RULES_BLOCKING_POOL[
                "rule1"
            ]: self._check_max_length_backing_store_rule,
            constants.UI_INPUT_RULES_BLOCKING_POOL[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_BLOCKING_POOL[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_BLOCKING_POOL[
                "rule4"
            ]: self._check_blockpool_not_used_before_rule,
        }

    def _check_blockpool_not_used_before_rule(self, rule_exp) -> bool:
        """
        Checks whether the blockpool name allowed to use again.

        This function executes an OpenShift command to retrieve the names of all existing blockpools in all namespaces.
        It then checks whether the name of the existed namespace store would be allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if not allowed to use duplicated blockpool name, False otherwise.
        """

        existing_blockpool_names = str(
            OCP().exec_oc_cmd(
                "get CephBlockPool --all-namespaces -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_blockpool_names, rule_exp
        )

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


class BackingStoreTab(DataFoundationDefaultTab, CreateResourceForm):
    def __init__(self):
        DataFoundationDefaultTab.__init__(self)
        CreateResourceForm.__init__(self)
        self.rules = {
            constants.UI_INPUT_RULES_BACKING_STORE[
                "rule1"
            ]: self._check_max_length_backing_store_rule,
            constants.UI_INPUT_RULES_BACKING_STORE[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_BACKING_STORE[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_BACKING_STORE[
                "rule4"
            ]: self._check_backingstore_name_not_used_before_per_namespace_rule,
        }
        self.name_input_loc = self.validation_loc["backingstore_name"]

    def _check_backingstore_name_not_used_before_per_namespace_rule(self, rule_exp):
        """
        Checks if a backing store name per namespace is not allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            A boolean value indicating whether the check passed or not.
        """
        existing_backingstore_names = str(
            OCP().exec_oc_cmd(
                f"get backingstore -n {config.ENV_DATA['cluster_namespace']} -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_backingstore_names, rule_exp
        )


class BucketClassTab(DataFoundationDefaultTab, CreateResourceForm):
    def __init__(self):
        DataFoundationTabBar.__init__(self)
        CreateResourceForm.__init__(self)
        self.rules = {
            constants.UI_INPUT_RULES_BUCKET_CLASS["rule1"]: self._check_3_63_char_rule,
            constants.UI_INPUT_RULES_BUCKET_CLASS[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_BUCKET_CLASS[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_BUCKET_CLASS[
                "rule4"
            ]: self._check_no_ip_address_rule,
            constants.UI_INPUT_RULES_BUCKET_CLASS[
                "rule5"
            ]: self._check_bucketclass_name_not_used_before_rule,
        }
        self.name_input_loc = self.bucketclass["bucketclass_name"]

    def _check_3_63_char_rule(self, rule_exp) -> bool:
        """
        Check if the input text length between 3 and 63 characters only can be used.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the input text length not violated, False otherwise.
        """
        logger.info(f"checking the input rule '{rule_exp}'")
        numbers = re.findall(r"\d+", rule_exp)
        min_len, max_len = map(int, numbers)
        params_list = [
            (rule_exp, min_len - 1, self.status_error),
            (rule_exp, min_len, self.status_success),
            (rule_exp, min_len + 1, self.status_success),
            (rule_exp, max_len - 1, self.status_success),
            (rule_exp, max_len, self.status_success),
            (rule_exp, max_len + 1, self.status_error),
        ]

        return all(self._check_input_text_length(*params) for params in params_list)

    def _check_no_ip_address_rule(self, rule_exp) -> bool:
        """
        Check if the input does not contain a valid IPv4 address.

        This function generates a random IPv4 address and a random string that is not an IP address.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the rule is satisfied for the random string that is not an IP address, False otherwise.
        """

        def _generate_ipv4_address_str():
            octets = [random.randint(0, 255) for _ in range(4)]
            ipv4_address_str = ".".join(map(str, octets))
            ipv4_address = ipaddress.IPv4Address(ipv4_address_str)
            return str(ipv4_address)

        random_ip = str(_generate_ipv4_address_str())
        not_ip = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )

        params_list = [
            (rule_exp, random_ip, self.status_error),
            (rule_exp, not_ip, self.status_success),
        ]

        return all(self._check_rule_case(*params) for params in params_list)

    def _check_bucketclass_name_not_used_before_rule(self, rule_exp) -> bool:
        """
        Checks whether the existed bucket class name allowed to use again.

        This function executes an OpenShift command to retrieve the names of all existing bucket classes
        in all namespaces.
        It then checks whether the name of the existed bucket class would be allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the bucket class name has not been used before, False otherwise.
        """
        existing_backingstore_names = str(
            OCP().exec_oc_cmd(
                "get bucketclass --all-namespaces -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_backingstore_names, rule_exp
        )


class NameSpaceStoreTab(DataFoundationDefaultTab, CreateResourceForm):
    def __init__(self):
        DataFoundationTabBar.__init__(self)
        CreateResourceForm.__init__(self)
        self.rules = {
            constants.UI_INPUT_RULES_NAMESPACE_STORE[
                "rule1"
            ]: self._check_max_length_backing_store_rule,
            constants.UI_INPUT_RULES_NAMESPACE_STORE[
                "rule2"
            ]: self._check_start_end_char_rule,
            constants.UI_INPUT_RULES_NAMESPACE_STORE[
                "rule3"
            ]: self._check_only_lower_case_numbers_periods_hyphens_rule,
            constants.UI_INPUT_RULES_NAMESPACE_STORE[
                "rule4"
            ]: self._check_namespace_store_not_used_before_rule,
        }
        self.name_input_loc = self.validation_loc["namespacestore_name"]

    def _check_namespace_store_not_used_before_rule(self, rule_exp) -> bool:
        """
        Checks whether the namespace store name allowed to use again.

        This function executes an OpenShift command to retrieve the names of all existing namespace stores
        in all namespaces.
        It then checks whether the name of the existed namespace store would be allowed to use.

        Args:
            rule_exp (str): the rule requested to be checked. rule_exp text should match the text from validation popup

        Returns:
            bool: True if the namespace name has not been used before, False otherwise.
        """
        existing_namespace_store_names = str(
            OCP().exec_oc_cmd(
                "get namespacestore --all-namespaces -o custom-columns=':metadata.name'"
            )
        )
        return self._check_resource_name_not_exists_rule(
            existing_namespace_store_names, rule_exp
        )


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
                logger.debug(
                    f"garbage collector to quit webdriver session id {obj.session_id}"
                )
                obj.quit()
                SeleniumDriver.remove_instance()
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

    @classmethod
    def remove_instance(cls):
        if hasattr(cls, "instance"):
            delattr(cls, "instance")
        else:
            logger.info("SeleniumDriver instance attr not found")


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
    page_nav_loc = locators[ocp_version]["page"]
    driver = SeleniumDriver()
    wait = WebDriverWait(driver, 40)
    driver.maximize_window()
    driver.implicitly_wait(10)
    driver.get(console_url)
    # Validate proceeding to the login console before taking any action:
    proceed_to_login_console()
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
        WebDriverWait(driver, 60).until(
            ec.visibility_of_element_located(
                (
                    page_nav_loc["page_navigator_sidebar"][1],
                    page_nav_loc["page_navigator_sidebar"][0],
                )
            )
        )
    if username is not constants.KUBEADMIN:
        element = wait.until(
            ec.element_to_be_clickable(
                (login_loc["skip_tour"][1], login_loc["skip_tour"][0])
            )
        )
        element.click()
    return driver


def close_browser():
    """
    Close Selenium WebDriver

    """
    logger.info("Close browser")
    take_screenshot()
    copy_dom()
    SeleniumDriver().quit()
    SeleniumDriver.remove_instance()
    time.sleep(10)
    garbage_collector_webdriver()


def proceed_to_login_console():
    """
    Proceed to the login console, if needed to confirm this action in a page that appears before.
    This is required to be as a solo function, because the driver initializes in the login_ui function.
    Function needs to be called just before login

    Returns:
        None

    """
    driver = SeleniumDriver()
    login_loc = locators[get_ocp_version()]["login"]
    if driver.title == login_loc["pre_login_page_title"]:
        proceed_btn = driver.find_element(
            by=login_loc["proceed_to_login_btn"][1],
            value=login_loc["proceed_to_login_btn"][0],
        )
        proceed_btn.click()
        WebDriverWait(driver, 60).until(ec.title_is(login_loc["login_page_title"]))
